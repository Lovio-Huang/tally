"""Model-based testing with Hypothesis.

Hypothesis generates random sequences of ledger operations (open accounts,
deposits, transfers, holds, captures, voids, reversals, payments, settlement
batches, hold expiry, whole maintenance runs repeated) and runs them against the
real database, as the API's runtime role. Alongside, a small in-memory model
predicts what each operation should do, from its own arithmetic rather than the
ledger's code. After every single step we check that:

  * reconciliation is clean: every transaction balances and every stored
    balance equals the sum of its entries
  * the database agrees with the model, balance for balance and hold for hold
    (status, and why settlement voided a payment)
  * no protected account is overdrawn

When a sequence breaks a rule, Hypothesis shrinks it to the shortest failing
sequence and prints it.
"""

from datetime import UTC, datetime, timedelta

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    initialize,
    invariant,
    multiple,
    precondition,
    rule,
    run_state_machine_as_test,
)

from tally import api
from tally.errors import LedgerError
from tally.ledger import Ledger, Leg, opposite
from tally.reconciliation import reconcile

from .conftest import Books, reset

amounts = st.integers(min_value=1, max_value=5_000)


def ledger_machine(db, app_db):
    books = Books(app_db)

    class LedgerMachine(RuleBasedStateMachine):
        accounts = Bundle("accounts")

        def __init__(self):
            super().__init__()
            reset(db)
            # The outside world: money enters and leaves the system through it.
            self.world = books.create_account("World", "equity", "USD", allow_negative=True)["id"]
            self.model = {self.world: {"balance": 0, "held": 0, "normal": "credit", "allow_negative": True}}
            self.holds: dict = {}  # active ordinary hold id -> (account id, amount)
            self.overdue: set = set()  # ordinary holds whose expiry has passed
            self.payments: dict = {}  # pending payment id -> (payer, destination, amount, refusal or None)
            self.statuses: dict = {}  # every hold id -> (status, void reason prefix or None)
            self.reversible: list = []  # (transaction id, legs)

        # --- model helpers ---------------------------------------------------------

        def effect(self, legs):
            """New balances if the legs were posted, or None if a protected account would be overdrawn."""
            balances = {a: m["balance"] for a, m in self.model.items()}
            for leg in legs:
                balances[leg.account_id] += (
                    leg.amount if leg.direction == self.model[leg.account_id]["normal"] else -leg.amount
                )
            for account_id, balance in balances.items():
                m = self.model[account_id]
                if not m["allow_negative"] and balance - m["held"] < 0:
                    return None
            return balances

        def apply(self, balances):
            for account_id, balance in balances.items():
                self.model[account_id]["balance"] = balance

        def post(self, legs, attempt):
            expected = self.effect(legs)
            try:
                result = attempt()
            except LedgerError as error:
                assert error.code == "INSUFFICIENT_FUNDS", error.code
                assert expected is None, "the ledger refused a transaction the model allows"
                return None
            assert expected is not None, "the ledger allowed an overdraft the model forbids"
            self.apply(expected)
            return result

        def expect_refusal(self, code, attempt):
            try:
                attempt()
            except LedgerError as error:
                assert error.code == code, (error.code, code)
            else:
                raise AssertionError(f"expected {code}")

        def payment_legs(self, payer, destination, amount):
            """What settling the payment posts: the payer spends, the destination takes the other side."""
            spend = Leg(payer, opposite(self.model[payer]["normal"]), amount)
            return [spend, Leg(destination, opposite(spend.direction), amount)]

        def payment_refusal(self, payer, destination):
            """The model's rule: a payment may not be able to overdraw its destination."""
            credit = self.payment_legs(payer, destination, 1)[1]
            shrinks = credit.direction != self.model[destination]["normal"]
            return "INVALID_DESTINATION" if shrinks and not self.model[destination]["allow_negative"] else None

        def release(self, account, amount):
            self.model[account]["held"] -= amount

        def settle_all(self, batch_size):
            """Settlement as specified: oldest payments first, batch by batch; an unsettleable
            payment is voided on its own; each destination's payments post together or are
            voided together; nothing else is touched."""
            pending = list(self.payments.items())
            for start in range(0, len(pending), batch_size):
                groups: dict = {}
                for hold_id, (payer, destination, amount, refusal) in pending[start : start + batch_size]:
                    del self.payments[hold_id]
                    if refusal:
                        self.release(payer, amount)
                        self.statuses[hold_id] = ("voided", f"settlement refused: {refusal}")
                    else:
                        groups.setdefault(destination, []).append((hold_id, payer, amount))
                for destination, batch in groups.items():
                    for _, payer, amount in batch:
                        self.release(payer, amount)
                    legs = [leg for _, payer, amount in batch for leg in self.payment_legs(payer, destination, amount)]
                    balances = self.effect(legs)
                    if balances is None:
                        outcome = ("voided", "settlement refused: INSUFFICIENT_FUNDS")
                    else:
                        self.apply(balances)
                        outcome = ("captured", None)
                    for hold_id, _, _ in batch:
                        self.statuses[hold_id] = outcome

        def expire_all(self):
            for hold_id in self.overdue:
                account, amount = self.holds.pop(hold_id)
                self.release(account, amount)
                self.statuses[hold_id] = ("expired", None)
            self.overdue.clear()

        # --- rules -----------------------------------------------------------------

        @initialize(target=accounts, funds=st.lists(st.integers(5_000, 50_000), min_size=2, max_size=2))
        def open_wallets_and_a_merchant(self, funds):
            # Without these, a sequence spends its first steps with nothing to pay with.
            wallets = [self.open_account("liability", False) for _ in funds]
            for wallet, amount in zip(wallets, funds, strict=True):
                self.deposit(wallet, amount)
            return multiple(*wallets, self.open_account("liability", False))

        @rule(
            target=accounts, kind=st.sampled_from(["asset", "liability", "revenue", "expense"]), negative=st.booleans()
        )
        def open_account(self, kind, negative):
            account = books.create_account(f"{kind} account", kind, "USD", allow_negative=negative)
            self.model[account["id"]] = {
                "balance": 0,
                "held": 0,
                "allow_negative": negative,
                "normal": "debit" if kind in ("asset", "expense") else "credit",
            }
            return account["id"]

        @rule(account=accounts, amount=amounts)
        def deposit(self, account, amount):
            grow = self.model[account]["normal"]
            legs = [Leg(account, grow, amount), Leg(self.world, opposite(grow), amount)]
            transaction = self.post(legs, lambda: books.post(legs))
            self.reversible.append((transaction["id"], legs))

        @rule(debit=accounts, credit=accounts, amount=amounts)
        def transfer(self, debit, credit, amount):
            if debit == credit:
                return
            legs = [Leg(debit, "debit", amount), Leg(credit, "credit", amount)]
            transaction = self.post(legs, lambda: books.transfer(debit, credit, amount))
            if transaction:
                self.reversible.append((transaction["id"], legs))

        def funds_for(self, account, amount):
            m = self.model[account]
            return m["allow_negative"] or m["balance"] - m["held"] >= amount

        def boundary(self, account, amount, all_of_it):
            """Half the time, exactly what's available: where reservations and spends meet."""
            available = self.model[account]["balance"] - self.model[account]["held"]
            return available if all_of_it and available > 0 else amount

        @rule(account=accounts, amount=amounts, all_of_it=st.booleans())
        def hold(self, account, amount, all_of_it):
            amount = self.boundary(account, amount, all_of_it)
            allowed = self.funds_for(account, amount)
            try:
                hold = books.create_hold(account, amount, datetime.now(UTC) + timedelta(days=1))
            except LedgerError as error:
                assert error.code == "INSUFFICIENT_FUNDS" and not allowed
                return
            assert allowed
            self.model[account]["held"] += amount
            self.holds[hold["id"]] = (account, amount)
            self.statuses[hold["id"]] = ("active", None)

        @rule(payer=accounts, destination=accounts, amount=amounts, all_of_it=st.booleans())
        def pay(self, payer, destination, amount, all_of_it):
            amount = self.boundary(payer, amount, all_of_it)

            def attempt():
                return books.create_hold(payer, amount, None, "", destination)

            if payer == destination:
                return self.expect_refusal("SAME_ACCOUNT", attempt)
            refusal = self.payment_refusal(payer, destination)
            if refusal is None and not self.funds_for(payer, amount):
                refusal = "INSUFFICIENT_FUNDS"
            if refusal:
                return self.expect_refusal(refusal, attempt)
            payment = attempt()
            self.model[payer]["held"] += amount
            self.payments[payment["id"]] = (payer, destination, amount, None)
            self.statuses[payment["id"]] = ("active", None)

        @rule(payer=accounts, destination=accounts, amount=amounts)
        def legacy_payment(self, payer, destination, amount):
            """A payment the API now refuses, written straight into the table, as one accepted
            before the destination check would be. Settlement must void it alone."""
            refusal = self.payment_refusal(payer, destination)
            if payer == destination or refusal is None or not self.funds_for(payer, amount):
                return
            row = db.run(
                lambda conn: conn.execute(
                    "INSERT INTO holds (account_id, amount, settle_to_account_id) VALUES (%s, %s, %s) RETURNING id",
                    (payer, amount, destination),
                ).fetchone()
            )
            self.model[payer]["held"] += amount
            self.payments[row["id"]] = (payer, destination, amount, refusal)
            self.statuses[row["id"]] = ("active", None)

        @precondition(lambda self: self.holds)
        @rule(data=st.data(), destination=accounts)
        def capture(self, data, destination):
            hold_id = data.draw(st.sampled_from(list(self.holds)))  # creation order: deterministic on replay
            account, held = self.holds[hold_id]
            if destination == account:
                return
            amount = data.draw(st.one_of(st.just(held), st.integers(min_value=1, max_value=held)))
            if hold_id in self.overdue:
                return self.expect_refusal("HOLD_EXPIRED", lambda: books.capture_hold(hold_id, destination, amount))
            self.release(account, held)  # released before the spend is checked
            out = opposite(self.model[account]["normal"])
            legs = [Leg(account, out, amount), Leg(destination, opposite(out), amount)]
            result = self.post(legs, lambda: books.capture_hold(hold_id, destination, amount))
            if result is None:  # the whole capture rolled back, hold included
                self.model[account]["held"] += held
                return
            del self.holds[hold_id]
            self.statuses[hold_id] = ("captured", None)
            self.reversible.append((result["transaction"]["id"], legs))

        @precondition(lambda self: self.payments)
        @rule(data=st.data(), destination=accounts)
        def capture_payment(self, data, destination):
            hold_id = data.draw(st.sampled_from(list(self.payments)))
            self.expect_refusal("PAYMENT_SETTLES_AUTOMATICALLY", lambda: books.capture_hold(hold_id, destination))

        @precondition(lambda self: self.holds or self.payments)
        @rule(data=st.data())
        def void(self, data):
            hold_id = data.draw(st.sampled_from(list(self.holds) + list(self.payments)))
            if hold_id in self.overdue:
                return self.expect_refusal("HOLD_EXPIRED", lambda: books.void_hold(hold_id))
            books.void_hold(hold_id)
            if hold_id in self.holds:
                account, amount = self.holds.pop(hold_id)
            else:
                account, _, amount, _ = self.payments.pop(hold_id)
            self.release(account, amount)
            self.statuses[hold_id] = ("voided", None)

        @precondition(lambda self: set(self.holds) - self.overdue)
        @rule(data=st.data())
        def let_hold_expire(self, data):
            hold_id = data.draw(st.sampled_from([h for h in self.holds if h not in self.overdue]))
            # As the owner: time passing, not something the API's role may do.
            db.run(
                lambda conn: conn.execute(
                    "UPDATE holds SET expires_at = now() - interval '1 second' WHERE id = %s", (hold_id,)
                )
            )
            self.overdue.add(hold_id)

        @rule(batch_size=st.integers(min_value=1, max_value=4))
        def settle(self, batch_size):
            while app_db.run(lambda conn: Ledger().settle_holds(conn, limit=batch_size)) == batch_size:
                pass
            self.settle_all(batch_size)

        @rule()
        def expire(self):
            app_db.run(Ledger().expire_holds)
            self.expire_all()

        @rule(runs=st.integers(min_value=1, max_value=3))
        def maintenance(self, runs):
            # Every task, as the background loop runs them; a repeat must change nothing.
            for _ in range(runs):
                api.run_maintenance(app_db, Ledger())
            self.settle_all(batch_size=100)
            self.expire_all()

        @precondition(lambda self: self.reversible)
        @rule(data=st.data())
        def reverse(self, data):
            index = data.draw(st.integers(min_value=0, max_value=len(self.reversible) - 1))
            transaction_id, legs = self.reversible[index]
            mirror = [Leg(leg.account_id, opposite(leg.direction), leg.amount) for leg in legs]
            if self.post(mirror, lambda: books.reverse(transaction_id)) is not None:
                self.reversible.pop(index)

        # --- invariants ------------------------------------------------------------

        @invariant()
        def books_reconcile(self):
            report = db.run(reconcile)
            assert report["ok"], report

        @invariant()
        def database_matches_model(self):
            rows = db.run(
                lambda conn: conn.execute(
                    "SELECT id, balance, held, available, allow_negative FROM accounts"
                ).fetchall()
            )
            for row in rows:
                m = self.model[row["id"]]
                assert (row["balance"], row["held"]) == (m["balance"], m["held"]), (row, m)
                assert row["allow_negative"] or row["available"] >= 0

        @invariant()
        def holds_match_model(self):
            rows = db.run(lambda conn: conn.execute("SELECT id, status, void_reason FROM holds").fetchall())
            assert {row["id"] for row in rows} == set(self.statuses)
            for row in rows:
                status, reason = self.statuses[row["id"]]
                assert row["status"] == status, (row, status)
                assert (row["void_reason"] is None) == (reason is None), (row, reason)
                assert (row["void_reason"] or "").startswith(reason or ""), (row, reason)

    return LedgerMachine


def test_random_operation_sequences_keep_the_books_correct(db, app_db):
    run_state_machine_as_test(
        ledger_machine(db, app_db),
        settings=settings(  # max_examples comes from the profile (see conftest.py)
            stateful_step_count=40,
            deadline=None,
            suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
        ),
    )
