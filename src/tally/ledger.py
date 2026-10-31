"""The ledger: accounts, balanced transactions, holds and reversals.

Every method takes an open connection and runs inside the caller's database
transaction, so the API layer can combine an operation with its idempotency
record atomically.

Who writes what: this module inserts transactions, entries and holds, and changes
hold statuses. The database derives everything else (migration 005): inserting an
entry moves the account's balance and fills in balance_after, and creating or
settling a hold moves the account's held amount. The checks here exist to fail
early with a precise error; the database enforces the same rules regardless.

Tenancy: a Ledger is bound to one API client and only ever sees that client's books.
Another client's account looks exactly like one that doesn't exist (404, not 403), so
ids leak nothing. ``Ledger()`` without a client is for global maintenance only (hold
expiry); any lookup it makes finds nothing and any insert fails, so forgetting the
client fails closed. The database enforces the same boundary (migrations 006 and 007).

Concurrency: every operation locks the account rows it touches with
``SELECT ... FOR NO KEY UPDATE``, always in ascending id order. Two transfers between
the same accounts in opposite directions therefore queue up instead of
deadlocking, and a balance is checked and changed under the same lock, so no
update is ever lost.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

import psycopg
from psycopg import Connection
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from .errors import LedgerError, conflict, not_found, unprocessable
from .observability import LOCK_WAIT_SECONDS, SETTLED, VOIDED

Direction = Literal["debit", "credit"]
AccountType = Literal["asset", "liability", "equity", "revenue", "expense"]

ACCOUNT_COLUMNS = (
    "id, name, type, normal_side, currency, allow_negative, balance, held, available, metadata, created_at"
)
TRANSACTION_COLUMNS = "id, description, metadata, reverses_id, posted_at"
ENTRY_COLUMNS = "id, account_id, direction, amount, currency, balance_after, posted_at"
EXPIRY_BATCH_SIZE = 500
# Payments settled per batch: one transaction with up to this many debits and one credit.
# The deferred balance check runs once per entry over the whole transaction, so cost grows
# with the square of the batch; 100 (the API's own entry limit) keeps a batch to milliseconds.
SETTLE_BATCH_SIZE = 100
# A destination's group that failed for a reason other than a ledger rule is passed over
# by this process for this long, so it can't keep the oldest slots of every batch.
SETTLE_RETRY_SECONDS = 60.0
# Errors that belong to one group's statements and leave the transaction usable once its
# savepoint is rolled back, by SQLSTATE: bad data (22), a violated constraint (23), a
# trigger's RAISE (P0), a trigger of ours refusing a change (55000). Everything else is
# the batch's problem, not the group's: a lost connection or server (08, 57, 53, 58, XX),
# a lock or statement timeout (55P03, 57014), a deadlock or serialization failure (40), a
# missing privilege (42). Those roll the batch back to be retried as a whole.
GROUP_SQLSTATES = ("22", "23", "P0", "55000")

log = logging.getLogger(__name__)

HOLD_COLUMNS = (
    "id, account_id, amount, status, description, expires_at, captured_transaction_id, settle_to_account_id, "
    "void_reason, created_at, updated_at"
)


def require_positive_amount(amount: object, what: str = "amount") -> None:
    # bool is a subclass of int in Python: True would otherwise pass as 1.
    if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
        raise unprocessable("INVALID_AMOUNT", f"{what} must be a positive whole number of minor units, got {amount!r}")


@dataclass(frozen=True)
class Leg:
    """One side of a transaction: move ``amount`` minor units on ``account_id``."""

    account_id: int
    direction: Direction
    amount: int

    def __post_init__(self) -> None:
        # Fail here with a clear error rather than later on a database CHECK.
        if self.direction not in ("debit", "credit"):
            raise unprocessable("INVALID_ENTRY", f"direction must be 'debit' or 'credit', got {self.direction!r}")
        require_positive_amount(self.amount)


def opposite(direction: str) -> Direction:
    return "credit" if direction == "debit" else "debit"


def confined_to_group(error: Exception) -> bool:
    """Whether a settlement group's failure can be rolled back to its savepoint and skipped."""
    if isinstance(error, psycopg.Error):
        return (error.sqlstate or "").startswith(GROUP_SQLSTATES)
    # A bug in the group's own code: its statements were rolled back with the savepoint,
    # and the next group starts from balances read afresh.
    return True


def payment_problem(payer: dict[str, Any], destination: dict[str, Any]) -> tuple[str, str] | None:
    """Why a payment from ``payer`` to ``destination`` can't be settled, as (code, message), or None.

    Settlement moves the payer away from its normal side and the destination by the same
    amount in the opposite direction, which is the payer's normal side. Paying a wallet
    (credit-normal) into a merchant liability credits the merchant: its balance grows.
    Paying it into an asset account would also credit it, but an asset grows with debits,
    so its balance would shrink; unless that account may go negative, settlement would
    overdraw it. The rule follows from normal sides, not from a list of account types.
    """
    if destination["currency"] != payer["currency"]:
        return "CURRENCY_MISMATCH", f"Cannot pay a {destination['currency']} account from a {payer['currency']} one"
    if destination["normal_side"] != payer["normal_side"] and not destination["allow_negative"]:
        return (
            "INVALID_DESTINATION",
            f"Paying account {destination['id']} from account {payer['id']} would reduce its balance "
            f"({destination['type']} accounts grow with {destination['normal_side']}s) and it may not go negative",
        )
    return None


class Ledger:
    def __init__(self, client_id: int | None = None) -> None:
        self.client_id = client_id
        # Payments whose settlement group failed, and until when (monotonic) to pass them over.
        self._settle_later: dict[UUID, float] = {}

    # --- accounts -----------------------------------------------------------------

    def create_account(
        self,
        conn: Connection,
        name: str,
        type: AccountType,
        currency: str,
        allow_negative: bool = False,
        metadata: dict | None = None,
    ) -> dict:
        return conn.execute(
            f"INSERT INTO accounts (client_id, name, type, currency, allow_negative, metadata) "
            f"VALUES (%s, %s, %s, %s, %s, %s) RETURNING {ACCOUNT_COLUMNS}",
            (self.client_id, name, type, currency, allow_negative, Jsonb(metadata or {})),
        ).fetchone()

    def get_account(self, conn: Connection, account_id: int) -> dict:
        account = conn.execute(
            f"SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE id = %s AND client_id = %s", (account_id, self.client_id)
        ).fetchone()
        if account is None:
            raise not_found("ACCOUNT_NOT_FOUND", f"Account {account_id} does not exist")
        return account

    def list_accounts(self, conn: Connection, limit: int = 50, after_id: int | None = None) -> list[dict]:
        return conn.execute(
            f"SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE client_id = %s AND id > %s ORDER BY id LIMIT %s",
            (self.client_id, after_id or 0, limit),
        ).fetchall()

    # --- posting ------------------------------------------------------------------

    def post(
        self,
        conn: Connection,
        legs: list[Leg],
        description: str = "",
        metadata: dict | None = None,
        reverses_id: UUID | None = None,
    ) -> dict:
        """Posts a balanced transaction; the database applies each entry to its account's balance."""
        accounts = self._lock_accounts(conn, {leg.account_id for leg in legs})
        self._check_postable(accounts, legs)
        transaction = self._insert_transaction(conn, description, metadata, reverses_id)
        return self._insert_entries(conn, transaction, accounts, legs)

    def transfer(
        self,
        conn: Connection,
        debit_account_id: int,
        credit_account_id: int,
        amount: int,
        description: str = "",
        metadata: dict | None = None,
    ) -> dict:
        """The common two-entry case: debit one account, credit another."""
        if debit_account_id == credit_account_id:
            raise unprocessable("SAME_ACCOUNT", "Debit and credit accounts must differ")
        legs = [Leg(debit_account_id, "debit", amount), Leg(credit_account_id, "credit", amount)]
        return self.post(conn, legs, description, metadata)

    def reverse(self, conn: Connection, transaction_id: UUID, description: str = "") -> dict:
        """Undoes a transaction by posting its mirror image. History is never edited."""
        # Row-lock the original first, so concurrent reversals of the same transaction
        # queue up here. Without it, the losers would pass the "already reversed?" check,
        # wait on the account locks, and then fail with a misleading INSUFFICIENT_FUNDS
        # (the money having just been moved back) instead of ALREADY_REVERSED.
        original = conn.execute(
            "SELECT id, reverses_id FROM transactions WHERE id = %s AND client_id = %s FOR UPDATE",
            (transaction_id, self.client_id),
        ).fetchone()
        if original is None:
            raise not_found("TRANSACTION_NOT_FOUND", f"Transaction {transaction_id} does not exist")
        if original["reverses_id"] is not None:
            raise conflict("CANNOT_REVERSE_REVERSAL", "A reversal cannot itself be reversed")
        if conn.execute("SELECT 1 FROM transactions WHERE reverses_id = %s", (transaction_id,)).fetchone():
            raise conflict("ALREADY_REVERSED", f"Transaction {transaction_id} has already been reversed")

        rows = conn.execute(
            "SELECT account_id, direction, amount FROM entries WHERE transaction_id = %s ORDER BY id",
            (transaction_id,),
        ).fetchall()
        legs = [Leg(r["account_id"], opposite(r["direction"]), r["amount"]) for r in rows]
        try:
            return self.post(conn, legs, description or f"Reversal of {transaction_id}", reverses_id=transaction_id)
        except psycopg.errors.UniqueViolation as exc:  # a concurrent reversal won the race
            raise conflict("ALREADY_REVERSED", f"Transaction {transaction_id} has already been reversed") from exc

    # --- holds --------------------------------------------------------------------

    def create_hold(
        self,
        conn: Connection,
        account_id: int,
        amount: int,
        expires_at: datetime | None,
        description: str = "",
        settle_to: int | None = None,
    ) -> dict:
        """Reserves funds: they stay in the balance but are no longer available.

        An ordinary hold (``settle_to`` None) is a card-style authorization: a client
        captures or voids it, and if nobody does, it expires at ``expires_at``.

        With ``settle_to`` the hold is a payment to that account. It has no expiry: once
        accepted it waits for the next settlement batch (``settle_holds``), however long
        that takes, and only settlement captures it. Only the payer's row is locked; the
        destination is read without a lock (its type, currency and owner can't change),
        which is the whole point when thousands of payers share one destination.
        """
        require_positive_amount(amount)
        if settle_to is None:
            # A naive datetime would be read in the database session's time zone, which can
            # shift the expiry by hours without any error.
            if expires_at is None or expires_at.tzinfo is None:
                raise unprocessable("INVALID_EXPIRY", "An ordinary hold needs expires_at, with a time zone")
            if expires_at <= datetime.now(UTC):
                raise unprocessable("INVALID_EXPIRY", f"expires_at must be in the future, got {expires_at.isoformat()}")
        else:
            if expires_at is not None:
                raise unprocessable(
                    "PAYMENTS_DO_NOT_EXPIRE", "A payment waits for settlement however long it takes; it has no expiry"
                )
            if settle_to == account_id:
                raise unprocessable("SAME_ACCOUNT", "A payment's destination must differ from the paying account")
            destination = self.get_account(conn, settle_to)  # no lock: see above
        account = self._lock_accounts(conn, {account_id})[account_id]
        if settle_to is not None and (problem := payment_problem(account, destination)):
            raise unprocessable(*problem)
        if not account["allow_negative"] and account["available"] < amount:
            raise conflict(
                "INSUFFICIENT_FUNDS", f"Account {account_id} has {account['available']} available, hold needs {amount}"
            )
        # The database adds an active hold to the account's held amount.
        return conn.execute(
            f"INSERT INTO holds (account_id, amount, expires_at, description, settle_to_account_id) "
            f"VALUES (%s, %s, %s, %s, %s) RETURNING {HOLD_COLUMNS}",
            (account_id, amount, expires_at, description, settle_to),
        ).fetchone()

    def capture_hold(
        self,
        conn: Connection,
        hold_id: UUID,
        destination_account_id: int,
        amount: int | None = None,
        description: str = "",
    ) -> dict:
        """Turns an ordinary hold into a real transaction. Any uncaptured remainder is released."""
        hold = self._lock_active_hold(conn, hold_id)
        if hold["settle_to_account_id"] is not None:
            # A payment's destination was fixed when it was accepted; capturing it by hand
            # could send the money elsewhere, or settle it twice.
            raise conflict(
                "PAYMENT_SETTLES_AUTOMATICALLY",
                f"Hold {hold_id} is a payment to account {hold['settle_to_account_id']}: settlement captures it",
            )
        amount = hold["amount"] if amount is None else amount
        require_positive_amount(amount, "capture amount")
        if amount > hold["amount"]:
            raise unprocessable("CAPTURE_EXCEEDS_HOLD", f"Cannot capture {amount}, the hold is for {hold['amount']}")
        if destination_account_id == hold["account_id"]:
            raise unprocessable("SAME_ACCOUNT", "Destination must differ from the held account")

        accounts = self._lock_accounts(conn, {hold["account_id"], destination_account_id})
        source = accounts[hold["account_id"]]
        # Spending moves the held account away from its normal side, whatever its type.
        out = opposite(source["normal_side"])
        legs = [Leg(hold["account_id"], out, amount), Leg(destination_account_id, opposite(out), amount)]
        # Check against the funds as they will be once this hold's reservation is
        # released; counting the reserved amount as unavailable would count it twice.
        released = {**accounts, source["id"]: {**source, "held": source["held"] - hold["amount"]}}
        self._check_postable(released, legs)

        transaction = self._insert_transaction(conn, description or hold["description"], {"hold_id": str(hold_id)})
        # Marking the hold captured releases its reservation (database trigger), so it
        # happens before the entries move the balance.
        captured = conn.execute(
            f"UPDATE holds SET status = 'captured', captured_transaction_id = %s "
            f"WHERE id = %s RETURNING {HOLD_COLUMNS}",
            (transaction["id"], hold_id),
        ).fetchone()
        transaction = self._insert_entries(conn, transaction, accounts, legs)
        return {"hold": captured, "transaction": transaction}

    def void_hold(self, conn: Connection, hold_id: UUID) -> dict:
        """Releases a hold. A payment can be voided too, until settlement has captured it."""
        self._lock_active_hold(conn, hold_id)
        # The database gives the amount back to the account's available funds.
        return conn.execute(
            f"UPDATE holds SET status = 'voided' WHERE id = %s RETURNING {HOLD_COLUMNS}", (hold_id,)
        ).fetchone()

    def expire_holds(self, conn: Connection, limit: int = EXPIRY_BATCH_SIZE) -> int:
        """Releases ordinary holds whose expiry has passed. Payments never expire; settlement ends them."""
        # SKIP LOCKED: holds that a capture or another instance is working on are left alone.
        expired = conn.execute(
            "SELECT id, account_id FROM holds "
            "WHERE status = 'active' AND settle_to_account_id IS NULL AND expires_at <= now() "
            "ORDER BY expires_at LIMIT %s FOR UPDATE SKIP LOCKED",
            (limit,),
        ).fetchall()
        if not expired:
            return 0
        # The trigger releasing each hold updates its account. Take those row locks first,
        # in id order like every other path, rather than in whatever order holds expire.
        self._lock_accounts(conn, {hold["account_id"] for hold in expired}, any_client=True)
        conn.execute("UPDATE holds SET status = 'expired' WHERE id = ANY(%s)", ([h["id"] for h in expired],))
        return len(expired)

    def settle_holds(self, conn: Connection, limit: int = SETTLE_BATCH_SIZE) -> int:
        """Settles one batch of the oldest pending payments; returns how many it took up.

        Each destination gets one transaction per batch: a debit from every payer, then one
        credit for the total, so the destination's row is locked once per batch instead of
        once per payment. Global, like expiry: each transaction belongs to its holds' client.

        One client's bad payment must not hold up everyone else's. A payment that can't be
        settled under the ledger's rules (see ``payment_problem``) is voided on its own, and
        each destination's group is posted under a savepoint:

        * the ledger refuses it (a ``LedgerError``): the group is undone and voided, with
          the reason in ``void_reason``;
        * it fails in a way confined to its own statements (``confined_to_group``: bad data,
          a constraint or trigger the checks here don't know, a bug in this code): the group
          is undone and left pending, reservations intact, nothing voided. The failure is
          logged, and this Ledger passes those payments over for ``SETTLE_RETRY_SECONDS`` so
          the payments behind them in the queue still get settled;
        * anything else (a lost connection, the database failing, a timeout or deadlock):
          it propagates, the whole batch rolls back, and every payment in it is retried at
          the next run.
        """
        now = time.monotonic()
        self._settle_later = {hold_id: until for hold_id, until in self._settle_later.items() if until > now}
        holds = conn.execute(
            f"SELECT {HOLD_COLUMNS}, client_id FROM holds "
            "WHERE status = 'active' AND settle_to_account_id IS NOT NULL AND NOT (id = ANY(%s)) "
            "ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED",
            (list(self._settle_later), limit),
        ).fetchall()
        if not holds:
            return 0
        # Same lock order as everywhere else: holds first, then accounts in id order.
        involved = {h["account_id"] for h in holds} | {h["settle_to_account_id"] for h in holds}
        accounts = self._lock_accounts(conn, involved, any_client=True)

        by_destination: dict[tuple[int, int], list[dict]] = defaultdict(list)
        for hold in holds:
            # The API refuses such payments, but one may predate that check or have been
            # written around the API. Voiding it is the only outcome that doesn't stall.
            problem = payment_problem(accounts[hold["account_id"]], accounts[hold["settle_to_account_id"]])
            if problem:
                self._void_payments(conn, [hold], ": ".join(problem))
            else:
                by_destination[(hold["client_id"], hold["settle_to_account_id"])].append(hold)

        for (client_id, destination), batch in by_destination.items():
            failure = self._in_savepoint(conn, self._settle_group, conn, client_id, destination, batch, accounts)
            if isinstance(failure, LedgerError):
                reason = f"{failure.code}: {failure.message}"
                failure = self._in_savepoint(conn, self._void_payments, conn, batch, reason)
            if failure is not None:
                self._settle_group_later(client_id, destination, batch, failure)
            # Later groups check against balances as they are now, not as they were locked.
            accounts.update(self._read_accounts(conn, involved))
        return len(holds)

    def _in_savepoint(self, conn: Connection, work, *args) -> Exception | None:
        """Runs ``work(*args)`` under a savepoint. Returns a ledger refusal, or a failure confined
        to the savepoint (rolled back with it); raises anything that may have broken the transaction."""
        try:
            with conn.transaction():
                work(*args)
        except LedgerError as refused:
            return refused
        except Exception as error:
            # The rollback to the savepoint must have worked, too: a broken or aborted
            # transaction is never used again.
            if not confined_to_group(error) or conn.broken or conn.info.transaction_status != TransactionStatus.INTRANS:
                raise
            return error
        return None

    def _settle_group_later(self, client_id: int, destination: int, batch: list[dict], failure: Exception) -> None:
        until = time.monotonic() + SETTLE_RETRY_SECONDS
        self._settle_later.update(dict.fromkeys((h["id"] for h in batch), until))
        log.error(
            "settlement group failed; its payments stay pending",
            extra={
                "event": "settlement_group_failed",
                "client_id": client_id,
                "destination_account_id": destination,
                "payments": len(batch),
                "amount": sum(h["amount"] for h in batch),
                "oldest_payment_id": str(batch[0]["id"]),
                "error": type(failure).__name__,
                "sqlstate": getattr(failure, "sqlstate", None),
                "detail": str(failure).splitlines()[0][:300] if str(failure) else "",
                "retry_in_seconds": SETTLE_RETRY_SECONDS,
            },
        )

    def _settle_group(
        self, conn: Connection, client_id: int, destination: int, batch: list[dict], accounts: dict[int, dict]
    ) -> None:
        legs = [Leg(h["account_id"], opposite(accounts[h["account_id"]]["normal_side"]), h["amount"]) for h in batch]
        # The destination takes the other side of the payers' legs (summed before any is added).
        totals = {d: sum(leg.amount for leg in legs if leg.direction == d) for d in ("debit", "credit")}
        legs += [Leg(destination, opposite(d), total) for d, total in totals.items() if total]
        # Check against the funds as they'll be once these reservations are released.
        released = dict(accounts)
        for hold in batch:
            source = released[hold["account_id"]]
            released[hold["account_id"]] = {**source, "held": source["held"] - hold["amount"]}
        self._check_postable(released, legs)

        tenant = Ledger(client_id)
        transaction = tenant._insert_transaction(conn, f"Settlement of {len(batch)} payments", {"payments": len(batch)})
        # Capturing releases each reservation (trigger), so it happens before the entries.
        conn.execute(
            "UPDATE holds SET status = 'captured', captured_transaction_id = %s WHERE id = ANY(%s)",
            (transaction["id"], [h["id"] for h in batch]),
        )
        tenant._insert_entries(conn, transaction, accounts, legs)
        SETTLED.inc(len(batch))

    def _void_payments(self, conn: Connection, holds: list[dict], reason: str) -> None:
        log.warning("settlement voided %d payment(s): %s", len(holds), reason)
        # Voiding releases each reservation (trigger); the reason is kept on the hold.
        conn.execute(
            "UPDATE holds SET status = 'voided', void_reason = %s WHERE id = ANY(%s)",
            (f"settlement refused: {reason}"[:500], [h["id"] for h in holds]),
        )
        VOIDED.inc(len(holds))

    def get_hold(self, conn: Connection, hold_id: UUID) -> dict:
        hold = conn.execute(
            f"SELECT {HOLD_COLUMNS} FROM holds WHERE id = %s AND client_id = %s", (hold_id, self.client_id)
        ).fetchone()
        if hold is None:
            raise not_found("HOLD_NOT_FOUND", f"Hold {hold_id} does not exist")
        return hold

    def list_holds(
        self,
        conn: Connection,
        account_id: int,
        status: str | None = None,
        limit: int = 50,
        before: tuple[datetime, UUID] | None = None,
    ) -> list[dict]:
        """An account's holds, newest first. ``before`` is the (created_at, id) of the last hold seen."""
        self.get_account(conn, account_id)
        after_cursor = "AND (created_at, id) < (%s, %s)" if before else ""
        return conn.execute(
            f"SELECT {HOLD_COLUMNS} FROM holds WHERE account_id = %s AND (%s::text IS NULL OR status = %s) "
            f"{after_cursor} ORDER BY created_at DESC, id DESC LIMIT %s",
            (account_id, status, status, *(before or ()), limit),
        ).fetchall()

    # --- history ------------------------------------------------------------------

    def get_transaction(self, conn: Connection, transaction_id: UUID) -> dict:
        transaction = conn.execute(
            f"SELECT {TRANSACTION_COLUMNS} FROM transactions WHERE id = %s AND client_id = %s",
            (transaction_id, self.client_id),
        ).fetchone()
        if transaction is None:
            raise not_found("TRANSACTION_NOT_FOUND", f"Transaction {transaction_id} does not exist")
        entries = conn.execute(
            f"SELECT {ENTRY_COLUMNS} FROM entries WHERE transaction_id = %s ORDER BY id", (transaction_id,)
        ).fetchall()
        reversed_by = conn.execute("SELECT id FROM transactions WHERE reverses_id = %s", (transaction_id,)).fetchone()
        return {**transaction, "entries": entries, "reversed_by": reversed_by["id"] if reversed_by else None}

    def recent_transactions(
        self, conn: Connection, limit: int = 20, before: tuple[datetime, UUID] | None = None
    ) -> list[dict]:
        """The latest transactions with their entries, two queries per page.

        ``before`` is the (posted_at, id) of the last transaction seen. The row comparison
        walks the (client_id, posted_at, id) index backwards from there, however deep the page.
        """
        after_cursor = "AND (t.posted_at, t.id) < (%s, %s)" if before else ""
        transactions = conn.execute(
            f"""
            SELECT {TRANSACTION_COLUMNS}, (SELECT r.id FROM transactions r WHERE r.reverses_id = t.id) AS reversed_by
              FROM transactions t
             WHERE t.client_id = %s {after_cursor}
             ORDER BY t.posted_at DESC, t.id DESC LIMIT %s
            """,
            (self.client_id, *(before or ()), limit),
        ).fetchall()
        entries = conn.execute(
            f"SELECT transaction_id, {ENTRY_COLUMNS} FROM entries WHERE transaction_id = ANY(%s) ORDER BY id",
            ([t["id"] for t in transactions],),
        ).fetchall()
        by_transaction: dict = {}
        for entry in entries:
            by_transaction.setdefault(entry.pop("transaction_id"), []).append(entry)
        return [{**t, "entries": by_transaction.get(t["id"], [])} for t in transactions]

    def statement(self, conn: Connection, account_id: int, limit: int = 50, before_id: int | None = None) -> list[dict]:
        """An account's entries, newest first, each with the running balance after it.

        Keyset pagination (``id < before_id``) stays fast on deep pages, unlike OFFSET,
        which has to walk past every skipped row.
        """
        self.get_account(conn, account_id)
        return conn.execute(
            """
            SELECT e.id, e.transaction_id, e.direction, e.amount, e.currency, e.balance_after, e.posted_at,
                   t.description
              FROM entries e JOIN transactions t ON t.id = e.transaction_id
             WHERE e.account_id = %s AND (%s::bigint IS NULL OR e.id < %s)
             ORDER BY e.id DESC
             LIMIT %s
            """,
            (account_id, before_id, before_id, limit),
        ).fetchall()

    def balance_at(self, conn: Connection, account_id: int, as_of: datetime) -> int:
        """The account's posted balance at a moment in the past, read from the running balance."""
        self.get_account(conn, account_id)
        # Within one account posted_at rises with id (migration 003 guarantees it), so
        # this is "the latest entry at or before as_of", answered by one probe of the
        # (account_id, posted_at, id) index however far back as_of is.
        row = conn.execute(
            "SELECT balance_after FROM entries WHERE account_id = %s AND posted_at <= %s "
            "ORDER BY posted_at DESC, id DESC LIMIT 1",
            (account_id, as_of),
        ).fetchone()
        return row["balance_after"] if row else 0

    # --- internals ----------------------------------------------------------------

    def _check_postable(self, accounts: dict[int, dict[str, Any]], legs: list[Leg]) -> None:
        """Validates legs against the locked accounts before anything is written."""
        if len(legs) < 2:
            raise unprocessable("TOO_FEW_ENTRIES", "A transaction needs at least two entries")
        currencies = [accounts[leg.account_id]["currency"] for leg in legs]
        if len(legs) == 2 and currencies[0] != currencies[1]:
            raise unprocessable(
                "CURRENCY_MISMATCH", f"Cannot move money between {currencies[0]} and {currencies[1]} accounts"
            )

        net: dict[str, int] = {}
        for leg, currency in zip(legs, currencies, strict=True):
            net[currency] = net.get(currency, 0) + (leg.amount if leg.direction == "debit" else -leg.amount)
        unbalanced = {currency: amount for currency, amount in net.items() if amount != 0}
        if unbalanced:
            detail = ", ".join(f"{c} debits - credits = {n}" for c, n in unbalanced.items())
            raise unprocessable("UNBALANCED", f"Debits must equal credits in every currency ({detail})")

        # Walk the legs the way the database will apply them, one entry at a time: a
        # protected account may not dip below zero at any step, not just at the end.
        balances = {account_id: account["balance"] for account_id, account in accounts.items()}
        for leg in legs:
            account = accounts[leg.account_id]
            balances[leg.account_id] += leg.amount if leg.direction == account["normal_side"] else -leg.amount
            if not account["allow_negative"] and balances[leg.account_id] - account["held"] < 0:
                raise conflict(
                    "INSUFFICIENT_FUNDS",
                    f"Account {leg.account_id} has {account['balance'] - account['held']} available, "
                    f"this transaction needs {account['balance'] - balances[leg.account_id]}",
                )

    def _insert_transaction(
        self, conn: Connection, description: str, metadata: dict | None, reverses_id: UUID | None = None
    ) -> dict:
        return conn.execute(
            f"INSERT INTO transactions (client_id, description, metadata, reverses_id) VALUES (%s, %s, %s, %s) "
            f"RETURNING {TRANSACTION_COLUMNS}",
            (self.client_id, description, Jsonb(metadata or {}), reverses_id),
        ).fetchone()

    def _insert_entries(
        self, conn: Connection, transaction: dict, accounts: dict[int, dict[str, Any]], legs: list[Leg]
    ) -> dict:
        # WITH ORDINALITY ... ORDER BY n: entries go in, and hit the balance trigger, in
        # exactly the order of the legs.
        entries = conn.execute(
            f"""
            INSERT INTO entries (transaction_id, account_id, direction, amount, currency)
            SELECT %s, account_id, direction, amount, currency
              FROM unnest(%s::bigint[], %s::text[], %s::bigint[], %s::char(3)[])
                   WITH ORDINALITY AS leg (account_id, direction, amount, currency, n)
             ORDER BY n
            RETURNING {ENTRY_COLUMNS}
            """,
            (
                transaction["id"],
                [leg.account_id for leg in legs],
                [leg.direction for leg in legs],
                [leg.amount for leg in legs],
                [accounts[leg.account_id]["currency"] for leg in legs],
            ),
        ).fetchall()
        return {**transaction, "entries": sorted(entries, key=lambda e: e["id"])}

    def _lock_accounts(
        self, conn: Connection, account_ids: set[int], any_client: bool = False
    ) -> dict[int, dict[str, Any]]:
        # ORDER BY id takes the row locks in ascending id order, the same order in every
        # code path, which is what rules out deadlocks. Only global maintenance
        # (any_client) may lock accounts outside the ledger's own client.
        # FOR NO KEY UPDATE, not FOR UPDATE: we only change balance and held, never a key,
        # and the weaker lock still excludes every other locker here, but it doesn't block
        # the FOR KEY SHARE lock a foreign key check takes. A payment to a merchant checks
        # its settle_to key against the merchant's row; with FOR UPDATE it would queue
        # behind the settler and every transfer to that merchant, which is the queue
        # batched settlement exists to avoid.
        with LOCK_WAIT_SECONDS.time():
            rows = conn.execute(
                f"SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE id = ANY(%s) AND (%s OR client_id = %s) "
                f"ORDER BY id FOR NO KEY UPDATE",
                (sorted(account_ids), any_client, self.client_id),
            ).fetchall()
        found = {row["id"]: row for row in rows}
        missing = sorted(account_ids - found.keys())
        if missing:
            raise not_found("ACCOUNT_NOT_FOUND", f"Account {missing[0]} does not exist")
        return found

    def _read_accounts(self, conn: Connection, account_ids: set[int]) -> dict[int, dict[str, Any]]:
        """Current rows of accounts this transaction has already locked."""
        rows = conn.execute(f"SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE id = ANY(%s)", (sorted(account_ids),))
        return {row["id"]: row for row in rows.fetchall()}

    def _lock_active_hold(self, conn: Connection, hold_id: UUID) -> dict:
        hold = conn.execute(
            f"SELECT {HOLD_COLUMNS}, expires_at <= now() AS is_expired FROM holds "
            f"WHERE id = %s AND client_id = %s FOR UPDATE",
            (hold_id, self.client_id),
        ).fetchone()
        if hold is None:
            raise not_found("HOLD_NOT_FOUND", f"Hold {hold_id} does not exist")
        if hold["status"] != "active":
            raise conflict("HOLD_NOT_ACTIVE", f"Hold {hold_id} is already {hold['status']}")
        if hold["is_expired"]:
            raise conflict("HOLD_EXPIRED", f"Hold {hold_id} expired at {hold['expires_at'].isoformat()}")
        return hold
