"""A payment's whole life, over HTTP and against the real database: accepted, then settled
(or refused) by maintenance, never lost to an expiry, never captured by hand."""

import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from fastapi.testclient import TestClient

from tally import api, auth
from tally.api import create_app
from tally.config import Settings
from tally.errors import LedgerError
from tally.ledger import Ledger
from tally.reconciliation import reconcile


class Tenant:
    """One API client, talking HTTP with its own key."""

    def __init__(self, http: TestClient, client_id: int, key: str) -> None:
        self.http, self.id, self.key = http, client_id, key

    def headers(self) -> dict:
        return {"Authorization": f"Bearer {self.key}", "Idempotency-Key": str(uuid.uuid4())}

    def post(self, path: str, body: dict):
        return self.http.post(path, json=body, headers=self.headers())

    def get(self, path: str) -> dict:
        response = self.http.get(path, headers=self.headers())
        assert response.status_code == 200, response.text
        return response.json()

    def account(self, name: str, type_: str, allow_negative: bool = False) -> dict:
        body = {"name": name, "type": type_, "currency": "USD", "allow_negative": allow_negative}
        response = self.post("/v1/accounts", body)
        assert response.status_code == 201, response.text
        return response.json()

    def funded(self, name: str, type_: str = "liability", amount: int = 1_000) -> dict:
        """A wallet (or any account) with ``amount`` in it, moved from a balancing account."""
        account = self.account(name, type_)
        source = self.account(f"{name} funding", "equity" if type_ == "asset" else "asset", allow_negative=True)
        debit, credit = (account, source) if type_ == "asset" else (source, account)
        moved = self.post(
            "/v1/transfers", {"debit_account_id": debit["id"], "credit_account_id": credit["id"], "amount": amount}
        )
        assert moved.status_code == 201, moved.text
        return account

    def pay(self, payer: dict, destination: dict, amount: int, **extra):
        body = {"account_id": payer["id"], "amount": amount, "settle_to_account_id": destination["id"], **extra}
        return self.post("/v1/holds", body)

    def balance(self, account: dict) -> dict:
        found = self.get(f"/v1/accounts/{account['id']}")
        return {"balance": found["balance"], "held": found["held"]}

    def hold(self, hold_id: str) -> dict:
        return self.get(f"/v1/holds/{hold_id}")


@pytest.fixture
def http(runtime_url):
    app = create_app(Settings(database_url=runtime_url, pool_size=10), run_migrations=False, maintenance=False)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def tenant(db, http):
    def make(name: str) -> Tenant:
        record, key = db.run(lambda conn: auth.create_client(conn, name))
        return Tenant(http, record["client_id"], key)

    return make


def snapshot(db) -> dict:
    """Everything a refused request or a repeated maintenance run must leave untouched."""

    def read(conn):
        counts = {
            table: conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]
            for table in ("transactions", "entries", "idempotency_keys", "outbox_events", "audit_log")
        }
        return {
            "accounts": conn.execute("SELECT id, balance, held FROM accounts ORDER BY id").fetchall(),
            "holds": conn.execute(
                "SELECT id, status, captured_transaction_id, void_reason FROM holds ORDER BY id"
            ).fetchall(),
            **counts,
        }

    return db.run(read)


def legacy_payment(migrated: str, payer: dict, destination: dict, amount: int) -> str:
    """An active payment written straight into the table, the way one accepted before the
    destination check (or written around the API) would be."""
    with psycopg.connect(migrated) as raw:
        return str(
            raw.execute(
                "INSERT INTO holds (account_id, amount, settle_to_account_id) VALUES (%s, %s, %s) RETURNING id",
                (payer["id"], amount, destination["id"]),
            ).fetchone()[0]
        )


def settle(app_db) -> int:
    return app_db.run(Ledger().settle_holds)


# --- P0: which destinations a payment may have -------------------------------------------


@pytest.mark.parametrize(
    ("payer_type", "destination_type", "allow_negative", "accepted"),
    [
        # A wallet (credit-normal) pays by being debited; the destination is credited.
        ("liability", "liability", False, True),  # a merchant's balance grows
        ("liability", "revenue", False, True),  # a fee: revenue grows
        ("liability", "equity", False, True),
        ("liability", "asset", False, False),  # an asset shrinks on a credit
        ("liability", "expense", False, False),  # so does an expense
        ("liability", "asset", True, True),  # unless it may go negative: the existing rule
        # A bank account (debit-normal) pays by being credited; the destination is debited.
        ("asset", "expense", False, True),  # paying a bill: the expense grows
        ("asset", "asset", False, True),
        ("asset", "liability", False, False),  # a debit shrinks a liability
    ],
)
def test_a_payment_may_not_shrink_a_protected_destination(
    app_db, db, tenant, payer_type, destination_type, allow_negative, accepted
):
    alice = tenant("alice")
    payer = alice.funded("payer", payer_type)
    destination = alice.account("destination", destination_type, allow_negative)
    before = snapshot(db)

    response = alice.pay(payer, destination, 100)

    if accepted:
        assert response.status_code == 201, response.text
        settle(app_db)
        assert alice.hold(response.json()["id"])["status"] == "captured"
        assert db.run(reconcile)["ok"]
    else:
        assert response.status_code == 422
        assert response.json()["code"] == "INVALID_DESTINATION"
        assert snapshot(db) == before  # no hold, no reservation, no key, no event, no audit row


def test_a_legacy_invalid_payment_is_voided_on_its_own_and_other_clients_are_settled(
    app_db, db, migrated, tenant, caplog
):
    alice, bob = tenant("alice"), tenant("bob")
    alice_wallet, empty_asset = alice.funded("wallet"), alice.account("empty asset", "asset")
    bad = legacy_payment(migrated, alice_wallet, empty_asset, 100)
    bob_wallet, merchant = bob.funded("wallet"), bob.account("merchant", "liability")
    good = bob.pay(bob_wallet, merchant, 50).json()
    assert alice.balance(alice_wallet) == {"balance": 1_000, "held": 100}

    with caplog.at_level(logging.WARNING):
        api.run_maintenance(app_db, Ledger())

    refused = alice.hold(bad)
    assert refused["status"] == "voided"
    assert refused["void_reason"].startswith("settlement refused: INVALID_DESTINATION")
    assert alice.balance(alice_wallet) == {"balance": 1_000, "held": 0}  # reservation released
    assert alice.balance(empty_asset)["balance"] == 0
    assert bob.hold(good["id"])["status"] == "captured"
    assert bob.balance(merchant)["balance"] == 50
    assert not [r for r in caplog.records if r.name == "tally.api"]  # no maintenance task failed
    assert db.run(reconcile)["ok"]


def refuse_posting_to(monkeypatch, destination: dict, error: Exception) -> None:
    """Fails a settlement group at its last step, after its transaction row and hold updates
    are written. ``payment_problem`` catches every refusal known today before posting, so
    this stands in for a rule only posting can check."""
    original = Ledger._insert_entries

    def insert(self, conn, transaction, accounts, legs):
        if any(leg.account_id == destination["id"] for leg in legs):
            raise error
        return original(self, conn, transaction, accounts, legs)

    monkeypatch.setattr(Ledger, "_insert_entries", insert)


def two_groups(tenant) -> tuple:
    """Alice's two customers pay her shop; Bob's customer pays his. Alice's group comes first."""
    alice, bob = tenant("alice"), tenant("bob")
    shop = alice.account("shop", "liability")
    payers = [alice.funded("ann"), alice.funded("amy")]
    alice_payments = [alice.pay(payer, shop, 30).json()["id"] for payer in payers]
    bob_wallet, bob_shop = bob.funded("wallet"), bob.account("shop", "liability")
    bob_payment = bob.pay(bob_wallet, bob_shop, 40).json()["id"]
    return alice, shop, payers, alice_payments, bob, bob_shop, bob_payment


def test_a_group_refused_at_posting_is_undone_and_voided_while_the_next_group_settles(app_db, db, tenant, monkeypatch):
    alice, shop, payers, alice_payments, bob, bob_shop, bob_payment = two_groups(tenant)
    refuse_posting_to(monkeypatch, shop, LedgerError(409, "INSUFFICIENT_FUNDS", "shop can't take it"))

    assert settle(app_db) == 3

    for payment in alice_payments:
        hold = alice.hold(payment)
        assert hold["status"] == "voided" and hold["captured_transaction_id"] is None
        assert hold["void_reason"] == "settlement refused: INSUFFICIENT_FUNDS: shop can't take it"
    assert [alice.balance(p) for p in payers] == [{"balance": 1_000, "held": 0}] * 2
    assert alice.balance(shop)["balance"] == 0
    # The savepoint took the refused group's transaction row with it: only Bob's remains.
    settlements = db.run(
        lambda conn: conn.execute("SELECT client_id FROM transactions WHERE description LIKE 'Settlement%'").fetchall()
    )
    assert settlements == [{"client_id": bob.id}]
    assert bob.hold(bob_payment)["status"] == "captured" and bob.balance(bob_shop)["balance"] == 40
    assert db.run(reconcile)["ok"]


@pytest.mark.parametrize(
    "error", [psycopg.OperationalError("server closed the connection"), RuntimeError("a bug"), KeyError("x")]
)
def test_an_unexpected_error_voids_nothing_and_the_next_run_settles_everything(
    app_db, db, tenant, monkeypatch, caplog, error
):
    alice, shop, payers, alice_payments, bob, bob_shop, bob_payment = two_groups(tenant)
    before = snapshot(db)
    refuse_posting_to(monkeypatch, shop, error)

    with caplog.at_level(logging.ERROR, logger="tally.api"):
        api.run_maintenance(app_db, Ledger())

    assert [r.getMessage() for r in caplog.records] == ["maintenance task failed: settle_payments"]
    assert snapshot(db) == before  # nothing voided, nothing half-settled: the batch rolled back
    monkeypatch.undo()
    api.run_maintenance(app_db, Ledger())
    assert {alice.hold(p)["status"] for p in alice_payments} == {"captured"}
    assert bob.hold(bob_payment)["status"] == "captured"
    assert (alice.balance(shop)["balance"], bob.balance(bob_shop)["balance"]) == (60, 40)


def test_repeated_maintenance_neither_posts_nor_releases_twice(app_db, db, migrated, tenant):
    alice, bob = tenant("alice"), tenant("bob")
    alice_wallet = alice.funded("wallet")
    legacy_payment(migrated, alice_wallet, alice.account("empty asset", "asset"), 100)
    card = alice.post("/v1/holds", {"account_id": alice_wallet["id"], "amount": 70, "expires_in_seconds": 1})
    bob_wallet, merchant = bob.funded("wallet"), bob.account("merchant", "liability")
    for _ in range(3):
        bob.pay(bob_wallet, merchant, 10)
    time.sleep(1.1)  # the card hold's expiry passes

    api.run_maintenance(app_db, Ledger())
    after_one = snapshot(db)
    api.run_maintenance(app_db, Ledger())
    api.run_maintenance(app_db, Ledger())

    assert snapshot(db) == after_one
    assert alice.hold(card.json()["id"])["status"] == "expired"
    assert alice.balance(alice_wallet) == {"balance": 1_000, "held": 0}
    assert bob.balance(bob_wallet) == {"balance": 970, "held": 0}
    assert bob.balance(merchant)["balance"] == 30
    assert db.run(reconcile)["ok"]


# --- P1: an accepted payment is never lost to time ----------------------------------------


def test_a_payment_has_no_expiry_and_refuses_one(db, tenant):
    alice = tenant("alice")
    wallet, merchant = alice.funded("wallet"), alice.account("merchant", "liability")
    before = snapshot(db)

    refused = alice.pay(wallet, merchant, 10, expires_in_seconds=1)

    assert refused.status_code == 422 and refused.json()["code"] == "PAYMENTS_DO_NOT_EXPIRE"
    assert snapshot(db) == before
    accepted = alice.pay(wallet, merchant, 10).json()
    assert accepted["expires_at"] is None and accepted["void_reason"] is None
    ordinary = alice.post("/v1/holds", {"account_id": wallet["id"], "amount": 10}).json()
    assert ordinary["expires_at"] is not None  # an ordinary hold still gets the 7-day default


def test_a_payment_outlives_maintenance_being_late_or_paused(app_db, db, tenant):
    # The review's reproduction: a payment accepted with a 1 s expiry, maintenance every
    # 5 s, and the payment expired unsettled. Here maintenance is late, then paused while
    # another instance keeps expiring holds, then resumes.
    alice = tenant("alice")
    wallet, merchant = alice.funded("wallet"), alice.account("merchant", "liability")

    def loop() -> tuple[threading.Event, threading.Thread]:
        stop = threading.Event()
        thread = threading.Thread(target=api._maintenance_loop, args=(app_db, Ledger(), 0.05, stop), daemon=True)
        thread.start()
        return stop, thread

    def wait_until_captured(hold_id: str) -> None:
        deadline = time.monotonic() + 5
        while alice.hold(hold_id)["status"] != "captured":
            assert time.monotonic() < deadline, alice.hold(hold_id)
            time.sleep(0.02)

    late = alice.pay(wallet, merchant, 10).json()["id"]
    card = alice.post("/v1/holds", {"account_id": wallet["id"], "amount": 5, "expires_in_seconds": 1}).json()["id"]
    time.sleep(1.2)  # maintenance is late: the card hold's expiry passes first
    assert app_db.run(Ledger().expire_holds) == 1  # expiry runs before settlement: worst order
    assert alice.hold(late)["status"] == "active"
    stop, thread = loop()
    wait_until_captured(late)

    stop.set()  # paused
    thread.join(timeout=5)
    paused = alice.pay(wallet, merchant, 20).json()["id"]
    time.sleep(1.2)
    for _ in range(3):
        app_db.run(Ledger().expire_holds)  # another instance's expiry keeps running meanwhile
    assert alice.hold(paused)["status"] == "active"
    stop, thread = loop()  # resumed
    wait_until_captured(paused)
    stop.set()
    thread.join(timeout=5)

    assert alice.hold(card)["status"] == "expired"
    assert alice.balance(wallet) == {"balance": 970, "held": 0}
    assert alice.balance(merchant)["balance"] == 30


def test_settlement_and_expiry_racing_settle_each_payment_once_and_release_each_hold_once(app_db, db, tenant):
    alice = tenant("alice")
    merchant = alice.account("merchant", "liability")
    payers = [alice.funded(f"payer {i}") for i in range(4)]
    payments = [alice.pay(p, merchant, 3).json()["id"] for p in payers for _ in range(15)]
    cards = [
        alice.post("/v1/holds", {"account_id": p["id"], "amount": 2, "expires_in_seconds": 1}).json()["id"]
        for p in payers
        for _ in range(15)
    ]
    time.sleep(1.1)
    start = threading.Barrier(4)

    def drain(job) -> int:
        start.wait()
        done = 0
        while n := app_db.run(lambda conn: job(conn, 7)):
            done += n
        return done

    ledger = Ledger()
    jobs = [ledger.settle_holds, ledger.expire_holds] * 2
    with ThreadPoolExecutor(max_workers=4) as pool:
        settled_a, expired_a, settled_b, expired_b = pool.map(drain, jobs)

    assert (settled_a + settled_b, expired_a + expired_b) == (60, 60)
    assert {alice.hold(p)["status"] for p in payments} == {"captured"}
    assert {alice.hold(c)["status"] for c in cards} == {"expired"}
    assert [alice.balance(p) for p in payers] == [{"balance": 955, "held": 0}] * 4
    assert alice.balance(merchant)["balance"] == 180
    assert db.run(reconcile)["ok"]


# --- P2: a payment's destination is fixed; settlement alone captures it ---------------------


@pytest.mark.parametrize(
    ("destination", "status", "code"),
    [
        ("original", 409, "PAYMENT_SETTLES_AUTOMATICALLY"),
        ("other", 409, "PAYMENT_SETTLES_AUTOMATICALLY"),
        ("omitted", 422, "VALIDATION_ERROR"),
    ],
)
def test_a_payment_cannot_be_captured_by_hand(app_db, db, tenant, destination, status, code):
    alice = tenant("alice")
    wallet, merchant, other = (
        alice.funded("wallet"),
        alice.account("merchant", "liability"),
        alice.account("x", "liability"),
    )
    payment = alice.pay(wallet, merchant, 30).json()
    body = {"original": {"destination_account_id": merchant["id"]}, "other": {"destination_account_id": other["id"]}}
    before = snapshot(db)

    response = alice.post(f"/v1/holds/{payment['id']}/capture", body.get(destination, {}))

    assert (response.status_code, response.json()["code"]) == (status, code)
    assert snapshot(db) == before
    settle(app_db)  # and settlement still pays the original destination, once
    assert alice.hold(payment["id"])["status"] == "captured"
    assert (alice.balance(merchant)["balance"], alice.balance(other)["balance"]) == (30, 0)


def test_an_ordinary_hold_is_still_captured_by_hand(tenant):
    alice = tenant("alice")
    wallet, merchant = alice.funded("wallet"), alice.account("merchant", "liability")
    hold = alice.post("/v1/holds", {"account_id": wallet["id"], "amount": 30}).json()

    response = alice.post(f"/v1/holds/{hold['id']}/capture", {"destination_account_id": merchant["id"], "amount": 20})

    assert response.status_code == 200, response.text
    assert alice.balance(wallet) == {"balance": 980, "held": 0}
    assert alice.balance(merchant)["balance"] == 20
