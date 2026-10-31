"""Payments to hot accounts: reserve on the payer now, settle in batches later."""

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import psycopg
import pytest

from tally.errors import LedgerError
from tally.ledger import Ledger
from tally.reconciliation import reconcile

LATER = timedelta(days=1)


def key() -> dict:
    return {"Idempotency-Key": str(uuid.uuid4())}


@pytest.fixture
def shop(books, wallet):
    """A merchant and three funded customers."""
    merchant = books.create_account("Merchant", "liability", "USD")
    customers = [wallet(f"customer {i}", funded=1_000) for i in range(3)]
    return {"merchant": merchant, "customers": customers}


def pay(books, customer, merchant, amount):
    return books.create_hold(customer["id"], amount, None, "coffee", merchant["id"])


def account(books, a) -> dict:
    return books.get_account(a["id"])


def settle(db) -> int:
    return db.run(lambda conn: Ledger().settle_holds(conn))


def test_a_payment_reserves_on_the_payer_and_the_merchant_is_paid_at_settlement(db, books, shop):
    merchant, (alice, bob, _) = shop["merchant"], shop["customers"]
    first, second = pay(books, alice, merchant, 300), pay(books, bob, merchant, 200)

    assert (account(books, alice)["available"], account(books, merchant)["balance"]) == (700, 0)
    assert settle(db) == 2

    settled = [books.get_hold(first["id"]), books.get_hold(second["id"])]
    assert {h["status"] for h in settled} == {"captured"}
    (transaction_id,) = {h["captured_transaction_id"] for h in settled}  # one transaction for the batch
    entries = books.get_transaction(transaction_id)["entries"]
    assert [(e["account_id"], e["direction"], e["amount"]) for e in entries] == [
        (alice["id"], "debit", 300),
        (bob["id"], "debit", 200),
        (merchant["id"], "credit", 500),
    ]
    assert (account(books, alice)["balance"], account(books, alice)["held"]) == (700, 0)
    assert account(books, merchant)["balance"] == 500
    assert db.run(reconcile)["ok"]


def test_settlement_goes_in_bounded_batches(db, books, shop):
    for customer in shop["customers"]:
        pay(books, customer, shop["merchant"], 10)
        pay(books, customer, shop["merchant"], 10)

    batches = [db.run(lambda conn: Ledger().settle_holds(conn, limit=4)) for _ in range(3)]

    assert batches == [4, 2, 0]
    assert account(books, shop["merchant"])["balance"] == 60


def test_paying_never_waits_on_the_merchants_row(migrated, db, shop):
    # The point of the feature: a transfer to a hot account queues on its row lock,
    # a payment doesn't. The blocker takes the lock Tally itself takes (a settlement
    # batch or a transfer in flight). Under FOR UPDATE the payment's foreign key check on
    # the merchant (FOR KEY SHARE) would queue too: this test caught exactly that.
    merchant, (alice, *_) = shop["merchant"], shop["customers"]
    impatient = Ledger(1)

    with psycopg.connect(migrated) as blocker:
        blocker.execute("SELECT 1 FROM accounts WHERE id = %s FOR NO KEY UPDATE", (merchant["id"],))

        def attempt(operation):
            def run(conn):
                conn.execute("SET LOCAL lock_timeout = '200ms'")
                return operation(conn)

            return db.run(run)

        with pytest.raises(psycopg.errors.LockNotAvailable):
            attempt(lambda conn: impatient.transfer(conn, alice["id"], merchant["id"], 10))
        hold = attempt(lambda conn: impatient.create_hold(conn, alice["id"], 10, None, "", merchant["id"]))

    assert hold["status"] == "active"


@pytest.mark.parametrize(
    ("destination", "code"),
    [("self", "SAME_ACCOUNT"), ("euro", "CURRENCY_MISMATCH"), ("nowhere", "ACCOUNT_NOT_FOUND")],
)
def test_a_payment_needs_a_valid_destination(books, shop, destination, code):
    alice = shop["customers"][0]
    target = {
        "self": alice["id"],
        "euro": books.create_account("Euro merchant", "liability", "EUR")["id"],
        "nowhere": 999_999,
    }[destination]

    with pytest.raises(LedgerError) as error:
        books.create_hold(alice["id"], 10, None, "", target)

    assert error.value.code == code


def test_a_voided_payment_is_not_settled(db, books, shop):
    merchant, (alice, bob, _) = shop["merchant"], shop["customers"]
    voided = pay(books, alice, merchant, 10)
    pay(books, bob, merchant, 30)
    books.void_hold(voided["id"])

    assert settle(db) == 1
    assert account(books, merchant)["balance"] == 30
    assert account(books, alice)["available"] == 1_000


def test_two_settlers_at_once_settle_each_payment_exactly_once(db, books, shop):
    for _ in range(40):
        for customer in shop["customers"]:
            pay(books, customer, shop["merchant"], 1)

    def drain(_):
        settled = 0
        while n := db.run(lambda conn: Ledger().settle_holds(conn, limit=7)):
            settled += n
        return settled

    with ThreadPoolExecutor(max_workers=2) as pool:
        settled = sum(pool.map(drain, range(2)))

    assert settled == 120
    assert account(books, shop["merchant"])["balance"] == 120
    assert db.run(reconcile)["ok"]


def test_a_payment_that_slipped_past_validation_cannot_stall_settlement(migrated, db, books, shop, caplog):
    merchant, bob = shop["merchant"], shop["customers"][1]
    euros = books.create_account("Euro wallet", "liability", "EUR", allow_negative=True)
    with psycopg.connect(migrated) as raw:  # around the API's currency check
        legacy = raw.execute(
            "INSERT INTO holds (account_id, amount, settle_to_account_id) VALUES (%s, 5, %s) RETURNING id",
            (euros["id"], merchant["id"]),
        ).fetchone()[0]
    good = pay(books, bob, merchant, 40)

    with caplog.at_level(logging.WARNING, logger="tally.ledger"):
        settle(db)

    assert books.get_hold(good["id"])["status"] == "captured"
    assert account(books, merchant)["balance"] == 40
    refused = books.get_hold(legacy)
    assert refused["status"] == "voided" and refused["void_reason"].startswith("settlement refused: CURRENCY_MISMATCH")
    assert account(books, euros)["held"] == 0
    assert any("voided 1 payment" in r.getMessage() for r in caplog.records)
    assert settle(db) == 0  # nothing left to trip over


def test_a_payments_destination_cannot_change(migrated, books, shop):
    hold = pay(books, shop["customers"][0], shop["merchant"], 10)
    other = books.create_account("Other merchant", "liability", "USD")

    with (
        psycopg.connect(migrated) as raw,
        pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="destination"),
    ):
        raw.execute("UPDATE holds SET settle_to_account_id = %s WHERE id = %s", (other["id"], hold["id"]))


def test_paying_over_http_and_reading_the_result(client, db, shop):
    merchant, alice = shop["merchant"], shop["customers"][0]

    created = client.post(
        "/v1/holds",
        json={"account_id": alice["id"], "amount": 250, "settle_to_account_id": merchant["id"]},
        headers=key(),
    )
    settle(db)
    hold = client.get(f"/v1/holds/{created.json()['id']}").json()

    assert created.status_code == 201 and created.json()["settle_to_account_id"] == merchant["id"]
    assert hold["status"] == "captured"
    assert client.get(f"/v1/accounts/{merchant['id']}").json()["balance"] == 250
