"""The outbox: every change to a client's books becomes an event, atomically and without gaps."""

import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg.rows import dict_row

from tally import auth, events
from tally.ledger import Ledger

from .conftest import DEFAULT_CLIENT, Books


def key() -> dict:
    return {"Idempotency-Key": str(uuid.uuid4())}


def feed(client, cursor=None, headers=None, limit=100) -> dict:
    params = {"limit": limit} | ({"cursor": cursor} if cursor else {})
    response = client.get("/v1/events", params=params, headers=headers or {})
    assert response.status_code == 200, response.text
    return response.json()


def types(page: dict) -> list[str]:
    return [e["type"] for e in page["data"]]


def test_a_transfer_is_one_event_carrying_its_entries(client, wallet):
    alice, bob = wallet("alice", funded=500), wallet("bob")
    start = feed(client)["next_cursor"]

    payment = client.post(
        "/v1/transfers",
        json={"debit_account_id": alice["id"], "credit_account_id": bob["id"], "amount": 120},
        headers=key(),
    ).json()

    (event,) = feed(client, start)["data"]
    assert event["type"] == "transaction.posted"
    assert event["data"]["id"] == payment["id"]
    assert [(e["account_id"], e["direction"], e["amount"], e["balance_after"]) for e in event["data"]["entries"]] == [
        (alice["id"], "debit", 120, 380),
        (bob["id"], "credit", 120, 120),
    ]


def test_every_kind_of_change_is_an_event(client, db, books, wallet):
    alice, bob = wallet("alice", funded=500), wallet("bob")
    payment = books.transfer(alice["id"], bob["id"], 10)
    books.reverse(payment["id"])
    captured = books.create_hold(alice["id"], 50, datetime.now(UTC) + timedelta(days=1))
    books.capture_hold(captured["id"], bob["id"], 30)
    voided = books.create_hold(alice["id"], 5, datetime.now(UTC) + timedelta(days=1))
    books.void_hold(voided["id"])
    expiring = books.create_hold(alice["id"], 5, datetime.now(UTC) + timedelta(days=1))
    db.run(  # as the owner: the API's role can't change an expiry
        lambda conn: conn.execute(
            "UPDATE holds SET expires_at = now() - interval '1 second' WHERE id = %s", (expiring["id"],)
        )
    )
    books.expire_holds()

    assert types(feed(client)) == [
        "account.created",  # cash
        "account.created",  # alice
        "transaction.posted",  # funding
        "account.created",  # bob
        "transaction.posted",
        "transaction.reversed",
        "hold.created",
        "hold.captured",
        "transaction.posted",  # what the capture posted
        "hold.created",
        "hold.voided",
        "hold.created",
        "hold.expired",
    ]


def test_a_rolled_back_change_leaves_no_event(client, wallet):
    alice, bob = wallet("alice", funded=100), wallet("bob")
    start = feed(client)["next_cursor"]

    refused = client.post(
        "/v1/transfers",
        json={"debit_account_id": alice["id"], "credit_account_id": bob["id"], "amount": 1_000},
        headers=key(),
    )

    assert refused.json()["code"] == "INSUFFICIENT_FUNDS"
    assert feed(client, start) == {"data": [], "next_cursor": start}


def test_a_transaction_failing_at_commit_leaves_no_event(migrated, client, wallet):
    alice, bob = wallet("alice", funded=100), wallet("bob")
    start = feed(client)["next_cursor"]

    with psycopg.connect(migrated) as raw:
        txn = raw.execute("INSERT INTO transactions (client_id) VALUES (1) RETURNING id").fetchone()[0]
        raw.execute(
            "INSERT INTO entries (transaction_id, account_id, direction, amount, currency, balance_after) "
            "VALUES (%s, %s, 'debit', 10, 'USD', 0), (%s, %s, 'credit', 9, 'USD', 0)",
            (txn, alice["id"], txn, bob["id"]),
        )
        with pytest.raises(psycopg.errors.IntegrityConstraintViolation):
            raw.commit()  # the balance check fires at commit; the deferred event goes with it

    assert feed(client, start)["data"] == []


def test_events_are_per_client(client, db, wallet):
    wallet("alice", funded=100)
    _, other_key = db.run(lambda conn: auth.create_client(conn, "other"))

    assert feed(client, headers={"Authorization": f"Bearer {other_key}"})["data"] == []
    assert len(feed(client)["data"]) == 3


def test_paging_resumes_exactly_where_it_left_off(client, books, wallet):
    alice, bob = wallet("alice", funded=100), wallet("bob")
    for _ in range(3):
        books.transfer(alice["id"], bob["id"], 1)

    seen, cursor = [], None
    while True:
        page = feed(client, cursor, limit=2)
        seen += [e["id"] for e in page["data"]]
        if not page["data"]:
            break
        cursor = page["next_cursor"]

    assert seen == sorted(seen) and len(seen) == len(set(seen)) == 7  # 3 accounts, funding, 3 transfers
    assert page["next_cursor"] == cursor  # caught up: same position, ready for the next poll


def test_a_later_commit_never_overtakes_an_earlier_transaction(migrated, client, db, wallet):
    # The classic outbox bug: id order is insert order, not commit order. Here an older
    # transaction (slow) commits after a younger one (fast). A reader that went by id
    # alone would hand out fast's event, move its cursor past slow's id, and never see it.
    alice, bob = wallet("alice", funded=100), wallet("bob")
    carol, dave = wallet("carol", funded=100), wallet("dave")  # other accounts: fast mustn't wait on slow's locks
    start = feed(client)["next_cursor"]

    with psycopg.connect(migrated, row_factory=dict_row) as slow_conn:
        slow = Ledger(DEFAULT_CLIENT).transfer(slow_conn, alice["id"], bob["id"], 1, "slow")  # open, uncommitted
        fast = Books(db).transfer(carol["id"], dave["id"], 1, "fast")  # committed

        assert feed(client, start)["data"] == []  # fast's event waits for the older transaction
        slow_conn.commit()

    after = feed(client, start)["data"]
    assert [e["data"]["id"] for e in after] == [str(slow["id"]), str(fast["id"])]


def test_events_older_than_the_retention_period_are_purged_in_batches(db, books, wallet):
    wallet("alice", funded=100)  # 3 events
    db.run(lambda conn: conn.execute("UPDATE outbox_events SET created_at = now() - interval '8 days' WHERE id <= 2"))

    batches = [db.run(lambda conn: events.purge_expired(conn, batch_size=1)) for _ in range(3)]

    assert batches == [1, 1, 0]
    assert db.run(lambda conn: conn.execute("SELECT count(*) AS n FROM outbox_events").fetchone()["n"]) == 1


def test_a_forged_cursor_is_rejected(client):
    response = client.get("/v1/events", params={"cursor": "WyJ4IiwgMV0"})  # ["x", 1]

    assert response.status_code == 422 and response.json()["code"] == "INVALID_CURSOR"
