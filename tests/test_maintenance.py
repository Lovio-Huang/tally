"""The API's background job: expires overdue holds and forgets idempotency keys past the replay window."""

import logging
import threading
import time
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from tally import api
from tally.ledger import Ledger


class SmallBatches(Ledger):
    """Expires two holds per call, so a handful of holds spans several batches."""

    calls: list[int]

    def __init__(self) -> None:
        super().__init__()  # no client: expiry is global
        self.calls = []

    def expire_holds(self, conn, limit: int = 2) -> int:
        expired = super().expire_holds(conn, limit)
        self.calls.append(expired)
        return expired


def run_loop_until(db, ledger, done, interval: float, within: float) -> None:
    """Runs the loop in a thread until ``done()`` holds; fails if that takes longer than ``within`` seconds."""
    stop = threading.Event()
    thread = threading.Thread(target=api._maintenance_loop, args=(db, ledger, interval, stop), daemon=True)
    started = time.monotonic()
    thread.start()
    try:
        while not done():
            assert time.monotonic() - started < within, "the maintenance loop didn't finish in time"
            time.sleep(0.02)
    finally:
        stop.set()
        thread.join(timeout=5)
    assert not thread.is_alive(), "the loop didn't stop when asked"


def active_holds(db) -> int:
    return db.run(lambda conn: conn.execute("SELECT count(*) AS n FROM holds WHERE status = 'active'").fetchone()["n"])


def make_overdue(db, books, account_id: int, count: int) -> None:
    for _ in range(count):
        books.create_hold(account_id, 10, datetime.now(UTC) + timedelta(days=1))
    db.run(
        lambda conn: conn.execute(
            "UPDATE holds SET expires_at = now() - interval '1 second' WHERE expires_at IS NOT NULL"
        )
    )


def test_a_backlog_of_overdue_holds_is_drained_in_one_run(db, books, wallet, monkeypatch):
    monkeypatch.setattr(api, "EXPIRY_BATCH_SIZE", 2)
    alice = wallet("alice", funded=1_000)
    make_overdue(db, books, alice["id"], 5)
    ledger = SmallBatches()

    # One run comes after 0.5 s. Without draining, 5 holds at 2 per run would take
    # three runs (1.5 s); with it, a full batch is followed at once by the next.
    run_loop_until(db, ledger, lambda: active_holds(db) == 0, interval=0.5, within=1.25)

    assert ledger.calls[:3] == [2, 2, 1]
    assert books.get_account(alice["id"])["held"] == 0


def test_idempotency_keys_past_the_replay_window_are_forgotten(db):
    def seed(conn):
        conn.execute(
            "INSERT INTO idempotency_keys (key, request_hash, created_at) VALUES "
            "('transfers:old', 'h', now() - interval '25 hours'), ('transfers:recent', 'h', now() - interval '1 hour')"
        )

    db.run(seed)

    def keys():
        return {r["key"] for r in db.run(lambda conn: conn.execute("SELECT key FROM idempotency_keys").fetchall())}

    run_loop_until(db, Ledger(), lambda: "transfers:old" not in keys(), interval=0.05, within=3)

    assert keys() == {"transfers:recent"}


def test_a_failed_run_is_logged_and_the_next_one_still_happens(db, books, wallet, caplog):
    alice = wallet("alice", funded=1_000)
    make_overdue(db, books, alice["id"], 1)

    class FailsOnce(Ledger):
        failed = False

        def expire_holds(self, conn, limit: int = 500) -> int:
            if not self.failed:
                self.failed = True
                raise RuntimeError("database hiccup")
            return super().expire_holds(conn, limit)

    with caplog.at_level(logging.ERROR, logger="tally.api"):
        run_loop_until(db, FailsOnce(), lambda: active_holds(db) == 0, interval=0.05, within=3)

    assert any(r.getMessage() == "maintenance task failed: expire_holds" for r in caplog.records)


@pytest.mark.parametrize("error", [psycopg.OperationalError("connection lost"), RuntimeError("bug in settlement")])
def test_a_failing_task_doesnt_stop_the_others_in_the_same_run(db, books, wallet, caplog, error):
    # Settlement runs first; when it fails (database gone, a bug), expiry and both purges
    # must still run in that pass, and the payments it didn't settle stay active.
    alice = wallet("alice", funded=1_000)
    merchant = books.create_account("Merchant", "liability", "USD")
    payment = books.create_hold(alice["id"], 10, None, "", merchant["id"])
    make_overdue(db, books, alice["id"], 1)
    db.run(
        lambda conn: conn.execute(
            "INSERT INTO idempotency_keys (key, request_hash, created_at) "
            "VALUES ('transfers:old', 'h', now() - interval '25 hours')"
        )
    )

    class SettlementDown(Ledger):
        def settle_holds(self, conn, limit: int = 100) -> int:
            raise error

    with caplog.at_level(logging.ERROR, logger="tally.api"):
        api.run_maintenance(db, SettlementDown())

    assert [r.getMessage() for r in caplog.records] == ["maintenance task failed: settle_payments"]
    assert active_holds(db) == 1 and books.get_hold(payment["id"])["status"] == "active"
    assert db.run(lambda conn: conn.execute("SELECT count(*) AS n FROM idempotency_keys").fetchone()["n"]) == 0
    assert books.get_account(alice["id"])["held"] == 10


@pytest.mark.parametrize("interval", [0.05, 30])
def test_the_loop_stops_promptly_when_asked(db, interval):
    # Shutdown sets the event; the loop must not sleep out a long interval first.
    stop = threading.Event()
    thread = threading.Thread(target=api._maintenance_loop, args=(db, Ledger(), interval, stop), daemon=True)
    thread.start()
    time.sleep(0.1)
    started = time.monotonic()
    stop.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert time.monotonic() - started < 1
