"""The migration runner and the indexes it creates."""

import uuid
from uuid import UUID

import psycopg
import pytest

from tally.db import Database, MigrationError, _migration_files, _statements, migrate
from tally.ledger import Ledger
from tally.reconciliation import reconcile

# Built CONCURRENTLY (migrations 002 and 007), so they could be left INVALID by a failed build.
INDEXES = (
    "accounts_client_id_key",
    "entries_by_account_time",
    "transactions_by_client_time",
    "transactions_client_id_key",
)
ALL_VERSIONS = [version for version, _ in _migration_files()]


@pytest.fixture
def scratch_database(migrated):
    """An empty database next to the test one, for exercising the runner from scratch."""
    name = f"tally_migrations_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(migrated, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    url = psycopg.conninfo.make_conninfo(migrated, dbname=name)
    yield url
    with psycopg.connect(migrated, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE "{name}" WITH (FORCE)')


def applied(url: str) -> list[str]:
    with psycopg.connect(url) as conn:
        return [row[0] for row in conn.execute("SELECT version FROM schema_migrations ORDER BY version")]


def test_migrations_apply_in_order_stop_where_asked_and_are_idempotent(scratch_database):
    assert migrate(scratch_database, until="001_ledger") == ["001_ledger"]
    assert migrate(scratch_database) == ALL_VERSIONS[1:]
    assert migrate(scratch_database) == []
    assert applied(scratch_database) == ALL_VERSIONS


def test_concurrently_built_indexes_exist_and_are_valid(migrated):
    # A failed CREATE INDEX CONCURRENTLY leaves an index that exists but is INVALID.
    with psycopg.connect(migrated) as conn:
        rows = conn.execute(
            "SELECT c.relname, i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relname = ANY(%s)",
            (list(INDEXES),),
        ).fetchall()
    assert dict(rows) == {name: True for name in INDEXES}


@pytest.mark.parametrize(
    ("query", "index"),
    [
        (
            "SELECT balance_after FROM entries WHERE account_id = 1 AND posted_at <= now() "
            "ORDER BY posted_at DESC, id DESC LIMIT 1",
            "entries_by_account_time",
        ),
        (
            "SELECT id FROM transactions WHERE client_id = 1 ORDER BY posted_at DESC, id DESC LIMIT 20",
            "transactions_by_client_time",
        ),
    ],
)
def test_hot_queries_can_use_their_index(migrated, query, index):
    # On a near-empty table a sequential scan (or a bitmap scan plus a sort) is cheaper,
    # so take those options away and check that the index fits the query's shape:
    # filter and sort order, read in index order with no Sort step.
    with psycopg.connect(migrated) as conn:
        conn.execute("SET enable_seqscan = off")
        conn.execute("SET enable_bitmapscan = off")
        plan = "\n".join(row[0] for row in conn.execute(f"EXPLAIN {query}"))
    assert index in plan
    assert "Sort" not in plan


def test_no_transaction_files_are_split_into_statements():
    sql = """-- migration: no-transaction
    -- a comment; with a semicolon
    DROP INDEX CONCURRENTLY IF EXISTS a;
    CREATE INDEX CONCURRENTLY a ON t (x);
    """
    assert _statements(sql) == ["DROP INDEX CONCURRENTLY IF EXISTS a", "CREATE INDEX CONCURRENTLY a ON t (x)"]


def test_instances_starting_at_the_same_time_migrate_once_without_deadlocking(scratch_database):
    # Several app workers boot together and each runs migrate(). The ones waiting for
    # the migration lock must not block CREATE INDEX CONCURRENTLY, which waits for every
    # open transaction to finish, while the index builder holds the lock they want.
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    gate = Barrier(4)

    def boot(_):
        gate.wait()
        return migrate(scratch_database)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(boot, range(4)))

    assert sorted(len(r) for r in results) == [0, 0, 0, len(ALL_VERSIONS)]  # one instance did all the work
    assert applied(scratch_database) == ALL_VERSIONS


def test_editing_an_already_applied_migration_is_refused(scratch_database):
    migrate(scratch_database)
    with psycopg.connect(scratch_database) as conn:  # as if 001's file had been edited since
        conn.execute("UPDATE schema_migrations SET checksum = 'something else' WHERE version = '001_ledger'")

    with pytest.raises(MigrationError, match="001_ledger was changed after it was applied"):
        migrate(scratch_database)


def test_databases_migrated_before_checksums_existed_get_them_filled_in(scratch_database):
    migrate(scratch_database)
    with psycopg.connect(scratch_database) as conn:  # the table as older versions created it
        conn.execute("ALTER TABLE schema_migrations DROP COLUMN checksum")

    assert migrate(scratch_database) == []
    with psycopg.connect(scratch_database) as conn:
        missing = conn.execute("SELECT count(*) FROM schema_migrations WHERE checksum IS NULL").fetchone()[0]
    assert missing == 0


def test_an_unknown_target_version_is_an_error(scratch_database):
    with pytest.raises(ValueError, match="unknown migration"):
        migrate(scratch_database, until="999_typo")


def test_function_bodies_are_refused_in_no_transaction_files():
    with pytest.raises(ValueError, match="function bodies"):
        _statements("-- migration: no-transaction\nCREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;")


def test_payments_waiting_at_the_upgrade_lose_their_expiry_and_still_settle(scratch_database):
    # Before 010 a payment carried an expiry and settlement skipped expired ones: one whose
    # expiry passed while maintenance was behind expired unsettled after a 201.
    migrate(scratch_database, until="009_batched_settlement")
    db = Database(scratch_database, min_size=1, max_size=2)
    try:
        books = Ledger(1)  # the 'default' client migration 006 creates

        def seed(conn):
            cash = books.create_account(conn, "cash", "asset", "USD", allow_negative=True)
            wallet = books.create_account(conn, "wallet", "liability", "USD")
            merchant = books.create_account(conn, "merchant", "liability", "USD")
            books.transfer(conn, cash["id"], wallet["id"], 1_000)

            def hold(expiry: str, destination: int | None, status: str = "active") -> UUID:
                hold_id = conn.execute(
                    f"INSERT INTO holds (account_id, amount, expires_at, settle_to_account_id) "
                    f"VALUES (%s, 10, now() + interval '{expiry}', %s) RETURNING id",
                    (wallet["id"], destination),
                ).fetchone()["id"]
                if status != "active":
                    conn.execute("UPDATE holds SET status = %s WHERE id = %s", (status, hold_id))
                return hold_id

            return merchant, {
                "overdue payment": hold("-1 minute", merchant["id"]),
                "waiting payment": hold("1 day", merchant["id"]),
                "voided payment": hold("1 day", merchant["id"], "voided"),
                "card hold": hold("1 day", None),
            }

        merchant, holds = db.run(seed)
        migrate(scratch_database)

        def expiries(conn) -> dict:
            rows = conn.execute("SELECT id, expires_at FROM holds").fetchall()
            by_id = {row["id"]: row["expires_at"] for row in rows}
            return {name: by_id[hold_id] is None for name, hold_id in holds.items()}

        # Waiting payments stop their clocks; finished ones and ordinary holds keep theirs.
        assert db.run(expiries) == {
            "overdue payment": True,
            "waiting payment": True,
            "voided payment": False,
            "card hold": False,
        }
        assert db.run(Ledger().expire_holds) == 0
        assert db.run(Ledger().settle_holds) == 2
        assert db.run(lambda conn: books.get_account(conn, merchant["id"]))["balance"] == 20
        assert db.run(reconcile)["ok"]
    finally:
        db.close()
