"""Connection pool, transactions with retry, and a minimal SQL migration runner."""

from __future__ import annotations

import contextlib
import hashlib
import logging
import random
import time
import weakref
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from typing import TypeVar

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .observability import DB_RETRIES

log = logging.getLogger(__name__)

T = TypeVar("T")

# Postgres asks the client to retry these: the transaction did nothing and can safely run again.
RETRYABLE = (psycopg.errors.DeadlockDetected, psycopg.errors.SerializationFailure)


def session_limits(
    lock_timeout_ms: int, statement_timeout_ms: int, idle_in_transaction_timeout_ms: int = 60_000
) -> str:
    """libpq ``options`` that make the server enforce these limits for the whole session."""
    return (
        f"-c lock_timeout={lock_timeout_ms} -c statement_timeout={statement_timeout_ms} "
        f"-c idle_in_transaction_session_timeout={idle_in_transaction_timeout_ms}"
    )


class Database:
    def __init__(
        self,
        url: str,
        min_size: int = 2,
        max_size: int = 20,
        *,
        lock_timeout_ms: int = 5_000,
        statement_timeout_ms: int = 30_000,
        idle_in_transaction_timeout_ms: int = 60_000,
        check_idle_after_seconds: float = 1.0,
        pool_timeout_seconds: float = 10.0,
    ) -> None:
        # Server-side limits on every pooled connection. Without them, a transaction
        # stuck behind a row lock (an open psql session, a hung client) waits forever
        # while holding a pool connection; enough of those and the pool runs dry.
        limits = session_limits(lock_timeout_ms, statement_timeout_ms, idle_in_transaction_timeout_ms)
        self._last_used: weakref.WeakKeyDictionary[psycopg.Connection, float] = weakref.WeakKeyDictionary()
        self._check_idle_after = check_idle_after_seconds
        self.pool = ConnectionPool(
            url,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row, "options": limits},
            check=self._check_if_idle,
            # How long a request waits for a free connection before PoolTimeout (the API
            # answers 503). psycopg_pool's default of 30 s would let an overloaded
            # service pile up requests for half a minute each.
            timeout=pool_timeout_seconds,
            open=True,
        )

    def close(self) -> None:
        self.pool.close()

    def _check_if_idle(self, conn: psycopg.Connection) -> None:
        """Pings a connection before handing it out, unless it was in use a moment ago.

        Dead connections come from proxy and firewall idle timeouts, restarts and
        failovers; without a check, the next request to draw one fails. Pinging on
        every checkout costs a round trip per request (measured: about 9% of transfer
        throughput), so, like HikariCP, only connections idle for longer than
        ``check_idle_after_seconds`` are pinged. Under load that is almost none of them,
        and idle timeouts only ever kill idle connections. A connection that breaks
        while busy still fails the request using it, which no check could prevent.
        """
        last_used = self._last_used.get(conn)
        if last_used is None or time.monotonic() - last_used > self._check_idle_after:
            ConnectionPool.check_connection(conn)

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection]:
        """One database transaction: commits on success, rolls back on any exception."""
        with self.pool.connection() as conn:
            try:
                yield conn
            finally:
                self._last_used[conn] = time.monotonic()

    def run(self, work: Callable[[psycopg.Connection], T], attempts: int = 3) -> T:
        """Runs ``work(conn)`` in one transaction, retrying on deadlock or serialization failure.

        Postgres rolls such a transaction back completely, so running it again is safe,
        provided ``work`` only touches the database: an HTTP call or a change to Python
        state inside it would happen once per attempt.
        """
        if attempts < 1:
            raise ValueError("attempts must be at least 1")
        for attempt in range(1, attempts + 1):
            try:
                with self.transaction() as conn:
                    return work(conn)
            except RETRYABLE as error:
                if attempt == attempts:
                    raise
                DB_RETRIES.labels(type(error).__name__).inc()
                # Exponential backoff with full jitter: transactions that collided once
                # must not all retry at the same instant and collide again.
                delay = random.uniform(0, 0.02 * 2**attempt)
                log.warning(
                    "retrying after %s (attempt %d of %d, waiting %.0f ms)",
                    type(error).__name__,
                    attempt,
                    attempts,
                    delay * 1000,
                )
                time.sleep(delay)
        raise AssertionError("unreachable")


def _migration_files() -> list[tuple[str, str]]:
    source = Path(__file__).resolve().parents[2] / "migrations"
    if source.is_dir():  # running from a checkout
        files = sorted(source.glob("*.sql"))
        return [(f.stem, f.read_text()) for f in files]
    packaged = resources.files("tally") / "migrations"  # installed wheel
    files = sorted((f for f in packaged.iterdir() if f.name.endswith(".sql")), key=lambda f: f.name)
    return [(f.name.removesuffix(".sql"), f.read_text()) for f in files]


MIGRATION_LOCK_ID = 7_301_984_221  # any constant; shared by every app instance

NO_TRANSACTION = "-- migration: no-transaction"


class MigrationError(RuntimeError):
    """The database's migration history doesn't match the migration files in this code."""


def _checksum(sql: str) -> str:
    return hashlib.sha256(sql.encode()).hexdigest()


def _statements(sql: str) -> list[str]:
    """Splits a simple DDL file on semicolons, for no-transaction migrations."""
    if "$$" in sql:
        # A function body contains semicolons; splitting on them would corrupt it.
        raise ValueError("no-transaction migrations must be plain DDL, without function bodies")
    code = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    return [statement.strip() for statement in code.split(";") if statement.strip()]


def _acquire_migration_lock(conn: psycopg.Connection, timeout_seconds: float = 600) -> None:
    """Waits for the migration lock by polling rather than blocking in pg_advisory_lock().

    A session blocked inside pg_advisory_lock() is mid-statement, so it counts as an
    open transaction, and CREATE INDEX CONCURRENTLY waits for every open transaction
    to finish. The instance building the index would wait for the waiters while they
    wait for its lock: a deadlock. pg_try_advisory_lock() returns immediately, so
    between attempts there is nothing for the index build to wait on.
    """
    deadline = time.monotonic() + timeout_seconds
    while not conn.execute("SELECT pg_try_advisory_lock(%s)", (MIGRATION_LOCK_ID,)).fetchone()[0]:
        if time.monotonic() > deadline:
            raise TimeoutError("timed out waiting for another instance to finish migrating")
        time.sleep(0.2)


def migrate(url: str, until: str | None = None) -> list[str]:
    """Applies pending migrations in order, stopping after version ``until`` if given.

    Each migration runs in its own transaction, so a failure leaves no half-applied
    schema. The exception is a file starting with ``-- migration: no-transaction``:
    its statements run one by one in autocommit mode, because ``CREATE INDEX
    CONCURRENTLY`` (building an index without blocking writes) refuses to run inside
    a transaction. Such files must be safe to re-run after a crash.

    A Postgres advisory lock makes this safe when several instances start at once:
    one applies the migrations, the others wait and then find nothing to do.

    Before applying anything it checks that no already-applied file was edited since
    (see ``_verify_history``).
    """
    files = _migration_files()
    if until is not None and until not in {version for version, _ in files}:
        raise ValueError(f"unknown migration {until!r}")
    applied: list[str] = []
    with psycopg.connect(url, autocommit=True) as conn:
        _acquire_migration_lock(conn)
        try:
            # Skipped when already in place: the API's role may read the table but not alter it.
            if not _history_table_ready(conn):
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS schema_migrations "
                    "(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), checksum TEXT)"
                )
                # Tables created before checksums were recorded.
                conn.execute("ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT")
            recorded = dict(conn.execute("SELECT version, checksum FROM schema_migrations").fetchall())
            _verify_history(conn, files, recorded)
            for version, sql in files:
                if version not in recorded:
                    _apply(conn, version, sql)
                    applied.append(version)
                if version == until:
                    break
        except psycopg.errors.InsufficientPrivilege as exc:
            raise MigrationError(
                f"this database role can't apply migrations ({exc}); "
                "run `tally migrate` with the schema owner's TALLY_DATABASE_URL first"
            ) from exc
        finally:
            # If the connection broke, closing it releases the lock anyway; an error here
            # must not hide the one that actually stopped the migration.
            with contextlib.suppress(psycopg.Error):
                conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_ID,))
    return applied


def _history_table_ready(conn: psycopg.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('schema_migrations') "
        "AND attname = 'checksum' AND NOT attisdropped"
    ).fetchone()
    return row is not None


def _verify_history(conn: psycopg.Connection, files: list[tuple[str, str]], recorded: dict) -> None:
    """Refuses to continue if a migration file changed after the database applied it.

    A database that already ran a file never sees later edits to it, so an edited
    migration silently gives different environments different schemas. Rows written
    before checksums existed are trusted once and filled in with the current file.
    """
    for version, sql in files:
        if version not in recorded:
            continue
        if recorded[version] is None:
            conn.execute("UPDATE schema_migrations SET checksum = %s WHERE version = %s", (_checksum(sql), version))
        elif recorded[version] != _checksum(sql):
            raise MigrationError(
                f"migration {version} was changed after it was applied; put the change in a new migration instead"
            )


def _apply(conn: psycopg.Connection, version: str, sql: str) -> None:
    record = ("INSERT INTO schema_migrations (version, checksum) VALUES (%s, %s)", (version, _checksum(sql)))
    if sql.lstrip().startswith(NO_TRANSACTION):
        for statement in _statements(sql):
            conn.execute(statement)
        conn.execute(*record)
    else:
        with conn.transaction():
            conn.execute(sql)
            conn.execute(*record)
    log.info("applied migration %s", version)
