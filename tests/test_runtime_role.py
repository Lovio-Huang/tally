"""The API's database role (migration 012) can run the ledger and nothing more.

Everything here connects as the login role the tests set up like production's (a member
of tally_app, see conftest), and tries what a compromised or buggy API might.
"""

import psycopg
import pytest
from fastapi.testclient import TestClient

from tally import events
from tally.api import create_app
from tally.config import Settings
from tally.db import MigrationError, migrate


@pytest.fixture
def runtime(runtime_url):
    with psycopg.connect(runtime_url) as conn:
        yield conn


def owner_of(url: str) -> str:
    return psycopg.conninfo.conninfo_to_dict(url)["user"]


def test_the_runtime_role_is_an_ordinary_role_that_owns_nothing(runtime, migrated):
    attributes = runtime.execute(
        "SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls FROM pg_roles "
        "WHERE rolname = current_user"
    ).fetchone()
    owned = runtime.execute(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND pg_has_role(current_user, c.relowner, 'USAGE')"
    ).fetchone()[0]
    can_become_owner = runtime.execute("SELECT pg_has_role(current_user, %s, 'MEMBER')", (owner_of(migrated),))

    assert attributes == (False, False, False, False, False)
    assert owned == 0
    assert can_become_owner.fetchone() == (False,)


@pytest.mark.parametrize(
    "statement",
    [
        # Switching the database's own rules off
        "ALTER TABLE entries DISABLE TRIGGER entries_append_only",
        "ALTER TABLE entries DISABLE TRIGGER ALL",
        "ALTER TABLE accounts DROP CONSTRAINT accounts_no_overdraft",
        "SET session_replication_role = replica",
        "DROP TRIGGER entries_balanced ON entries",
        "CREATE OR REPLACE FUNCTION forbid_changes() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$",
        "ALTER FUNCTION audit_change() SECURITY INVOKER",
        "CREATE TABLE shadow (id int)",
        # Becoming someone else
        "SET ROLE {owner}",
        "GRANT {owner} TO current_user",
        "ALTER ROLE current_user SUPERUSER",
        # Rewriting history
        "DELETE FROM entries",
        "DELETE FROM transactions",
        "DELETE FROM holds",
        "TRUNCATE entries",
        "UPDATE holds SET amount = 1",
        "UPDATE holds SET expires_at = now()",
        "UPDATE holds SET settle_to_account_id = NULL",
        "UPDATE accounts SET allow_negative = true",
        "UPDATE accounts SET currency = 'EUR'",
        # The audit trail and the event feed
        "SELECT * FROM audit_log",
        "INSERT INTO audit_log (client_id, action, resource_id) VALUES (1, 'account.created', '1')",
        "DELETE FROM audit_log",
        "INSERT INTO outbox_events (client_id, type, payload) VALUES (1, 'x', '{{}}')",
        "DELETE FROM outbox_events",
        # Keys, clients and the migration history: the admin CLI's, run as the owner
        "INSERT INTO api_clients (name) VALUES ('intruder')",
        "INSERT INTO api_keys (client_id, key_hash, key_prefix) VALUES (1, 'h', 'p')",
        "UPDATE api_keys SET revoked_at = NULL",
        "DELETE FROM schema_migrations",
    ],
)
def test_the_runtime_role_is_refused(runtime, migrated, statement):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        runtime.execute(statement.format(owner=owner_of(migrated)))


@pytest.mark.parametrize(
    "statement",
    [
        # Granted only so row locks and the balance triggers work; a direct write is refused.
        "UPDATE accounts SET balance = 1",
        "UPDATE accounts SET held = 1",
        "UPDATE transactions SET description = 'edited'",
    ],
)
def test_columns_granted_for_locking_still_cant_be_written(runtime, books, wallet, statement):
    wallet("alice", funded=100)
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        runtime.execute(statement)


def test_the_api_starts_as_the_runtime_role_and_checks_the_schema_is_current(runtime_url, api_key):
    # run_migrations stays on: with nothing pending, the check only reads schema_migrations.
    app = create_app(Settings(database_url=runtime_url, pool_size=2), maintenance=False)
    with TestClient(app, headers={"Authorization": f"Bearer {api_key}"}) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/v1/accounts").status_code == 200
    assert migrate(runtime_url) == []


def test_the_runtime_role_cant_migrate_and_says_who_can(migrated, runtime_url):
    # A database whose schema is behind: the runtime role is told to run migrations as the owner.
    name = "tally_runtime_behind"
    with psycopg.connect(migrated, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        conn.execute(f'CREATE DATABASE "{name}"')
    try:
        migrate(psycopg.conninfo.make_conninfo(migrated, dbname=name), until="011_sealed_transactions")
        with pytest.raises(MigrationError, match="schema owner"):
            migrate(psycopg.conninfo.make_conninfo(runtime_url, dbname=name))
    finally:
        with psycopg.connect(migrated, autocommit=True) as conn:
            conn.execute(f'DROP DATABASE "{name}" WITH (FORCE)')


def test_the_runtime_role_purges_only_expired_events(db, app_db, books, wallet):
    wallet("alice", funded=100)  # an account and a transaction: two fresh events
    db.run(
        lambda conn: conn.execute(
            "UPDATE outbox_events SET created_at = now() - interval '8 days' "
            "WHERE id = (SELECT min(id) FROM outbox_events)"
        )
    )
    count = "SELECT count(*) AS n FROM outbox_events"
    before = db.run(lambda conn: conn.execute(count).fetchone()["n"])

    assert app_db.run(events.purge_expired) == 1
    assert db.run(lambda conn: conn.execute(count).fetchone()["n"]) == before - 1
