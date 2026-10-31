#!/usr/bin/env python3
"""Upgrade acceptance: a ledger built by the old code (up to migration 009), upgraded to this one.

The old code (git archive of --old-ref) runs in a subprocess and builds a ledger the way it
really would, through its own API: an ordinary active hold, one about to expire, a captured
hold, a settled payment, a payment it let expire unsettled, payments still waiting (one past
its old expiry), a payment to a destination it should have refused, and a reversed transfer.
Then this code upgrades it the way docs/UPGRADING.md says and checks that:

  * the migrations change nothing but what they should (waiting payments lose their expiry);
  * the API and maintenance, connected as a runtime role, settle, void, expire and serve it;
  * history is sealed and the runtime role can't touch it;
  * a migration that fails leaves no trace of itself, and rerunning after the fix completes;
  * running the deploy steps again changes nothing.

It creates and drops its own databases and login role on the server --admin-url points at
(a superuser, as the schema owner would be), and touches nothing else there.

  python scripts/upgrade_acceptance.py --admin-url postgresql://tally:tally@127.0.0.1:5432/postgres
"""

import argparse
import json
import logging
import os
import secrets
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psycopg
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.rows import dict_row

from tally.api import create_app, run_maintenance
from tally.config import Settings
from tally.db import Database, MigrationError, migrate
from tally.ledger import Ledger
from tally.reconciliation import reconcile

ROOT = Path(__file__).resolve().parents[1]
NEW = ["010_payment_lifecycle", "011_sealed_transactions", "012_runtime_role"]

# Runs under the old code. Builds the ledger through the old API, as the old deployment would.
SEED = r"""
import json, sys, time, uuid
import tally
from fastapi.testclient import TestClient
from tally import auth
from tally.api import create_app
from tally.config import Settings
from tally.db import Database
from tally.ledger import Ledger

url, root = sys.argv[1], sys.argv[2]
assert tally.__file__.startswith(root), f"expected the old code, imported {tally.__file__}"
app = create_app(Settings(database_url=url, pool_size=2), maintenance=False)  # migrates to its own latest
db = Database(url, min_size=1, max_size=2)
with TestClient(app) as http:
    _, key = db.run(lambda conn: auth.create_client(conn, "acme"))

    def call(method, path, body=None, status=201):
        headers = {"Authorization": f"Bearer {key}", "Idempotency-Key": str(uuid.uuid4())}
        response = http.request(method, path, json=body, headers=headers)
        assert response.status_code == status, (path, response.status_code, response.text)
        return response.json()

    def account(name, kind, negative=False):
        body = {"name": name, "type": kind, "currency": "USD", "allow_negative": negative}
        return call("POST", "/v1/accounts", body)["id"]

    def hold(account_id, amount, seconds=None, to=None):
        body = {"account_id": account_id, "amount": amount}
        body |= {"expires_in_seconds": seconds} if seconds else {}
        body |= {"settle_to_account_id": to} if to else {}
        return call("POST", "/v1/holds", body)["id"]

    cash, a, b = account("cash", "asset", True), account("wallet A", "liability"), account("wallet B", "liability")
    merchant, empty = account("merchant", "liability"), account("empty asset", "asset")
    for wallet in (a, b):
        call("POST", "/v1/transfers", {"debit_account_id": cash, "credit_account_id": wallet, "amount": 1000})
    ids = {}
    moved = call("POST", "/v1/transfers", {"debit_account_id": a, "credit_account_id": b, "amount": 100})
    ids["reversed transaction"] = moved["id"]
    ids["reversal"] = call("POST", f"/v1/transactions/{moved['id']}/reversal", {})["id"]
    ids["settled payment"] = hold(a, 50, to=merchant)
    db.run(lambda conn: Ledger().settle_holds(conn))
    ids["expired payment"] = hold(a, 30, seconds=1, to=merchant)
    time.sleep(1.2)
    db.run(lambda conn: Ledger().expire_holds(conn))  # old semantics: accepted, then expired unsettled
    ids["captured hold"] = hold(a, 10)
    call("POST", f"/v1/holds/{ids['captured hold']}/capture", {"destination_account_id": merchant}, 200)
    ids["waiting payment"] = hold(b, 70, to=merchant)
    ids["payment past its old expiry"] = hold(b, 20, seconds=2, to=merchant)
    ids["invalid payment"] = hold(a, 40, to=empty)  # accepted by the old API (review finding P0)
    ids["active hold"] = hold(a, 25)
    ids["hold due soon"] = hold(b, 15, seconds=2)
db.close()
accounts = {"cash": cash, "a": a, "b": b, "merchant": merchant, "empty": empty}
print(json.dumps({"key": key, "accounts": accounts, "ids": ids, "seeded_at": time.time()}))
"""

# Runs under the old code, against the upgraded database: what happens if it's started again.
OLD_AFTER = r"""
import json, sys, uuid
import tally
from fastapi.testclient import TestClient
from tally.api import create_app
from tally.config import Settings
from tally.db import Database
from tally.ledger import Ledger

owner, runtime, root, key, payer, merchant = sys.argv[1:7]
assert tally.__file__.startswith(root), f"expected the old code, imported {tally.__file__}"
result = {}
try:  # started as the runtime role, the way the new deployment connects
    with TestClient(create_app(Settings(database_url=runtime, pool_size=2), maintenance=False)):
        result["starts_as_runtime_role"] = True
except Exception as error:
    result["starts_as_runtime_role"] = False
    result["runtime_role_error"] = type(error).__name__
app = create_app(Settings(database_url=owner, pool_size=2), run_migrations=False, maintenance=False)
with TestClient(app) as http:
    body = {"account_id": int(payer), "amount": 1, "settle_to_account_id": int(merchant)}
    headers = {"Authorization": f"Bearer {key}", "Idempotency-Key": str(uuid.uuid4())}
    response = http.post("/v1/holds", json=body, headers=headers)
    result["payment"] = [response.status_code, response.json().get("code")]
db = Database(owner, min_size=1, max_size=1)
result["settled_by_old_code"] = db.run(lambda conn: Ledger().settle_holds(conn))
db.close()
print(json.dumps(result))
"""

failures: list[str] = []


def check(name: str, ok: bool, detail: object = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail != "" else ""))
    if not ok:
        failures.append(name)


def snapshot(url: str) -> dict:
    """Everything the upgrade and repeated deploy steps must preserve, read as the owner."""
    with psycopg.connect(url, row_factory=dict_row) as conn:
        rows = {
            "accounts": "SELECT id, balance, held FROM accounts ORDER BY id",
            "entries": "SELECT id, transaction_id, account_id, direction, amount, balance_after, posted_at "
            "FROM entries ORDER BY id",
            "transactions": "SELECT id, reverses_id, description, posted_at FROM transactions ORDER BY id",
            "holds": "SELECT id, status, amount, account_id, settle_to_account_id, captured_transaction_id, "
            "expires_at, created_at FROM holds ORDER BY id",
        }
        result = {name: conn.execute(query).fetchall() for name, query in rows.items()}
        for table in ("audit_log", "outbox_events", "api_keys", "idempotency_keys"):
            result[table] = conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]
        return result


def scalar(url: str, query: str, params=()) -> object:
    with psycopg.connect(url) as conn:
        return conn.execute(query, params).fetchone()[0]


def refused(url: str, statement: str, params=()) -> str | None:
    """The error class a statement fails with, or None if it was allowed (and rolled back)."""
    with psycopg.connect(url) as conn:
        try:
            conn.execute(statement, params)
        except psycopg.Error as error:
            return type(error).__name__
        conn.rollback()
        return None


def seed(old: Path, url: str) -> dict:
    env = {**os.environ, "PYTHONPATH": str(old / "src")}
    done = subprocess.run(
        [sys.executable, "-c", SEED, url, str(old)], cwd=old, env=env, capture_output=True, text=True, check=False
    )
    if done.returncode:
        sys.exit(f"seeding with the old code failed:\n{done.stderr}")
    return json.loads(done.stdout.strip().splitlines()[-1])


def runtime_login(owner: str, role: str, password: str) -> str:
    """The deploy step that creates the API's login role; safe to repeat (resets the password)."""
    with psycopg.connect(owner, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
        verb = sql.SQL("ALTER ROLE {} PASSWORD {}" if exists else "CREATE ROLE {} LOGIN IN ROLE tally_app PASSWORD {}")
        conn.execute(verb.format(sql.Identifier(role), sql.Literal(password)))
    return psycopg.conninfo.make_conninfo(owner, user=role, password=password)


def maintenance_errors(db: Database) -> list[str]:
    """Runs one maintenance pass and returns any task failures it logged."""
    records: list[logging.LogRecord] = []
    handler = logging.Handler(level=logging.ERROR)
    handler.emit = records.append
    logging.getLogger("tally.api").addHandler(handler)
    try:
        run_maintenance(db, Ledger())
    finally:
        logging.getLogger("tally.api").removeHandler(handler)
    return [r.getMessage() for r in records]


def holds_by_name(url: str, ids: dict) -> dict:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        rows = {r["id"]: r for r in conn.execute("SELECT id::text, status, expires_at, void_reason FROM holds")}
    return {name: rows[hold_id] for name, hold_id in ids.items() if hold_id in rows}


def old_code_after_upgrade(old: Path, owner: str, runtime: str, data: dict) -> None:
    """The old version against the upgraded database (docs/UPGRADING.md says it can't serve it)."""
    env = {**os.environ, "PYTHONPATH": str(old / "src")}
    accounts = data["accounts"]
    argv = [owner, runtime, str(old), data["key"], str(accounts["b"]), str(accounts["merchant"])]
    done = subprocess.run(
        [sys.executable, "-c", OLD_AFTER, *argv], cwd=old, env=env, capture_output=True, text=True, check=False
    )
    if done.returncode:
        sys.exit(f"running the old code after the upgrade failed:\n{done.stderr}")
    result = json.loads(done.stdout.strip().splitlines()[-1])
    check("the old version can't start as the runtime role", not result["starts_as_runtime_role"], result)
    check("the old version's payments fail after 010", result["payment"] == [409, "CONSTRAINT_VIOLATION"], result)
    waiting = scalar(owner, "SELECT count(*) FROM holds WHERE status = 'active' AND settle_to_account_id IS NOT NULL")
    check(
        "the old version's settlement takes none of the waiting payments",
        result["settled_by_old_code"] == 0 and waiting == 3,
        f"settled {result['settled_by_old_code']}, still waiting {waiting}",
    )


def upgrade(owner: str, data: dict, login, old: Path) -> str:
    """The normal path: migrate, create the login role (it joins tally_app, which 012 creates),
    then run the new API and maintenance as that role. Returns the role's URL."""
    before = snapshot(owner)
    applied = migrate(owner)
    check("the upgrade applies 010-012 and nothing else", applied == NEW, applied)
    after = snapshot(owner)
    for table in ("accounts", "entries", "transactions", "audit_log", "outbox_events", "api_keys"):
        check(f"the upgrade leaves {table} unchanged", after[table] == before[table])
    waiting = {data["ids"][n] for n in ("waiting payment", "payment past its old expiry", "invalid payment")}
    changed = {str(new["id"]) for old, new in zip(before["holds"], after["holds"], strict=True) if old != new}
    check("only waiting payments change, and only their expiry", changed == waiting, sorted(changed))
    stopped = all(h["expires_at"] is None for h in after["holds"] if str(h["id"]) in waiting)
    check("waiting payments have no expiry any more", stopped)
    held = {name: h["expires_at"] is not None for name, h in holds_by_name(owner, data["ids"]).items()}
    check("finished payments and ordinary holds keep their expiry", held["expired payment"] and held["active hold"])
    unstamped = scalar(owner, "SELECT count(*) FROM transactions WHERE created_xid IS NOT NULL")
    check("old transactions are sealed (created_xid NULL, not invented)", unstamped == 0, unstamped)
    report = Database(owner, min_size=1, max_size=1)
    check("the books reconcile right after the upgrade", report.run(reconcile)["ok"])
    report.close()

    runtime = login()
    old_code_after_upgrade(old, owner, runtime, data)
    headers = {"Authorization": f"Bearer {data['key']}"}
    app = create_app(Settings(database_url=runtime, pool_size=3), maintenance=False)  # checks nothing is pending
    db = Database(runtime, min_size=1, max_size=3)
    with TestClient(app, headers=headers) as http:
        check("the new API starts as the runtime role", http.get("/health").status_code == 200)
        waiting_hold = http.get(f"/v1/holds/{data['ids']['waiting payment']}").json()
        check("a waiting payment reads back with expires_at null", waiting_hold["expires_at"] is None, waiting_hold)
        old_expired = http.get(f"/v1/holds/{data['ids']['expired payment']}").json()
        check("the payment the old code expired stays expired", old_expired["status"] == "expired")

        time.sleep(max(0.0, data["seeded_at"] + 2.2 - time.time()))  # the old 2 s expiries have passed
        check("a maintenance pass as the runtime role logs no failure", maintenance_errors(db) == [])
        statuses = {name: h["status"] for name, h in holds_by_name(owner, data["ids"]).items()}
        expected = {
            "settled payment": "captured",
            "expired payment": "expired",
            "captured hold": "captured",
            "waiting payment": "captured",
            "payment past its old expiry": "captured",
            "invalid payment": "voided",
            "active hold": "active",
            "hold due soon": "expired",
        }
        check("settlement and expiry end each hold as specified", statuses == expected, statuses)
        reason = holds_by_name(owner, data["ids"])["invalid payment"]["void_reason"] or ""
        check("the invalid payment says why", reason.startswith("settlement refused: INVALID_DESTINATION"), reason)
        accounts = data["accounts"]
        balances = {n: http.get(f"/v1/accounts/{i}").json() for n, i in accounts.items() if n != "cash"}
        summary = {n: (b["balance"], b["held"]) for n, b in balances.items()}
        wanted = {"a": (940, 25), "b": (910, 0), "merchant": (150, 0), "empty": (0, 0)}
        check("balances and reservations after settlement", summary == wanted, summary)

        def post(path, body, status):
            response = http.post(path, json=body, headers={"Idempotency-Key": secrets.token_hex(8)})
            return response.status_code == status, (response.status_code, response.json().get("code"))

        pay = {"account_id": accounts["b"], "amount": 5, "settle_to_account_id": accounts["merchant"]}
        check("a payment with an expiry is refused", *post("/v1/holds", {**pay, "expires_in_seconds": 60}, 422))
        bad = {"account_id": accounts["a"], "amount": 5, "settle_to_account_id": accounts["empty"]}
        check("a payment to a protected asset is refused", *post("/v1/holds", bad, 422))
        new_payment = http.post("/v1/holds", json=pay, headers={"Idempotency-Key": secrets.token_hex(8)}).json()
        capture = {"destination_account_id": accounts["merchant"]}
        check("a new payment can't be captured by hand", *post(f"/v1/holds/{new_payment['id']}/capture", capture, 409))
        card = http.post(
            "/v1/holds", json={"account_id": accounts["b"], "amount": 8}, headers={"Idempotency-Key": "card"}
        ).json()
        check("an ordinary hold is still captured by hand", *post(f"/v1/holds/{card['id']}/capture", capture, 200))
        check("the next maintenance pass logs no failure", maintenance_errors(db) == [])
        merchant = http.get(f"/v1/accounts/{accounts['merchant']}").json()["balance"]
        check("the new payment settled", merchant == 150 + 5 + 8, merchant)
    db.close()

    pair = (
        "INSERT INTO entries (transaction_id, account_id, direction, amount, currency, balance_after) "
        "VALUES (%s, %s, 'debit', 1, 'USD', 0), (%s, %s, 'credit', 1, 'USD', 0)"
    )
    for name in ("reversed transaction", "reversal"):
        transaction = data["ids"][name]
        error = refused(owner, pair, (transaction, accounts["a"], transaction, accounts["b"]))
        check(
            f"an entry pair can't be appended to the {name}, even by the owner", error == "ObjectNotInPrerequisiteState"
        )
    for statement in (
        "DELETE FROM entries",
        "UPDATE entries SET amount = amount + 1",
        "ALTER TABLE entries DISABLE TRIGGER ALL",
        "UPDATE holds SET expires_at = now()",
    ):
        error = refused(runtime, statement)
        check(f"the runtime role is refused: {statement}", error == "InsufficientPrivilege", error)
    reopen = refused(runtime, "UPDATE holds SET status = 'active' WHERE id = %s", (data["ids"]["expired payment"],))
    check("the runtime role can't reopen the payment the old code expired", reopen == "ObjectNotInPrerequisiteState")
    report = Database(owner, min_size=1, max_size=1)
    check("the books reconcile after the new code ran", report.run(reconcile)["ok"])
    report.close()
    return runtime


def repeat_deploy(owner: str, role: str, password: str) -> None:
    before = snapshot(owner)
    check("migrating again does nothing", migrate(owner) == [])
    runtime = runtime_login(owner, role, password)  # the role step again
    app = create_app(Settings(database_url=runtime, pool_size=2), maintenance=False)
    db = Database(runtime, min_size=1, max_size=2)
    with TestClient(app) as http:
        check("the API starts again", http.get("/health").status_code == 200)
    check("maintenance again logs no failure", maintenance_errors(db) == [] and maintenance_errors(db) == [])
    db.close()
    check("repeating every deploy step changes no data", snapshot(owner) == before)


def failing_migration(old: Path, owner: str, runtime_for, version: str, sabotage: str, repair: str) -> None:
    data = seed(old, owner)
    with psycopg.connect(owner, autocommit=True) as conn:
        conn.execute(sabotage)
    before = snapshot(owner)
    try:
        migrate(owner)
        check(f"a failure injected into {version} stops the upgrade", False)
    except psycopg.Error as error:
        check(f"a failure injected into {version} stops the upgrade", True, type(error).__name__)
    recorded = scalar(owner, "SELECT max(version) FROM schema_migrations")
    previous = NEW[NEW.index(version) - 1] if version != NEW[0] else "009_batched_settlement"
    check(f"{version} isn't recorded; everything before it is", recorded == previous, recorded)
    if version == NEW[0]:
        not_null = scalar(
            owner, "SELECT attnotnull FROM pg_attribute WHERE attrelid = 'holds'::regclass AND attname = 'expires_at'"
        )
        constraint = scalar(owner, "SELECT count(*) FROM pg_constraint WHERE conname = 'holds_expiry_matches_kind'")
        check("nothing of 010 remains: expiry NOT NULL, no new constraint", not_null and constraint == 0)
        check("no data changed (payments keep their expiry)", snapshot(owner) == before)
    else:
        granted = scalar(owner, "SELECT has_table_privilege('tally_app', 'accounts', 'SELECT')")
        definer = scalar(owner, "SELECT prosecdef FROM pg_proc WHERE proname = 'audit_change'")
        check("nothing of 012 remains: no grants, audit_change not SECURITY DEFINER", not granted and not definer)
        now = snapshot(owner)
        same = all(now[t] == before[t] for t in ("accounts", "entries", "transactions", "audit_log", "outbox_events"))
        waiting = {data["ids"][n] for n in ("waiting payment", "payment past its old expiry", "invalid payment")}
        changed = {str(n["id"]) for o, n in zip(before["holds"], now["holds"], strict=True) if o != n}
        check("data changed only as the applied 010 does (waiting payments' expiry)", same and changed == waiting)
        try:
            migrate(runtime_for(owner))
            check("the new API refuses to start on the half-upgraded schema", False)
        except MigrationError as error:
            check("the new API refuses to start on the half-upgraded schema", "schema owner" in str(error))
    with psycopg.connect(owner, autocommit=True) as conn:
        conn.execute(repair)
    applied = migrate(owner)
    check(
        f"after removing the cause, rerunning completes from {version}", applied == NEW[NEW.index(version) :], applied
    )
    waiting = data["ids"]["waiting payment"]
    expiry = scalar(owner, "SELECT expires_at FROM holds WHERE id = %s", (waiting,))
    check("and the waiting payment lost its expiry as in a clean upgrade", expiry is None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--admin-url", required=True, help="a superuser on a server where it may create databases")
    parser.add_argument("--old-ref", default="a717e4f", help="a commit whose migrations end at 009")
    args = parser.parse_args()
    suffix = secrets.token_hex(4)
    names = [f"tally_upgrade_{suffix}_{case}" for case in ("main", "fail010", "fail012")]
    role, password = f"tally_upgrade_api_{suffix}", secrets.token_hex(16)
    url = {name: psycopg.conninfo.make_conninfo(args.admin_url, dbname=name) for name in names}

    with psycopg.connect(args.admin_url, autocommit=True) as conn:
        for name in names:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        with tempfile.TemporaryDirectory(prefix="tally-old-") as tmp:
            old = Path(tmp)
            archive = subprocess.run(["git", "-C", str(ROOT), "archive", args.old_ref], capture_output=True, check=True)
            subprocess.run(["tar", "-x", "-C", str(old)], input=archive.stdout, check=True)

            print(f"== {args.old_ref} builds a ledger, this code upgrades it")
            owner = url[names[0]]
            data = seed(old, owner)
            upgrade(owner, data, lambda: runtime_login(owner, role, password), old)
            print("== the deploy steps, run a second time")
            repeat_deploy(owner, role, password)

            def runtime_for(owner_url: str) -> str:
                return psycopg.conninfo.make_conninfo(owner_url, user=role, password=password)

            print("== 010 fails halfway (a column it adds already exists)")
            failing_migration(
                old,
                url[names[1]],
                runtime_for,
                NEW[0],
                "ALTER TABLE holds ADD COLUMN void_reason TEXT",
                "ALTER TABLE holds DROP COLUMN void_reason",
            )
            print("== 012 fails halfway (a function it creates already exists)")
            failing_migration(
                old,
                url[names[2]],
                runtime_for,
                NEW[2],
                "CREATE FUNCTION purge_outbox_events(integer) RETURNS integer LANGUAGE sql AS 'SELECT 0'",
                "DROP FUNCTION purge_outbox_events(integer)",
            )
    finally:
        with psycopg.connect(args.admin_url, autocommit=True) as conn:
            for name in names:
                conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
            conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))
    print(f"\n{len(failures)} check(s) failed: {failures}" if failures else "\nall checks passed; databases dropped")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
