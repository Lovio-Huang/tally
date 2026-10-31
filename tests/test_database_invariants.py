"""The database enforces the ledger's rules on its own, even against hand-written SQL.

These tests deliberately bypass the application to prove that a buggy service,
a migration script or a tired engineer at a psql prompt still can't corrupt the books.
"""

from datetime import UTC, datetime, timedelta

import psycopg
import pytest


@pytest.fixture
def raw(migrated):
    with psycopg.connect(migrated) as conn:
        yield conn


@pytest.fixture
def payment(books, wallet):
    alice, bob = wallet("alice", funded=1_000), wallet("bob")
    return books.transfer(alice["id"], bob["id"], 250)


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE entries SET amount = amount + 1",
        "DELETE FROM entries",
        "UPDATE transactions SET description = 'edited'",
        "UPDATE transactions SET created_xid = pg_current_xact_id()",  # unsealing it
        "DELETE FROM transactions",
    ],
)
def test_history_is_append_only(raw, payment, statement):
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="append-only"):
        raw.execute(statement)


def test_an_unbalanced_transaction_fails_at_commit(raw, wallet):
    alice, bob = wallet("alice", funded=1_000), wallet("bob")
    txn = raw.execute("INSERT INTO transactions (client_id) VALUES (1) RETURNING id").fetchone()[0]
    raw.execute(
        "INSERT INTO entries (transaction_id, account_id, direction, amount, currency, balance_after) VALUES "
        "(%s, %s, 'debit', 100, 'USD', 0), (%s, %s, 'credit', 99, 'USD', 0)",
        (txn, alice["id"], txn, bob["id"]),
    )

    # The check is deferred: it fires once the whole transaction is in place.
    with pytest.raises(psycopg.errors.IntegrityConstraintViolation, match="does not balance"):
        raw.commit()


def test_a_transaction_needs_at_least_two_entries(raw):
    raw.execute("INSERT INTO transactions (client_id) VALUES (1)")

    with pytest.raises(psycopg.errors.IntegrityConstraintViolation, match="at least two entries"):
        raw.commit()


def test_entries_must_use_their_accounts_currency(raw, wallet):
    alice = wallet("alice")
    txn = raw.execute("INSERT INTO transactions (client_id) VALUES (1) RETURNING id").fetchone()[0]

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        raw.execute(
            "INSERT INTO entries (transaction_id, account_id, direction, amount, currency, balance_after) "
            "VALUES (%s, %s, 'credit', 100, 'EUR', 0)",
            (txn, alice["id"]),
        )


def test_a_protected_account_can_never_go_negative(raw, wallet):
    alice = wallet("alice", funded=100)
    txn = raw.execute("INSERT INTO transactions (client_id) VALUES (1) RETURNING id").fetchone()[0]

    # Applying the entry moves the balance at once, and the CHECK refuses -1.
    with pytest.raises(psycopg.errors.CheckViolation, match="accounts_no_overdraft"):
        raw.execute(
            "INSERT INTO entries (transaction_id, account_id, direction, amount, currency, balance_after) "
            "VALUES (%s, %s, 'debit', 101, 'USD', 0)",
            (txn, alice["id"]),
        )


def test_a_transaction_cannot_be_reversed_twice_even_by_hand(raw, payment):
    raw.execute("INSERT INTO transactions (client_id, reverses_id) VALUES (1, %s)", (payment["id"],))

    with pytest.raises(psycopg.errors.UniqueViolation):
        raw.execute("INSERT INTO transactions (client_id, reverses_id) VALUES (1, %s)", (payment["id"],))


def test_amounts_must_be_positive(raw, wallet):
    alice = wallet("alice")
    txn = raw.execute("INSERT INTO transactions (client_id) VALUES (1) RETURNING id").fetchone()[0]

    with pytest.raises(psycopg.errors.CheckViolation):
        raw.execute(
            "INSERT INTO entries (transaction_id, account_id, direction, amount, currency, balance_after) "
            "VALUES (%s, %s, 'debit', -5, 'USD', 0)",
            (txn, alice["id"]),
        )


# --- rules added by migration 003 ---------------------------------------------------


@pytest.mark.parametrize(
    "statement",
    ["TRUNCATE entries", "TRUNCATE transactions CASCADE", "TRUNCATE accounts CASCADE", "TRUNCATE holds"],
)
def test_history_cannot_be_wiped_with_truncate(raw, payment, statement):
    # TRUNCATE skips row triggers, so it needs its own guard. CASCADE reaches entries too.
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        raw.execute(statement)


@pytest.mark.parametrize("change", ["type = 'asset'", "currency = 'EUR'"])
def test_an_accounts_type_and_currency_are_fixed(raw, wallet, change):
    alice = wallet("alice", funded=100)

    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="cannot change"):
        raw.execute(f"UPDATE accounts SET {change} WHERE id = %s", (alice["id"],))


def test_other_account_details_stay_editable(raw, wallet):
    alice = wallet("alice")

    raw.execute("UPDATE accounts SET name = 'Alice B.', allow_negative = true WHERE id = %s", (alice["id"],))

    assert raw.execute("SELECT name FROM accounts WHERE id = %s", (alice["id"],)).fetchone()[0] == "Alice B."


@pytest.fixture
def settled_hold(books, wallet):
    alice, store = wallet("alice", funded=1_000), wallet("store")
    hold = books.create_hold(alice["id"], 300, datetime.now(UTC) + timedelta(days=1))
    books.capture_hold(hold["id"], store["id"])
    return hold


@pytest.fixture
def active_hold(books, wallet):
    alice = wallet("alice", funded=1_000)
    return books.create_hold(alice["id"], 300, datetime.now(UTC) + timedelta(days=1))


def test_a_settled_hold_cannot_be_reopened(raw, settled_hold):
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="already captured"):
        raw.execute(
            "UPDATE holds SET status = 'active', captured_transaction_id = NULL WHERE id = %s", (settled_hold["id"],)
        )


def test_an_active_hold_keeps_its_amount_and_tracks_updates(raw, active_hold):
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="cannot change"):
        raw.execute("UPDATE holds SET amount = 1 WHERE id = %s", (active_hold["id"],))
    raw.rollback()

    updated = raw.execute(
        "UPDATE holds SET description = 'renamed' WHERE id = %s RETURNING updated_at", (active_hold["id"],)
    ).fetchone()[0]

    assert updated > active_hold["updated_at"]


@pytest.mark.parametrize("status", ["active", "captured", "expired"])
def test_only_a_voided_hold_carries_a_void_reason(raw, active_hold, status):
    with pytest.raises(psycopg.errors.CheckViolation, match="holds_void_reason_only_when_voided"):
        raw.execute(
            "UPDATE holds SET status = %(status)s, void_reason = 'made up', captured_transaction_id = CASE "
            "WHEN %(status)s = 'captured' THEN (SELECT id FROM transactions LIMIT 1) END WHERE id = %(id)s",
            {"status": status, "id": active_hold["id"]},
        )


def test_a_voided_hold_can_carry_a_void_reason(raw, active_hold):
    raw.execute("UPDATE holds SET status = 'voided', void_reason = 'refused' WHERE id = %s", (active_hold["id"],))
    raw.commit()


def test_holds_cannot_be_deleted(raw, active_hold):
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="permanent"):
        raw.execute("DELETE FROM holds WHERE id = %s", (active_hold["id"],))


def insert_transfer(conn, debit_id, credit_id, posted_at=None) -> list:
    """Hand-written transfer; posted_at is passed explicitly when given, to try to override it."""
    txn = conn.execute("INSERT INTO transactions (client_id) VALUES (1) RETURNING id").fetchone()[0]
    # client_id is filled in by a trigger, which replica mode skips; then this value stands.
    columns = "client_id, transaction_id, account_id, direction, amount, currency, balance_after" + (
        ", posted_at" if posted_at else ""
    )
    extra = ", %s" if posted_at else ""
    rows = []
    for account_id, direction in ((debit_id, "debit"), (credit_id, "credit")):
        params = (txn, account_id, direction) + ((posted_at,) if posted_at else ())
        rows.append(
            conn.execute(
                f"INSERT INTO entries ({columns}) VALUES (1, %s, %s, %s, 1, 'USD', 0{extra}) RETURNING posted_at",
                params,
            ).fetchone()[0]
        )
    return rows


def test_entry_timestamps_are_stamped_by_the_database(raw, wallet):
    alice, bob = wallet("alice", funded=100), wallet("bob")

    stamped = insert_transfer(raw, alice["id"], bob["id"], posted_at=datetime(2000, 1, 1, tzinfo=UTC))

    assert all(abs(ts - datetime.now(UTC)) < timedelta(minutes=1) for ts in stamped)


def test_entry_timestamps_never_go_backwards_within_an_account(raw, wallet):
    alice, bob = wallet("alice", funded=100), wallet("bob")
    # Simulate a server clock that was ahead and then stepped back: plant an entry an
    # hour in the future, bypassing the trigger the way a bulk restore would.
    raw.execute("SET LOCAL session_replication_role = replica")
    future = insert_transfer(raw, alice["id"], bob["id"], posted_at=datetime.now(UTC) + timedelta(hours=1))
    raw.execute("SET LOCAL session_replication_role = origin")

    later = insert_transfer(raw, alice["id"], bob["id"])

    assert later[0] >= future[0]


@pytest.mark.parametrize(
    "metadata",
    ['["not", "an", "object"]', '"just a string"', '{"blob": "' + "x" * 5000 + '"}'],
    ids=["array", "string", "over-4kb"],
)
def test_metadata_must_be_a_small_json_object(raw, metadata):
    with pytest.raises(psycopg.errors.CheckViolation, match="metadata_shape"):
        raw.execute(
            "INSERT INTO accounts (client_id, name, type, currency, metadata) "
            "VALUES (1, 'x', 'asset', 'USD', %s::jsonb)",
            (metadata,),
        )


# --- rules added by migration 005: balances are derived, never written ----------------


@pytest.mark.parametrize("column", ["balance", "held"])
def test_balances_cannot_be_edited_directly(raw, wallet, column):
    alice = wallet("alice", funded=100)

    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="derived"):
        raw.execute(f"UPDATE accounts SET {column} = {column} + 1 WHERE id = %s", (alice["id"],))


def test_a_new_account_cannot_start_with_money(raw):
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="zero balance"):
        raw.execute(
            "INSERT INTO accounts (client_id, name, type, currency, balance) VALUES (1, 'x', 'liability', 'USD', 500)"
        )


def test_entries_written_by_hand_still_move_balances_and_reconcile(raw, db, wallet):
    from tally.reconciliation import reconcile

    alice, bob = wallet("alice", funded=1_000), wallet("bob")

    insert_transfer(raw, alice["id"], bob["id"])  # 1 cent, with a bogus balance_after of 0
    raw.commit()

    balances = dict(raw.execute("SELECT id, balance FROM accounts WHERE id = ANY(%s)", ([alice["id"], bob["id"]],)))
    last = raw.execute(
        "SELECT balance_after FROM entries WHERE account_id = %s ORDER BY id DESC LIMIT 1", (alice["id"],)
    ).fetchone()[0]
    assert balances == {alice["id"]: 999, bob["id"]: 1}
    assert last == 999  # computed by the database, not taken from the INSERT
    assert db.run(reconcile)["ok"]


def test_holds_written_by_hand_still_move_the_held_amount(raw, db, wallet):
    from tally.reconciliation import reconcile

    alice = wallet("alice", funded=1_000)
    hold = raw.execute(
        "INSERT INTO holds (account_id, amount, expires_at) VALUES (%s, 300, now() + interval '1 day') RETURNING id",
        (alice["id"],),
    ).fetchone()[0]
    held_while_active = raw.execute("SELECT held FROM accounts WHERE id = %s", (alice["id"],)).fetchone()[0]

    raw.execute("UPDATE holds SET status = 'voided' WHERE id = %s", (hold,))
    raw.commit()

    assert held_while_active == 300
    assert raw.execute("SELECT held FROM accounts WHERE id = %s", (alice["id"],)).fetchone()[0] == 0
    assert db.run(reconcile)["ok"]


@pytest.fixture
def stranger(db):
    """An account belonging to another client."""
    from tally import auth
    from tally.ledger import Ledger

    record, _ = db.run(lambda conn: auth.create_client(conn, "stranger"))
    return db.run(lambda conn: Ledger(record["client_id"]).create_account(conn, "Stranger", "liability", "USD"))


def test_a_transaction_cannot_touch_another_clients_account(raw, wallet, stranger):
    alice = wallet("alice", funded=100)
    txn = raw.execute("INSERT INTO transactions (client_id) VALUES (1) RETURNING id").fetchone()[0]

    with pytest.raises(psycopg.errors.ForeignKeyViolation, match="entries_account_client"):
        raw.execute(
            "INSERT INTO entries (transaction_id, account_id, direction, amount, currency, balance_after) "
            "VALUES (%s, %s, 'debit', 10, 'USD', 0), (%s, %s, 'credit', 10, 'USD', 0)",
            (txn, alice["id"], txn, stranger["id"]),
        )


def test_an_entrys_client_comes_from_its_transaction_whatever_the_insert_says(raw, wallet, stranger):
    alice, bob = wallet("alice", funded=100), wallet("bob")
    txn = raw.execute("INSERT INTO transactions (client_id) VALUES (1) RETURNING id").fetchone()[0]
    raw.execute(
        "INSERT INTO entries (client_id, transaction_id, account_id, direction, amount, currency, balance_after) "
        "VALUES (%s, %s, %s, 'debit', 10, 'USD', 0), (%s, %s, %s, 'credit', 10, 'USD', 0)",
        (2, txn, alice["id"], 2, txn, bob["id"]),  # claims client 2; the trigger says otherwise
    )

    assert {r[0] for r in raw.execute("SELECT client_id FROM entries WHERE transaction_id = %s", (txn,))} == {1}


def test_an_account_cannot_change_owner(raw, wallet, stranger):
    alice = wallet("alice")

    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="owner"):
        raw.execute("UPDATE accounts SET client_id = 2 WHERE id = %s", (alice["id"],))


def test_the_audit_log_is_append_only(raw, db, api_key):
    raw.execute("INSERT INTO audit_log (client_id, api_key_id, action, resource_id) VALUES (1, 1, 'x', 'y')")
    raw.commit()

    for statement in ("UPDATE audit_log SET action = 'edited'", "DELETE FROM audit_log", "TRUNCATE audit_log"):
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="append-only"):
            raw.execute(statement)
        raw.rollback()


def append_pair(conn, transaction_id, debit_id, credit_id) -> None:
    conn.execute(
        "INSERT INTO entries (transaction_id, account_id, direction, amount, currency, balance_after) VALUES "
        "(%s, %s, 'debit', 5, 'USD', 0), (%s, %s, 'credit', 5, 'USD', 0)",
        (transaction_id, debit_id, transaction_id, credit_id),
    )


@pytest.mark.parametrize("target", ["committed", "reversed", "reversal"])
def test_a_committed_transaction_is_sealed(raw, db, books, payment, target):
    # A balanced pair passes the deferred balance check, so before sealing this rewrote
    # history and still reconciled.
    from tally.reconciliation import reconcile

    alice, bob = (e["account_id"] for e in payment["entries"])
    reversal = books.reverse(payment["id"]) if target != "committed" else None
    transaction_id = reversal["id"] if target == "reversal" else payment["id"]
    before = raw.execute("SELECT id, balance FROM accounts ORDER BY id").fetchall()
    raw.commit()

    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="sealed"):
        append_pair(raw, transaction_id, alice, bob)  # would leave every balance non-negative
    raw.rollback()

    assert raw.execute("SELECT id, balance FROM accounts ORDER BY id").fetchall() == before
    assert raw.execute("SELECT count(*) FROM entries WHERE transaction_id = %s", (transaction_id,)).fetchone()[0] == 2
    assert db.run(reconcile)["ok"]


def test_the_creating_transaction_can_add_entries_over_several_statements(raw, db, wallet):
    from tally.reconciliation import reconcile

    alice, bob = wallet("alice", funded=1_000), wallet("bob")
    # A caller can't stamp a transaction with another session's id: the database overrides it.
    txn = raw.execute("INSERT INTO transactions (client_id, created_xid) VALUES (1, '3') RETURNING id").fetchone()[0]
    append_pair(raw, txn, alice["id"], bob["id"])
    append_pair(raw, txn, alice["id"], bob["id"])
    stamped = raw.execute("SELECT created_xid = pg_current_xact_id() FROM transactions WHERE id = %s", (txn,))
    assert stamped.fetchone()[0]
    raw.commit()

    assert raw.execute("SELECT balance FROM accounts WHERE id = %s", (bob["id"],)).fetchone()[0] == 10
    assert db.run(reconcile)["ok"]
