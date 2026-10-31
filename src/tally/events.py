"""Reading the outbox (migration 008): every change to a client's books, in commit-safe order.

A consumer polls ``read`` with the position of the last event it processed and gets the
next ones. Two guarantees, both from the xid8 watermark explained in migration 008:

* in order: events are sorted by the transaction that wrote them, then by id;
* no gaps: an event is only handed out once every older transaction has finished, so
  nothing can later appear behind a position a consumer has already passed.

The price is a short delay: while a transaction is open, events from younger
transactions wait for it. Transactions here last milliseconds, and the server cuts off
any that idle for over a minute (idle_in_transaction_session_timeout).
"""

from __future__ import annotations

from psycopg import Connection

# Consumers more than 7 days behind have to resync from the API (purge_outbox_events, migration 012).
PURGE_BATCH_SIZE = 5_000


def read(conn: Connection, client_id: int, after: tuple[str, int] | None, limit: int) -> list[dict]:
    """Events after position ``after`` = (txid, id), oldest first."""
    txid, event_id = after or ("0", 0)
    return conn.execute(
        """
        SELECT id, txid::text AS txid, type, created_at, payload
          FROM outbox_events
         WHERE client_id = %s AND (txid, id) > (%s::xid8, %s)
           AND txid < pg_snapshot_xmin(pg_current_snapshot())
         ORDER BY txid, id
         LIMIT %s
        """,
        (client_id, txid, event_id, limit),
    ).fetchall()


def purge_expired(conn: Connection, batch_size: int = PURGE_BATCH_SIZE) -> int:
    """Deletes one batch of events older than the retention period; call until it returns less than a batch.

    The API's database role can't delete events itself (migration 012): this function,
    run as the owner, only ever removes those past the 7-day retention."""
    return conn.execute("SELECT purge_outbox_events(%s) AS purged", (batch_size,)).fetchone()["purged"]
