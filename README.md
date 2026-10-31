# Tally

[![CI](https://github.com/Lovio-Huang/tally/actions/workflows/ci.yml/badge.svg)](https://github.com/Lovio-Huang/tally/actions/workflows/ci.yml)
![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue)
![PostgreSQL 17](https://img.shields.io/badge/PostgreSQL-17-336791)

A **double-entry ledger API**: the part of a payments or wallet company that records where every cent
is. Accounts, transfers, multi-leg transactions, card-style holds, reversals, statements and
reconciliation.

The interesting part is **where the rules live**. "Debits equal credits", "history is never edited"
and "no overdrafts" are enforced by **PostgreSQL itself** (deferred constraint triggers, CHECK
constraints, composite foreign keys), not just by application code. A buggy service, a bad script or a
hand-typed `UPDATE` can't corrupt the books.

**Stack:** Python 3.14 · FastAPI · PostgreSQL 17 · psycopg 3 (raw SQL, connection pool) · pytest · Hypothesis (stateful, model-based) · Testcontainers · Docker Compose · GitHub Actions

![Tally console](docs/console.jpg)

*The built-in console (`/`): reconciliation status (re-checked every minute and after each write), a journal-entry editor that totals debits and credits as you type, statements with running balances, holds, and a button that replays the last request with the same idempotency key. Like any client it calls the API with a key (`tally demo` prints one).*

## Double-entry in 30 seconds

Every movement of money is a **transaction** made of **entries**. Each entry debits or credits one account,
and the debits must equal the credits. Whether a debit makes an account go up or down depends on its type:

| Account type | Examples | Increased by |
|---|---|---|
| Asset | Cash at the bank | debit |
| Liability | Customer wallets (money we owe customers) | credit |
| Revenue | Fees we earn | credit |
| Expense, Equity | Costs; owners' capital | debit, credit |

| What happened | Debit | Credit |
|---|---|---|
| Alice deposits $500 | Cash $500 | Alice's wallet $500 |
| Alice pays Bob $25 with a $0.75 fee | Alice's wallet $25.75 | Bob's wallet $25.00, Fee revenue $0.75 |
| Bob withdraws $50 | Bob's wallet $50 | Cash $50 |

Because every transaction balances, the whole ledger always balances. Any error shows up as a nonzero
sum, which is what the reconciliation report checks.

## Guarantees, and how each is enforced

| Guarantee | Enforced by | Proven by |
|---|---|---|
| Every transaction balances, per currency | `CONSTRAINT TRIGGER ... DEFERRABLE INITIALLY DEFERRED`, checked at `COMMIT` | `test_an_unbalanced_transaction_fails_at_commit` (raw SQL) |
| A transaction has at least two entries | Deferred constraint trigger on `transactions` | `test_a_transaction_needs_at_least_two_entries` |
| History is append-only; fixes are reversals | `BEFORE UPDATE OR DELETE` triggers that raise | `test_history_is_append_only` (5 cases) |
| A committed transaction can't gain entries later (a balanced pair appended to an old transaction would pass the balance check) | Each transaction records the database transaction that created it (`created_xid`, an `xid8` stamped by a trigger); the entry trigger accepts entries only from that same database transaction. Rows from before migration 011 are `NULL`, so sealed too, rather than stamped with an invented id | `test_a_committed_transaction_is_sealed` (committed, reversed, reversal), `test_transactions_from_before_sealing_are_sealed_too` |
| The API can't switch any of this off | It connects as a role that owns nothing and has column-level grants; audit rows and events are written by `SECURITY DEFINER` functions (migration 012, see [Database roles](#database-roles)) | `test_runtime_role.py`: `DISABLE TRIGGER`, replica mode, `SET ROLE`, editing history, the audit log or keys are all refused |
| An entry's currency matches its account | Composite FK `(account_id, currency)` → `accounts (id, currency)` | `test_entries_must_use_their_accounts_currency` |
| Protected accounts never go negative | `CHECK (allow_negative OR balance - held >= 0)`, plus a service-level check under row lock | `test_a_protected_account_can_never_go_negative`, `test_concurrent_withdrawals_never_overdraw` |
| A transaction is reversed at most once | `UNIQUE (reverses_id)`, plus a row lock on the original | `test_a_transaction_reversed_concurrently_is_reversed_once` |
| A retried request moves money once | Idempotency key claimed in the **same** DB transaction as the money movement | `test_concurrent_requests_with_one_idempotency_key_move_money_once` |
| Stored balances equal the sum of entries; `held` equals the sum of active holds | **Derived by triggers**: inserting an entry moves its account's balance and fills in `balance_after`, settling a hold releases its amount, and direct writes to `balance`/`held` are refused (migration 005). `/v1/reconciliation` re-derives both independently | `test_balances_cannot_be_edited_directly`, `test_entries_written_by_hand_still_move_balances_and_reconcile`; the Hypothesis state machine after every step |
| History can't be wiped with `TRUNCATE` either (it skips row triggers) | Statement-level `BEFORE TRUNCATE` triggers, which also fire through `CASCADE` | `test_history_cannot_be_wiped_with_truncate` (4 cases) |
| An account's type and currency never change (changing the type would flip the meaning of its whole balance) | `BEFORE UPDATE OF type, currency` trigger | `test_an_accounts_type_and_currency_are_fixed` |
| Settled holds are final; an active hold keeps its account and amount | Lifecycle trigger on `holds`, plus no `DELETE`/`TRUNCATE` | `test_a_settled_hold_cannot_be_reopened`, `test_holds_cannot_be_deleted` |
| Within an account, entry timestamps never go backwards (point-in-time balances depend on it) | `BEFORE INSERT` trigger stamps `posted_at` itself, never below the account's previous entry, even if the server clock steps back | `test_entry_timestamps_never_go_backwards_within_an_account` |
| `metadata` is a JSON object of at most 4 KB | `CHECK` added `NOT VALID`, then validated in a separate migration so a large table keeps taking writes | `test_metadata_must_be_a_small_json_object` |

## Data model

```mermaid
erDiagram
    accounts ||--o{ entries : "has"
    transactions ||--|{ entries : "2..n balanced"
    transactions |o--o| transactions : "reverses"
    accounts ||--o{ holds : "reserves funds on"
    holds |o--o| transactions : "captured as"

    accounts {
        bigint id PK
        text type "asset | liability | equity | revenue | expense"
        text normal_side "generated: debit or credit"
        char currency
        bigint balance "minor units"
        bigint held "reserved by active holds"
        bigint available "generated: balance - held"
        bool allow_negative
    }
    transactions {
        uuid id PK
        uuid reverses_id UK "at most one reversal"
        timestamptz posted_at
        xid8 created_xid "creating DB transaction: seals it"
    }
    entries {
        bigint id PK
        uuid transaction_id FK
        bigint account_id FK "with currency"
        text direction "debit | credit"
        bigint amount "> 0"
        bigint balance_after "running balance"
    }
    holds {
        uuid id PK
        bigint account_id FK
        bigint amount
        text status "active | captured | voided | expired"
        timestamptz expires_at "ordinary holds; null for payments"
        bigint settle_to_account_id FK "payments only"
        text void_reason "why settlement refused it"
    }
    idempotency_keys {
        text key PK
        text request_hash
        jsonb response
    }
```

Design choices you can see in the schema ([`migrations/001_ledger.sql`](migrations/001_ledger.sql)):

- **Money is `BIGINT` minor units**, never floats (`0.1 + 0.2 != 0.3`).
- **`balance` is stored, not computed on every read,** so reading it is a primary-key lookup instead of summing the entire history. A trigger on `entries` keeps it in step with every insert, which makes it a cache that can't go stale; reconciliation still re-derives it from scratch as an independent check.
- **`balance_after` on every entry** gives statements a running balance and answers "what was the balance on March 1st?" with one indexed lookup.
- **`clock_timestamp()` instead of `now()`** for `posted_at`. `now()` is the start of the database transaction, which can be earlier than the moment the row lock was acquired. That would let a later entry carry an earlier timestamp and break point-in-time balances.
- **A partial index** (`WHERE status = 'active'`) keeps the hold-expiry scan small no matter how many settled holds pile up.

### Indexes, measured

[`migrations/002_query_indexes.sql`](migrations/002_query_indexes.sql) adds two indexes. I found the slow queries by
reading their plans on a realistic data set rather than guessing, and
[`scripts/index_benchmark.py`](scripts/index_benchmark.py) reproduces the numbers. It loads 1M transactions /
2M entries spread over a year, with one hot merchant account in 20% of them, and runs `EXPLAIN ANALYZE` on the
same data before and after the migration (median of warm runs, PostgreSQL 17 on an Apple M5):

| Query | Before | After |
|---|---|---|
| Balance as of Jan 8, hot account (200k entries) | 112.5 ms: walked the **whole table's primary key** backwards, filtering every row | 0.003 ms: one probe of `(account_id, posted_at, id)` |
| Balance as of Dec 10, hot account | 0.94 ms | 0.002 ms |
| Balance as of Jan 8, typical account (~1.8k entries) | 0.19 ms | 0.002 ms |
| Recent transactions feed (latest 20 of 1M) | 83.6 ms: sequential scan + sort of 1M rows | 0.008 ms: backward scan of `(posted_at, id)` |

The surprise was the first row: Postgres did not use the existing `(account_id, id)` index at all, and scanned
the entire primary key instead. The fix also needed a query change. Within one account, `posted_at` rises with
`id`, because it is taken under the account's row lock. So `ORDER BY posted_at DESC, id DESC` returns the same
row as `ORDER BY id DESC`, and that order matches the new index.

Indexes cost writes, so I re-ran the load test after the migration: spread 3,000–3,130 transfers/s (was
3,050–3,150), hotspot 1,560–1,620/s (was 1,590–1,640). At this data size that is within run-to-run noise.

**Adding indexes to a live ledger.** The migration uses `CREATE INDEX CONCURRENTLY`, which doesn't block writes
but can't run inside a transaction, so the migration runner supports a `-- migration: no-transaction` marker.
That surfaced a real bug: when several app workers booted at once, the ones blocked in `pg_advisory_lock()`
counted as open transactions, and `CREATE INDEX CONCURRENTLY` waits for all open transactions to finish, while
the index builder held the lock they were waiting for. Postgres reported a deadlock and startup failed. The
runner now polls with `pg_try_advisory_lock()`, and
`test_instances_starting_at_the_same_time_migrate_once_without_deadlocking` reproduces the race.

## Clients and tenancy

Every request authenticates with an API key, and the key decides whose books it sees. Each account, transaction
and hold belongs to one client; another client's look exactly like ones that don't exist (`404`, never `403`, so
ids leak nothing).

- **Keys.** `tally create-client NAME` prints a key once (`tly_` plus 256 random bits); only its SHA-256 is stored.
  A fast hash is the right choice here: the key is random rather than a password, so there is nothing to brute
  force, and every request can afford the lookup. A client can hold several keys, which is how a key is rotated
  without downtime (`create-key`, move callers over, `revoke-key`). Revocation takes effect on the next request.
- **Isolation in code and in the database.** The ledger filters every query by client, and an unknown client fails
  closed (the lookup finds nothing, an insert fails). The database enforces the same boundary the way it enforces
  currencies: composite foreign keys `(client_id, account_id)` and `(client_id, transaction_id)` make it impossible
  for a transaction to touch another client's account, whatever the code does. Entries and holds take their client
  from their transaction and account (triggers), so it can't be faked either. On an existing ledger the keys were
  added without blocking writes: indexes `CONCURRENTLY`, foreign keys `NOT VALID` and then validated.
- **Idempotency keys are per client**, so two clients may pick the same one.
- **Audit log.** Every change (account opened, transaction posted or reversed, hold created, captured, voided or
  expired) gets an append-only `audit_log` row naming the client, the API key and the request id. Triggers write it,
  inside the statement that makes the change: a committed change always has its record, a rolled-back one never
  does, and no new code path can forget it. The API tells the database who is acting with `set_config(...)` local
  to the transaction, in the same query that checks the key. Changes made any other way (psql, a script, the expiry
  job) are recorded with no key, which are exactly the rows worth a second look.
- **Reconciliation** over HTTP covers only the caller's books; `tally reconcile` checks everything, including that no
  entry or hold ever crossed from one client's books into another's.

**What it cost.** The first version checked the key in its own transaction and wrote the audit row from the
application: about 20% of transfer throughput, an extra pool checkout and an extra round trip, the second one while
holding the account locks. Checking the key inside the request's own transaction and writing the audit row from a
trigger brought the cost down to roughly 6% on the hot-account scenario and 7-16% on spread traffic (measured A/B
against the previous commit on the same machine; run-to-run noise here is about 10%).

## Events (transactional outbox)

Other services usually need to know when the books change: to send a receipt, update a search index, notify a
merchant. Writing to the database and then publishing to a broker is two writes that can't both be guaranteed,
so a crash in between either loses an event or announces a change that was rolled back. Here every change also
inserts a row into `outbox_events` **in the same database transaction**: account opened, transaction posted or
reversed (with its entries), hold created, captured, voided or expired. Triggers do it, so no code path can forget.
Consumers poll `GET /v1/events?cursor=...` and get their own client's events; a relay could forward the same
table to Kafka.

The subtle part is reading them in order without gaps. Ids are handed out at insert time, not commit time, so
event 101 can become visible before event 100; a consumer that moved its cursor past 101 would never see 100.
Each event records its transaction's `xid8`, and readers only get events from transactions older than every
transaction still running (`pg_snapshot_xmin(pg_current_snapshot())`), ordered by `(txid, id)`: nothing can ever
commit behind that line. `test_a_later_commit_never_overtakes_an_earlier_transaction` holds a transaction open,
commits a younger one, and checks that the younger event waits and then arrives after the older one; dropping the
watermark makes it fail. The price is that a running transaction delays younger events until it finishes, which is
milliseconds here (and at most a minute: idle transactions are cut off). Read-only transactions, reconciliation's
long one included, don't hold the line back. Events are kept 7 days and purged in batches.

## Concurrency

Every operation locks the account rows it touches with `SELECT ... FOR NO KEY UPDATE`, **always in ascending
id order**. That ordering is what prevents deadlocks: A→B and B→A transfers at the same time queue up
instead of each holding one lock and waiting for the other (`test_opposite_direction_transfers_do_not_deadlock`).
A balance is checked and changed under the same lock, so no update is lost, and the code doesn't need
`SERIALIZABLE` isolation plus retry loops. Deadlock and serialization errors are still retried
transparently as a safety net.

**The tests found a real bug here.** Ten threads reversing the same transaction always produced exactly
one reversal, but the nine losers reported `INSUFFICIENT_FUNDS` instead of `ALREADY_REVERSED`. They had
passed the "already reversed?" check, waited on the account locks, and by the time they got the locks
the money had already moved back. The fix was to lock the original transaction row first, so reversals
of the same transaction are serialized and later ones see the first.

## Idempotency

Every money-moving endpoint requires an `Idempotency-Key` header. The key is inserted **inside the same
database transaction** as the transfer it protects, so they commit or roll back together. There's no
window where money moved but the key wasn't saved (or the other way round), even if the process
crashes. Two concurrent requests with the same key are serialized by the primary key: the second
`INSERT ... ON CONFLICT DO NOTHING` waits for the first transaction, then returns its stored response with
`Idempotent-Replayed: true`. That wait is capped by `lock_timeout`; if the original request is still running
when it expires, the retry gets `409 IDEMPOTENCY_KEY_IN_USE` (try again shortly) rather than a server error.
Reusing a key with a different body is a `422`. A failed request (say, insufficient funds) is not stored, so it
can be retried with the same key once the account is topped up.

Keys are remembered for 24 hours. A background job deletes expired ones in batches of 5,000. One big `DELETE`
would be all or nothing: Postgres deleted about 1.7M keys per second here, so with a 30-second statement timeout
a backlog of roughly 50M keys (about 14 hours of traffic at 1,000 transfers/s) would time out, roll back, and
fail identically on every later run. Keys are per client (the client id is part of the stored key), so two
clients can pick the same key and neither can ever replay the other's response.

## Reconciliation

`GET /v1/reconciliation` (or `tally reconcile`, which exits 1 on any finding and 2 if it couldn't run) re-derives the books from the raw
entries and holds without trusting any stored total. The triggers already enforce every rule on each write,
but triggers can be bypassed: a superuser in `session_replication_role = replica` mode (as bulk loads and some
restore tools use) skips all of them, foreign keys included. So the report re-checks each rule independently:
unbalanced and incomplete transactions, entries in the wrong currency, stored balances and held amounts against
their sources, and every entry's `balance_after` and timestamp order, which statements and point-in-time
balances depend on.

Each check lists up to 100 findings and reports the exact total. Before these checks existed, a benchmark
database loaded past the triggers had 2M wrong running balances, and the report said nothing about them. The
whole report runs in one `REPEATABLE READ` snapshot, so its counts, trial balance and findings all describe the
same moment while transfers keep committing. It has its own 10-minute statement timeout, because the
connection default is sized for requests. On 2M entries it takes about 3.5 s.

## Load test

[`scripts/load_test.py`](scripts/load_test.py) funds 200 wallets and fires concurrent payments in three patterns,
then checks that every request got a real answer (a transfer or `INSUFFICIENT_FUNDS`, never a 5xx, timeout or
dropped connection), that every accepted payment was captured, that reconciliation is clean, and that the wallets
still hold exactly the money deposited. It exits 1 otherwise, which makes it a CI gate. (The answer check matters: money is also "conserved" when every
transfer fails. Against a server with a 1 ms lock timeout, an earlier version exited 0 while 1,718 of 3,000
transfers came back `503`.)

Measured on 2026-10-31 on an Apple M5 laptop: `tally serve --workers 4` (Python 3.13, the API connected as its
runtime role), Postgres 17.11 in Docker Desktop, load generator on the same machine; 200 wallets, 5,000 requests per
scenario from 100 concurrent clients, two runs each. Commands, environment and raw output are in
[`perf/results/`](perf/results/README.md). With `TALLY_MAINTENANCE_INTERVAL=1` (settlement every second):

| Scenario | Accepted /s | p50 | p99 | Lock wait | Completed end to end |
|---|---|---|---|---|---|
| **Spread**: random wallet to wallet | 2,160 – 2,167 | 42 ms | 118 – 120 ms | 11 – 13 ms | the same: a transfer is done when answered |
| **Hotspot**: everyone pays one merchant, by transfer | 565 – 572 | 42 – 48 ms | 1.56 – 1.62 s | 136 – 137 ms | the same |
| **Hotspot, batched**: the same payments, settled in batches | 1,944 – 2,118 | 35 – 38 ms | 159 – 215 ms | 16 – 20 ms | 1,576 – 1,628 /s; the last payment settled 0.60 – 0.71 s after the last was accepted |

*Accepted* is `201` answers per second: for a payment that is the reservation on the payer, not the merchant's
credit. *Completed end to end* counts captured payments from the first request until none is still reserved.
Latency runs from sending a request to reading its response, so for payments it ends at acceptance. Every run:
reconciliation clean, money conserved, all 5,000 payments captured (none voided, expired or left active).

**The settlement delay is the maintenance interval.** With the default `TALLY_MAINTENANCE_INTERVAL=5`, the batched
row accepts 2,491 – 2,623 /s (fewer settlement batches compete while payments arrive) but completes only 1,106 –
1,266 /s end to end, and the last payment settles 2.0 – 2.5 s after the last acceptance (up to about 5 s in
general). The 0.6 s figure needs `TALLY_MAINTENANCE_INTERVAL=1`. Absolute numbers are lower than an earlier run
against Postgres running natively on the same laptop (about 3,300/s spread and 1,600/s hotspot then): Docker
Desktop runs Postgres in a VM. Compare rows, not runs.

**The hotspot is the instructive number.** Spread traffic grows with the number of worker processes. Hotspot
traffic doesn't, because every transfer needs the merchant's row lock: requests queue behind it, and p99 latency
is ten times the spread case.

**Batched settlement** removes the queue. A payment to a hot account is a hold with `settle_to_account_id`:
creating it locks only the payer's row (the money is reserved, so it can't be spent twice), and a settler in each
worker turns up to 100 pending payments into one transaction that debits every payer and credits the merchant
once. That's how card payments work: authorize now, clear in batches. The merchant's lock is taken once per batch
instead of once per payment, so the batched hotspot accepts payments nearly as fast as spread traffic
(1,944 – 2,118/s against 2,160/s) with a p99 of 0.16 – 0.21 s instead of the transfer hotspot's 1.6 s.
The cost is consistency on that one account: the merchant's balance trails by up to one settlement interval, while
the payer's side is immediate. Two details made it work:

- **Locks that don't block foreign keys.** The payment's foreign key to the merchant takes a `FOR KEY SHARE` lock
  on the merchant's row, which `FOR UPDATE` blocks: payments would still have queued behind every settlement batch.
  Accounts are now locked `FOR NO KEY UPDATE`, which excludes every other locker in Tally the same way but leaves
  key-share locks alone (only balance and held change, never a key). `test_paying_never_waits_on_the_merchants_row`
  holds the merchant's row the way Tally does and checks that a transfer times out while a payment goes through.
- **Batches that stay cheap.** The deferred "debits = credits" check runs once per entry over the whole
  transaction, so a batch costs the square of its size; 100 per batch (the API's own entry limit) keeps it to
  milliseconds. Settlers in several workers run at once without stepping on each other (`SKIP LOCKED`).

**A payment's life.** Once accepted (`201`), a payment ends in exactly one of three ways, and never by the clock:

- **Captured by settlement**, in the next maintenance run that reaches it: one transaction per destination per
  batch. A payment has no expiry (`expires_at` is `null`; sending `expires_in_seconds` with a payment is
  `422 PAYMENTS_DO_NOT_EXPIRE`), hold expiry skips it, and settlement takes every pending payment whatever its age.
  Before migration 010 a payment carried an expiry: if maintenance ran later than that, the payment expired
  unsettled after the client had been told `201`. The migration cleared the expiry of payments still waiting.
- **Voided by its owner** (`POST /v1/holds/{id}/void`) before settlement reaches it.
- **Voided by settlement**, with the reason in `void_reason`, if it can't be posted under the ledger's rules.
  The API refuses such payments up front: a destination in another currency, or one that settling would shrink
  while it may not go negative (paying a wallet into an empty asset account credits that asset). The rule follows
  from normal sides, not a list of account types: `422 INVALID_DESTINATION`. One written before that check, or
  around the API, is voided on its own. Each destination's group is then posted under a savepoint, so a group
  the ledger refuses (a `LedgerError`) is undone and voided while the other groups settle. Any other error (a
  lost connection, a database failure, a bug) is not a verdict on the payments: the whole batch rolls back and
  they wait for the next run.

A payment can't be captured by hand: its destination was fixed when it was accepted, so `POST .../capture` is
`409 PAYMENT_SETTLES_AUTOMATICALLY`, whatever destination is sent. The four maintenance tasks (settle payments,
expire holds, purge idempotency keys, purge events) run one after another, and each failure is logged as
`maintenance task failed: <task>` without stopping the others.

## API

Every `/v1` route needs an API key: `Authorization: Bearer tly_...`. See [Clients and tenancy](#clients-and-tenancy).

| Method | Path | |
|---|---|---|
| `POST` | `/v1/accounts` 🗝️ | Open an account (type, currency, `allow_negative`) |
| `GET` | `/v1/accounts`, `/v1/accounts/{id}` | Balance, held and available amounts |
| `GET` | `/v1/accounts/{id}/entries` | Statement, newest first, with the running balance |
| `GET` | `/v1/accounts/{id}/balance?as_of=` | Balance at any past moment (`as_of` must carry a time zone) |
| `GET` | `/v1/accounts/{id}/holds?status=` | The account's holds, newest first |
| `POST` | `/v1/transfers` 🔑 | Debit one account, credit another |
| `POST` | `/v1/transactions` 🔑 | Any balanced set of 2–100 entries (fees, splits, multi-currency) |
| `GET` | `/v1/transactions`, `/v1/transactions/{id}` | Recent activity; one transaction with its entries |
| `POST` | `/v1/transactions/{id}/reversal` 🔑 | Post the mirror image of a transaction |
| `POST` | `/v1/holds` 🔑 | Reserve funds (card authorization) until `expires_in_seconds` (default 7 days); with `settle_to_account_id`, a payment instead: no expiry, settled in the next batch |
| `POST` | `/v1/holds/{id}/capture` 🔑 | Settle an ordinary hold, optionally for less; the rest is released (a payment: `409 PAYMENT_SETTLES_AUTOMATICALLY`) |
| `POST` | `/v1/holds/{id}/void` 🗝️ | Release a hold |
| `GET` | `/v1/events?cursor=` | Every change to your books, oldest first, without gaps (see [Events](#events-transactional-outbox)) |
| `GET` | `/v1/reconciliation` | Re-derive every balance from raw entries and report any mismatch |
| `GET` | `/metrics` | Prometheus metrics (see [Observability](#observability)) |

🔑 = requires `Idempotency-Key`; 🗝️ = accepts one. Without a key, a retried void whose first attempt
succeeded gets `409 HOLD_NOT_ACTIVE`, and a retried account creation opens a second account. Created
resources come back with a `Location` header. Interactive docs are at `/docs`.

**Errors.** Every failure is an [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem document
(`application/problem+json`) with a stable `code`, including 404s for unknown routes and 405s. The status says
what the client should do:

| Status | Meaning | Examples |
|---|---|---|
| `409` / `422` | Your request can't succeed as sent; don't retry it unchanged | `INSUFFICIENT_FUNDS`, `UNBALANCED`, `HOLD_EXPIRED`, `INVALID_DESTINATION`, `PAYMENTS_DO_NOT_EXPIRE`, `PAYMENT_SETTLES_AUTOMATICALLY`, `IDEMPOTENCY_KEY_REUSED`, `VALIDATION_ERROR`, `INVALID_CURSOR` |
| `413` | Body over 64 KB, refused before it's read (chunked bodies are counted as they arrive) | `CONTENT_TOO_LARGE` |
| `503` + `Retry-After` | Temporary; retry with the same `Idempotency-Key` | `LOCK_TIMEOUT`, `QUERY_TIMEOUT`, `SERVER_BUSY` (pool exhausted), `DATABASE_UNAVAILABLE` |
| `500` | A bug. Logged with the traceback; the response says nothing about internals | `INTERNAL_ERROR` |

A database constraint firing (which validation should have prevented) is a `409 CONSTRAINT_VIOLATION`. The
constraint's name and the failing row go to the log, not to the client.

**Pagination.** List endpoints return `{"data": [...], "next_cursor": "..."}`; pass `next_cursor` back as
`?cursor=` until it is `null`. The cursor is opaque (base64 of the last row's sort key), so the server can change
what's inside without breaking clients. Every list uses keyset pagination on a unique key: `(created_at, id)` for
holds and `(posted_at, id)` for transactions, so rows sharing a timestamp are neither skipped nor repeated.
Statements and the transaction feed read straight from an index, so page 1,000 costs the same as page 1, unlike
`OFFSET`. Holds are indexed only by account, so each page sorts that account's holds; that's cheap at thousands
per account, and an `(account_id, created_at, id)` index would fix it if accounts ever have millions.

## Console

The console is a thin client over the same public API, in plain JavaScript modules with no build step. It's
small, but it has to get the same details right as any client that moves money:

- **One idempotency key per user intent, not per request.** A double click or a retry after a timeout reuses
  the key, so the server moves the money once; the key is retired only after a definite answer
  ([`actions.js`](src/tally/static/actions.js)). Before this, a double-clicked "Post transfer" posted twice.
- **No floats between the user and the API.** Typed amounts are parsed as exact decimals into minor units using
  each currency's own digits (2 for USD, 0 for JPY, 3 for KWD), and input that isn't exact (`1.005` dollars) is
  refused rather than rounded ([`money.js`](src/tally/static/money.js)).
- **Polling that doesn't hurt the database.** Lists refresh every 4 s, never overlap, and re-render only when the
  data changed. Reconciliation scans every entry (1.5 s at 1.4M entries), so it runs every minute and after each
  write instead of on every poll.
- **Defense in depth.** Everything rendered is HTML-escaped, and the page is served with a Content-Security-Policy
  that only allows its own scripts, so an escaping slip still couldn't run injected code. Files are served
  `Cache-Control: no-cache` (a cheap 304 when unchanged), so after a deploy a browser can't run a cached
  `app.js` against a new `index.html`.

## Observability

- **Request IDs.** Every response carries `X-Request-ID`: the caller's own, if it's a plain token, otherwise a
  new one. Error bodies include it, and so does every log line written while the request runs, so the ID a
  client quotes in a bug report finds everything the server did about it.
- **JSON logs.** One line per request (`method`, `route`, `status`, `ms`, `request_id`), plus warnings and
  errors with their tracebacks. `/health` and `/metrics` are left out of the access log, since they're polled
  every few seconds.
- **`/metrics`** in Prometheus format:

| Metric | What it shows |
|---|---|
| `tally_http_request_duration_seconds{method, route, status}` | Latency histogram per route template (`/v1/accounts/{account_id}`, not one series per id) |
| `tally_account_lock_wait_seconds` | Time spent acquiring account row locks: the number that climbs when an account gets hot |
| `tally_db_retries_total{error}` | Transactions retried after a deadlock or serialization failure; should stay at 0 |
| `tally_problems_total{code}` | Error responses by code (`INSUFFICIENT_FUNDS`, `LOCK_TIMEOUT`, ...) |
| `tally_settled_payments_total` | Payments settled by the batch settler |
| `tally_settlement_voided_payments_total` | Payments settlement refused and voided (the reason is on the hold, `void_reason`) |

Under `tally serve --workers N` each worker is its own process with its own counters. The CLI points
`PROMETHEUS_MULTIPROC_DIR` at a fresh directory before the workers start, so `/metrics` reports the sum over all
of them whichever worker answers (checked: 20 requests spread over 2 workers read as 20 on every scrape).

## Run it

```bash
echo "TALLY_APP_DB_PASSWORD=$(openssl rand -hex 16)" > .env   # the API's database password (git-ignored)
docker compose up --build -d --wait          # migrates as the owner, then starts the API; returns once healthy
docker compose run --rm admin tally demo     # sample data, and prints the demo client's API key
open http://localhost:8000                   # the console (paste the key); API docs at /docs
```

Admin commands (`demo`, `create-client`, `create-key`, `revoke-key`, `list-keys`, `migrate`) run in the `admin`
service, as the schema owner; the API itself can't create clients or keys (see [Database roles](#database-roles)).
A data volume created before the roles were split doesn't have the API's login role yet; create it once with
`docker compose exec postgres sh /docker-entrypoint-initdb.d/10-runtime-role.sh`.
Ports are published on `127.0.0.1` only (the owner's password is `tally`); if 8000 or 5432 is taken, set
`TALLY_PORT` or `TALLY_DB_PORT`. The ledger lives in a named volume, so it survives `docker compose down`;
`down -v` deletes it. The image installs the exact dependency versions in
[`requirements.txt`](requirements.txt), the same ones CI tests, and Dependabot proposes updates weekly.

**Updating dependencies.** `requirements.txt` pins the runtime, `requirements-dev.txt` adds every test tool and
their dependencies on top (`-r requirements.txt`); `pyproject.toml` keeps the compatible ranges. Dependabot's
weekly PR moves the pins in both files and has to pass CI. After changing the dev tools in `pyproject.toml`,
regenerate the dev pins with [`scripts/lock-dev.sh`](scripts/lock-dev.sh), which resolves them in a clean
`python:3.14-slim` container against the runtime pins.

Without Docker, against your own PostgreSQL (tested on 17):

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt && pip install --no-deps -e . && pip check   # what CI installs
export TALLY_DATABASE_URL=postgresql://owner:pass@localhost:5432/tally          # the schema owner
tally migrate && tally demo
tally create-client acme               # prints acme's first API key; also create-key, revoke-key, list-keys
psql "$TALLY_DATABASE_URL" -c "CREATE ROLE tally_api LOGIN PASSWORD '...' IN ROLE tally_app"
TALLY_DATABASE_URL=postgresql://tally_api:...@localhost:5432/tally tally serve   # the API, as the runtime role
tally reconcile                        # exit code 1 if anything disagrees
```

Exit codes are meant for schedulers: `0` done, `1` reconciliation found discrepancies, `2` the command couldn't
run (bad flag or configuration, database unreachable or not migrated, an edited migration). A cron job that pages
on `1` therefore never pages "the books are wrong" because the database was down. Operator mistakes print one
line; only bugs print a traceback. `tally demo` seeds in a single transaction and only into an empty ledger, so
running it twice, or killing it halfway, leaves nothing duplicated or half-written.

| Variable | Default | |
|---|---|---|
| `TALLY_DATABASE_URL` | `postgresql://tally:tally@localhost:5432/tally` | Never printed: `Settings` redacts the password in logs and tracebacks |
| `TALLY_POOL_SIZE` | `20` | Connections per worker process |
| `TALLY_LOCK_TIMEOUT_MS` | `5000` | Give up waiting for a row lock (Postgres `lock_timeout`) |
| `TALLY_STATEMENT_TIMEOUT_MS` | `30000` | Cancel any statement that runs longer (`statement_timeout`) |
| `TALLY_MAINTENANCE_INTERVAL` | `5` | Seconds between maintenance runs: payment settlement, hold expiry, idempotency-key and event cleanup. A payment is credited within about one interval |
| `TALLY_APP_DB_PASSWORD` | none | Compose only: the API's database password, used by `compose.yaml` and the Postgres init script |

### Database roles

Every rule above is enforced by the database, but an owner of the schema can switch any of them off:
`ALTER TABLE ... DISABLE TRIGGER`, drop a constraint, `SET session_replication_role = replica`. So the API doesn't
connect as the owner (migration 012):

- **The owner** (`tally` in Compose) runs migrations and the admin CLI. In Compose that's the one-shot `admin`
  service, which migrates and exits before the API starts.
- **The API** connects as a login role that is a member of `tally_app` and nothing else (`tally_api` in Compose,
  created by [`docker/postgres-init/10-runtime-role.sh`](docker/postgres-init/10-runtime-role.sh) with the
  password from `TALLY_APP_DB_PASSWORD`). It isn't a superuser and owns nothing, so it can't alter tables,
  disable triggers, switch on replica mode or become the owner. It gets exactly the privileges the code uses,
  column by column for updates: it can insert entries but not delete them, change a hold's status but not its
  amount or expiry, and read keys but not create or revoke them. Audit rows and events are written by
  `SECURITY DEFINER` trigger functions, so the API can neither forge nor delete them; expired events are removed
  only through `purge_outbox_events()`, which never touches anything younger than 7 days.
- At startup the API checks that no migration is pending (it can read `schema_migrations` but not apply anything)
  and refuses to start, telling you to run `tally migrate` as the owner, if one is.

The owner, and any superuser, remain inside the trust boundary: they can still do everything listed above, and
so can anyone holding their credentials. What this buys is that a bug or an attacker in the API process can't.
The API's role can also still do everything the API legitimately does, across all clients (it is the
multi-tenant service), and can set the session values the audit trigger records as the acting key.
`test_runtime_role.py` connects as such a role and checks both sides.

### Connection handling

Every pooled connection gets `lock_timeout`, `statement_timeout` and `idle_in_transaction_session_timeout`.
Without them, one transaction stuck behind a row lock (an engineer's open psql session, a hung client) waits
forever while holding a pool connection, and enough of those drain the pool. Connections are pinged before
use only if they sat idle for over a second, the trick HikariCP uses. Pinging on every checkout cost about 9%
of transfer throughput in the load test (3,130–3,160 → 2,820–2,880 transfers/s). Skipping recently used
connections brought it back, and still catches connections killed by idle timeouts or a database restart.
The migration runner stores a checksum of each applied file and refuses to start if one was edited afterwards,
because a database that already ran a migration never sees later edits to it.

## Tests

```bash
pip install -r requirements-dev.txt && pip install --no-deps -e . && pip check   # exact versions, as CI
pytest -q        # starts a throwaway postgres:17 container, or set TALLY_TEST_DATABASE_URL
pytest -q --cov  # plus statement and branch coverage; fails under 90% combined (CI runs this)
coverage json -o coverage.json   # after --cov: totals.covered_lines/num_statements, covered_branches/num_branches
pytest tests/test_invariants.py --hypothesis-profile=thorough   # 500 random sequences instead of 100
python scripts/mutation_check.py tests/mutants/capture_releases_after_spending.patch \
    tests/test_holds.py::test_a_capture_at_the_funds_boundary_succeeds tests/test_invariants.py
node --test 'tests/js/*.test.js'   # the console's money and idempotency logic (Node 22+, no npm install)
# Docker Desktop on macOS without the default socket enabled also needs:
# export DOCKER_HOST=unix://$HOME/.docker/run/docker.sock
```

297 Python tests, all against a real PostgreSQL because the rules under test live in the database, plus 11 for
the console's JavaScript; warnings fail the run. The API's tests, `books` and the maintenance tests connect as the
runtime role, not the owner. Coverage, measured on 2026-10-31 with `pytest -q --cov` and `coverage json` from a
clean `requirements-dev.txt` install (same figures on Python 3.14 and 3.11):

| Statements | Branches | Combined (what `--cov` prints; the 90% floor applies to it) |
|---|---|---|
| 98.2% (1,127 of 1,148) | 94.5% (189 of 200) | 97.6% |

An earlier version of this README said "98% branch coverage". Measured the same way on that commit (`a717e4f`), it
was 96.8% combined, with branches at 91.8% (178 of 194) and statements at 97.7%.

| Suite | What it covers |
|---|---|
| `test_transactions.py` (24) | Deposits, multi-leg fees, per-currency balancing, overdrafts, reversals, statements, point-in-time balances, leg validation, entry-by-entry overdraft checks, single-entry posts, 404s for unknown transactions and holds |
| `test_holds.py` (18) | Authorize, partial capture, void, expiry in bounded batches, capture on debit-normal accounts, double settlement, capturing the last of the funds and every capture at the funds boundary, capturing into the held account, expiry and amount validation |
| `test_database_invariants.py` (39) | **Raw SQL that bypasses the app** to try to break each rule, and gets rejected; a transaction touching another client's account, a forged entry client, an account changing owner, an editable audit log, entries appended to committed, reversed or reversing transactions, a forged `created_xid` |
| `test_concurrency.py` (4) | 50 threads draining one account, opposite-direction transfers, one idempotency key from 20 threads, concurrent reversals. Each race gets its own pool, filled first, so all 60 transactions really run at once, and fails if any transaction was retried, so a deadlock can't hide behind `db.run`'s retries |
| `test_api.py` (9) | HTTP contract, idempotent replay, key reuse, problem details, pagination, holds, reconciliation |
| `test_api_errors.py` (22) | A real row lock becomes `503 LOCK_TIMEOUT`; timeouts, pool exhaustion and outages become 503s; constraint names and crashes don't leak; oversized and chunked bodies get 413; naive timestamps and forged cursors are rejected; paging 120 holds that share one timestamp; optional idempotency; `Location` headers; error codes that don't depend on the Python version |
| `test_tenancy.py` (13) | Missing, malformed, unknown and revoked keys get `401`; key rotation; only a hash stored; another client's accounts, transactions and holds are `404` for every read and write and no money moves; idempotency keys per client; reconciliation per client; audit rows naming key and request, none for replays or failures, keyless rows for changes made around the API |
| `test_events.py` (9) | One event per change with the transaction's entries; every kind of change; nothing for a rollback or a commit-time failure; per client; paging resumes exactly; an older transaction committing later is never skipped; batched purge; forged cursors |
| `test_settlement.py` (11) | A payment reserves on the payer and pays the merchant at settlement, one transaction per batch; bounded batches; paying never waits on the merchant's row lock (a transfer does); destination validation; voided payments skipped; two settlers at once settle each payment once; a payment written around the API can't stall settlement; the destination can't change |
| `test_payment_lifecycle.py` (22) | Over HTTP with two clients: which destinations a payment may have (9 account-type pairings, refused ones leave no trace); a legacy invalid payment voided alone while another client's settles; a group refused at posting undone by its savepoint and voided while the next settles; connection errors, bugs and unknown errors voiding nothing; repeated maintenance changing nothing; payments refusing an expiry, outliving late and paused maintenance (the review's 1 s reproduction), and settled exactly once while expiry races settlement; manual capture of a payment refused for its own, another or no destination |
| `test_runtime_role.py` (36) | The API's role is no superuser and owns nothing; 29 escalation attempts refused (disabling triggers, replica mode, `SET ROLE`, editing history, the audit log, events, keys and migration history); columns granted only for locking still can't be written; the API starts as that role and refuses pending migrations; it purges only expired events |
| `test_cli.py` (21) | Exit codes (0 / 1 for discrepancies / 2 for a database that's down, unmigrated or edited), fast failure without a pool timeout, demo refusing a non-empty ledger, rolling back when it fails halfway, two demos at once seeding once, argument and config validation, Ctrl-C; API keys: only the key on stdout, revoke and list, unknown clients and keys, taken names |
| `test_console.py` (3), `tests/js/` (11) | The console's CSP and cache revalidation; exact money parsing and formatting per currency; one idempotency key per intent (double clicks send one request, retries reuse the key, definite answers retire it) |
| `test_idempotency.py` (8) | Run once and replay, key reuse, per-operation scope, failed requests not remembered, a retry racing the original, a key purged mid-claim, batched purge |
| `test_maintenance.py` (7) | The background job: drains a backlog of overdue holds in one run, forgets keys past the replay window, survives a failed run, a failing task (database gone, or a bug) doesn't stop the others, stops promptly on shutdown |
| `test_observability.py` (9) | Request IDs generated, echoed or replaced, and quoted in errors; one JSON access line per request; latency per route template; errors by code; lock waits; retried transactions; the client and key in the access log |
| `test_reconciliation.py` (10) | Damages the ledger past every trigger (replica mode) and checks each kind of damage is reported; capped lists with exact totals; one consistent snapshot while transfers commit; an entry crossing into another client's books |
| `test_migrations.py` (12) | Ordered, idempotent migrations; valid concurrent indexes; the hot queries use them; four instances migrating at once without deadlocking; edited migrations refused; payments waiting at the upgrade to 010 lose their expiry and settle; transactions from before 011 are sealed |
| `test_db.py` (8), `test_config.py` (11) | Commit/rollback, deadlock retries in fresh transactions, lock timeouts, replacing dropped connections, idle-only pings; config parsing, validation and password redaction |
| `test_invariants.py` | **Model-based testing**: Hypothesis generates 100 random sequences (500 with the `thorough` profile) of up to 40 operations: deposits, transfers, holds, captures, voids, reversals, payments (valid, refused, and invalid ones written around the API), settlement in random batch sizes, hold expiry, whole maintenance runs repeated up to three times, and manual captures of payments. Amounts lean towards "all of what's available". After *every* step it checks that reconciliation is clean, the database matches an in-memory model balance for balance and hold for hold (status and void reason), and no protected account is overdrawn. On the laptop above: about 19 s for 100 sequences, 96 s for 500 |

Tests are only worth what they catch, so the important ones were checked by planting bugs (mutation testing):

- A partial capture that releases only the captured amount instead of the whole hold: Hypothesis found a failing
  sequence in about a second. That patch can't be replayed any more (since migration 005 a trigger releases the
  hold's own amount), so its boundary is kept as an equivalent mutant,
  [`tests/mutants/capture_releases_after_spending.patch`](tests/mutants/capture_releases_after_spending.patch):
  the reservation is released after the spend instead of before, which refuses any capture of most of the funds.
  [`scripts/mutation_check.py`](scripts/mutation_check.py) applies it to a temporary copy, requires each test to
  pass without it and fail with it, and deletes the copy (command above). The deterministic boundary test fails
  every time. The state machine (no shrinking, up to 300 sequences) caught it in 5 of 5 runs, after 1.5–17 s; with
  CI's 100 sequences it was 8 of 10 runs, which is why the deterministic test exists.
- 10% of transactions locking accounts in reverse order. Before the race harness was tightened, the concurrency
  tests ran only 18 of their 60 transactions at once and let `db.run` retry the resulting deadlocks; one run in
  six passed with the bug in place. Now all six runs fail.
- The maintenance loop processing one batch per run, or letting an exception kill the thread: each breaks a test.

CI installs exact versions of every test tool from [`requirements-dev.txt`](requirements-dev.txt) and runs the
suite on Python 3.11 (the oldest `pyproject.toml` allows) and 3.14 (what the image runs), plus lint and the
JavaScript tests, then builds the image, starts the Compose stack, runs the load test against it
and `tally reconcile` inside the container. Testing 3.11 paid off at once: the framework's error codes were
derived from `HTTPStatus` names, which Python 3.13 renamed (`413` was `REQUEST_ENTITY_TOO_LARGE`), so the same
error had a different `code` depending on the server's Python. The workflow token is read-only, actions are
pinned to commit SHAs (a tag can be moved to other code, a commit can't), runs time out after 15 minutes, and a
newer push cancels the older run.

## Project layout

```
migrations/001_ledger.sql   schema, constraints, triggers: the rules of the ledger
migrations/002_*.sql        indexes for the slow queries, built CONCURRENTLY
migrations/003_*, 004_*     hardening: TRUNCATE guard, fixed account identity, hold lifecycle,
                            DB-stamped timestamps, metadata CHECKs (added NOT VALID, then validated)
migrations/005_*            balances and held amounts maintained by triggers; direct writes refused
migrations/006_*, 007_*     API clients, keys, the audit log; tenant foreign keys added without blocking writes
migrations/008_*            the outbox: an event per change, written by triggers in the same transaction
migrations/009_*            payments to hot accounts: holds with a destination, settled in batches
migrations/010_*            payments have no expiry; void_reason for payments settlement refuses
migrations/011_*            committed transactions are sealed (created_xid)
migrations/012_*            the API's least-privilege role, tally_app
src/tally/
├── ledger.py               validation, ordered row locks, posting, holds, reversals, statements, batched settlement
├── auth.py                 API clients and keys (hashed), and who is acting
├── events.py               reading the outbox in commit-safe order, and purging it
├── idempotency.py          idempotency keys in the same transaction as the operation
├── reconciliation.py       re-checks every trigger-enforced rule in one REPEATABLE READ snapshot
├── db.py                   connection pool with timeouts and idle checks, deadlock retry, migration runner
├── api.py, schemas.py      FastAPI routes, Pydantic models, RFC 9457 errors
├── observability.py        request IDs, JSON logs, Prometheus metrics (summed across worker processes)
├── cli.py                  tally migrate | serve | reconcile | demo | create-client | create-key | revoke-key | list-keys
└── static/                 the console (plain JS modules, no build step)
scripts/load_test.py        concurrent load test: answers, settlement, reconciliation, conservation; --json keeps raw numbers
scripts/index_benchmark.py  EXPLAIN ANALYZE before/after migration 002 on 2M entries (refuses non-bench databases)
scripts/mutation_check.py   applies a mutant to a temporary copy and checks the tests catch it
scripts/lock-dev.sh         regenerates requirements-dev.txt in a clean container
perf/results/               raw load test output, with the commands and environment
tests/mutants/              planted bugs the tests must catch
docker/postgres-init/       creates the API's login role when the Compose database is first created
requirements.txt            exact runtime versions for the image and CI (Dependabot keeps them current)
requirements-dev.txt        those plus exact versions of every test tool, for CI and local development
```

## Not built (yet)

- **FX between currencies.** Each currency balances on its own; a conversion would post through FX accounts with a rate.
- **Row-level security** as a third layer of tenant isolation, under the ledger's filters and the composite foreign keys: `SET LOCAL` the client per transaction and let Postgres hide other clients' rows even from a forgotten `WHERE`. The API now runs as a non-owner role (migration 012), which RLS needs; what's left is the policies, and care with the jobs that work across clients (settlement, expiry).
- **Scale for very long histories:** partition `entries` by month, archive old partitions, and snapshot balances so point-in-time queries don't touch cold data.
- **Pushing events out.** Consumers poll `/v1/events`; a relay forwarding the outbox to Kafka, or signed webhooks with retries, would read the same table with the same watermark.
- **Settlement status in one place.** A batched payment's state is its hold's (`active` until settled, then `captured` with the settlement transaction, or `voided` with a `void_reason`); a backlog gauge would need Prometheus' multiprocess gauge mode.

## License

MIT
