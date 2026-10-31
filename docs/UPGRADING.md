# Upgrading a running ledger to migrations 010–012

For a database at migration 009 (code up to `a717e4f`). Three migrations change how the API must run:

| Migration | What changes |
|---|---|
| `010_payment_lifecycle` | Payments (holds with `settle_to_account_id`) have no expiry: waiting payments lose theirs, new ones must have none (`CHECK`). Adds `holds.void_reason`. |
| `011_sealed_transactions` | Entries can be added to a transaction only by the database transaction that created it. Existing transactions are sealed. |
| `012_runtime_role` | Creates `tally_app`, the role the API must now connect as (through a login role that is a member of it), with least-privilege grants. |

## The old version can't run against 010 or later

Run the upgrade with the old version stopped, not as a rolling deploy:

- **Old code can't create payments after 010.** It always gives a payment an expiry, which the new `CHECK`
  refuses: every payment request fails with `409 CONSTRAINT_VIOLATION`.
- **Old code never settles a payment the new code accepted.** Its settlement only takes payments whose
  `expires_at > now()`, which is never true for `NULL`, and its expiry skips them too: they would stay reserved.
- **Old code can't run as the runtime role.** It creates the migration table at startup and deletes old events
  directly, and the runtime role may do neither (after 012).

The maintenance jobs (settlement, expiry, cleanup) run inside the API's worker processes, so stopping every API
process stops them too.

## Steps

With Docker Compose (the same steps without it are below each command's comment):

```bash
# 1. Stop the old API, and with it the old maintenance loop. Every replica and worker.
docker compose stop app

# 2. Back up, and keep the list of payments the old code let expire (see "Payments the old code expired").
docker compose exec -T postgres pg_dump -U tally -Fc tally > tally-before-010.dump

# 3. New code, the API's database password, and the postgres container recreated with the new compose.yaml
#    (its environment and init script mount; the data volume is kept).
git pull
echo "TALLY_APP_DB_PASSWORD=$(openssl rand -hex 16)" > .env
docker compose up -d postgres

# 4. Migrate as the schema owner. Prints the versions it applied: 010, 011, 012.
#    Without Compose: TALLY_DATABASE_URL=<owner URL> tally migrate
docker compose build && docker compose run --rm admin

# 5. Create the API's login role (a member of tally_app). Safe to rerun: it only resets the password.
#    Without Compose: CREATE ROLE tally_api LOGIN PASSWORD '...' IN ROLE tally_app;
docker compose exec postgres sh /docker-entrypoint-initdb.d/10-runtime-role.sh

# 6. Start the new API, connected as tally_api. It refuses to start if any migration is pending.
docker compose up -d --build --wait
```

Migration 012 creates the `tally_app` role, so the owner needs `CREATEROLE`. Where it doesn't have it (managed
Postgres), have an administrator run `CREATE ROLE tally_app NOLOGIN` first; 012 then skips creating it.

## Checks after the upgrade

```bash
curl -fsS http://localhost:8000/health                   # {"status":"ok"}
docker compose exec -T app tally reconcile               # exit code 0: every balance matches its entries
```

```sql
-- The API connects as tally_api, not as the owner.
SELECT usename, count(*) FROM pg_stat_activity WHERE datname = current_database() GROUP BY 1;

-- Payments still waiting. Those from before the upgrade settle in the first maintenance run
-- (TALLY_MAINTENANCE_INTERVAL, 5 s by default).
SELECT count(*) FROM holds WHERE status = 'active' AND settle_to_account_id IS NOT NULL;

-- Payments settlement refused, with the reason. Before 010 the API accepted payments it now refuses
-- (a destination that settling would overdraw); they are voided here, once, and their reservation released.
SELECT id, client_id, account_id, settle_to_account_id, amount, created_at, void_reason
  FROM holds WHERE void_reason IS NOT NULL ORDER BY created_at;
```

The maintenance log line `maintenance task failed: <task>` means a task failed and will be retried at the next
interval; it never means payments were voided.

## Retrying, and when a migration fails

Each migration runs in its own transaction. If one fails, nothing of it remains, the ones before it stay applied,
and `schema_migrations` says where it stopped. Fix the cause and run step 4 again: it continues from the
migration that failed. Steps 4–6 can be repeated safely; repeating them changes no data.
`scripts/upgrade_acceptance.py` checks all of this against a copy built by the old code (see below).

A database stopped between migrations (say 010 and 011 applied, 012 not) can be served by neither version: the
old code is incompatible from 010 on, and the new API refuses to start until 012 is applied. Finish the upgrade,
or restore the backup.

## Rolling back

There are no down migrations, and the old version can't serve a database at 010 or later (see above). So:

- **Before step 6** (nothing new has been written), restoring the backup from step 2 and starting the old version
  is safe.
- **After step 6**, prefer fixing forward. Restoring the backup also discards every transfer, payment and hold
  made since, which the clients were told succeeded. Running the old version against the upgraded database
  without restoring is not an option: payments accepted after the upgrade would never settle.

## Payments the old code expired

Before 010, a payment carried an expiry, and if maintenance ran later than that expiry, the payment expired
unsettled: the client had been told `201`, the payer got the reserved money back, and the merchant was never paid.
The upgrade doesn't touch these (a settled hold is final) and nothing settles them automatically. Review each one
by hand:

```sql
-- Every payment that ended by expiring, with the request that created it.
SELECT h.id, c.name AS client, h.account_id AS payer, h.settle_to_account_id AS destination, h.amount,
       h.created_at, h.expires_at, h.updated_at AS expired_at,
       a.api_key_id, a.request_id
  FROM holds h
  JOIN api_clients c ON c.id = h.client_id
  LEFT JOIN audit_log a ON a.resource_id = h.id::text AND a.action = 'hold.created'
 WHERE h.settle_to_account_id IS NOT NULL AND h.status = 'expired'
 ORDER BY h.created_at;

-- For one of them: did the payer pay the same destination again afterwards (a retry)?
SELECT id, amount, status, created_at FROM holds
 WHERE account_id = :payer AND settle_to_account_id = :destination AND created_at > :created_at
 ORDER BY created_at;
```

For each payment, with the client:

1. Find out what the payment was for, from the request id and the client's own records.
2. Check whether it was already made good: a later payment or transfer for the same purpose, or a refund on the
   client's side.
3. If money still has to move, the client sends a new payment (or ops posts a transfer at the client's request)
   with a description naming the expired hold's id. Never reopen or re-settle the expired hold: settled holds are
   final, the database refuses to reopen them, and doing it by switching triggers off as the owner would bypass
   the very checks that keep the books right.
4. Record the decision and the id of whatever was posted.

## Acceptance check

`scripts/upgrade_acceptance.py` replays this upgrade on throwaway databases. It builds a ledger with the old code
(`git archive a717e4f`), through the old API: an active hold, one about to expire, a captured hold, a settled
payment, one the old code let expire, waiting payments (one past its old expiry), a payment to a destination the
new API refuses, and a reversed transfer. It then upgrades it and checks the data, the new semantics under the
runtime role, sealed history, failures injected into 010 and 012, and a second run of the deploy steps. It needs a
superuser on a server where it may create databases, and drops what it creates:

```bash
python scripts/upgrade_acceptance.py --admin-url postgresql://tally:tally@127.0.0.1:5432/postgres
```
