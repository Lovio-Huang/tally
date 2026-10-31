# Load test results

Raw output of [`scripts/load_test.py`](../../scripts/load_test.py): `*.txt` is what it printed, `*.json` the
same numbers written by `--json` (with the `--note` describing the server). The main README quotes these files.

## 2026-10-31

**Environment.** Apple M5 laptop (Mac17,2, 10 cores, 24 GB), macOS 27.0.1. PostgreSQL 17.11
(`postgres:17-alpine`) in Docker Desktop 29.8.2, whose VM has 10 CPUs and 7.7 GiB. The API ran natively
(Python 3.13.15): `tally serve --workers 4`, pool of 20 connections per worker (the default), connected as the
runtime role (README, "Database roles"). The load generator ran on the same machine, so it competes with the
server for CPU.

**Scale.** 200 wallets funded with 10,000 each, then three scenarios of 5,000 requests from 100 concurrent
clients: spread transfers, hotspot transfers (everyone pays one merchant), and the same hotspot as payments
settled in batches. Two runs per maintenance interval. The database was recreated before each interval's runs.

| File | `TALLY_MAINTENANCE_INTERVAL` |
|---|---|
| `2026-10-31-interval1-run{1,2}.*` | `1` |
| `2026-10-31-interval5-run{1,2}.*` | unset: the default, `5` |

**Commands** (the database URL, port and role names are what these runs used; any empty database works):

```bash
OWNER=postgresql://tally:tally@127.0.0.1:55433/tally_perf
TALLY_DATABASE_URL=$OWNER tally migrate
APP_PASSWORD=$(openssl rand -hex 16)
psql "$OWNER" -c "CREATE ROLE tally_perf_api LOGIN PASSWORD '$APP_PASSWORD' IN ROLE tally_app"
export TALLY_API_KEY=$(TALLY_DATABASE_URL=$OWNER tally create-client perf)

TALLY_DATABASE_URL=postgresql://tally_perf_api:$APP_PASSWORD@127.0.0.1:55433/tally_perf \
TALLY_MAINTENANCE_INTERVAL=1 tally serve --port 8012 --workers 4 &

python3 scripts/load_test.py --base-url http://127.0.0.1:8012 --wallets 200 --requests 5000 --concurrency 100 \
    --json perf/results/2026-10-31-interval1-run1.json --note "..." | tee perf/results/2026-10-31-interval1-run1.txt
```

**Results.** Every run: every request answered `201`, reconciliation clean, money conserved, and all 5,000
payments captured (none voided, expired or left active).

| Interval | Scenario | Accepted /s | HTTP latency p50 / p99 | Lock wait (mean) | Completed end to end | Each payment's completion latency | Drain: last settled after last accepted |
|---|---|---|---|---|---|---|---|
| 1 s | spread | 2,160 – 2,167 | 42 ms / 118 – 120 ms | 11 – 13 ms | = accepted | = HTTP latency | n/a |
| 1 s | hotspot | 565 – 572 | 42 – 48 ms / 1.56 – 1.62 s | 136 – 137 ms | = accepted | = HTTP latency | n/a |
| 1 s | hotspot, batched | 1,944 – 2,118 | 35 – 38 ms / 159 – 215 ms (acceptance only) | 16 – 20 ms | 1,576 – 1,628 /s (3.07 – 3.17 s) | not measured | 0.60 – 0.71 s |
| 5 s | spread | 2,201 – 2,284 | 38 – 43 ms / 120 – 127 ms | 9 – 14 ms | = accepted | = HTTP latency | n/a |
| 5 s | hotspot | 568 – 576 | 37 – 41 ms / 1.85 – 1.86 s | 135 – 137 ms | = accepted | = HTTP latency | n/a |
| 5 s | hotspot, batched | 2,491 – 2,623 | 37 – 39 ms / 72 – 74 ms (acceptance only) | 4 ms | 1,106 – 1,266 /s (3.95 – 4.52 s) | not measured | 2.04 – 2.51 s |

How to read it (definitions in the script's docstring):

- **Accepted /s** is `201` answers per second of the scenario's wall time. For a transfer that is a finished
  transfer. For a payment it is only the reservation on the payer; the merchant is credited later.
- **HTTP latency** is per request, on the client: from just before the request is sent until its response has been
  read. For transfers that is completion; for payments it ends at acceptance, not settlement. The script doesn't
  record when each payment settled, so per-payment completion latency (and its percentiles) is not measured.
- **Drain** is how long after the last payment was accepted the last one was settled: the completion-side tail.
- **Completed end to end** (batched only) counts captured payments per second from the first request sent until
  the moment no payment was still reserved, which the script polls every 50 ms.
- **Lock wait** is the server's mean time to acquire account row locks, from `/metrics`: one observation per
  locking statement, so settlement batches are included in the batched row.
- Limits: one machine; the load generator shares it with the server; the server ran Python 3.13, the image runs
  3.14; two runs per setting.
- The settlement delay depends on the interval: a payment waits for the next maintenance tick, so with the default
  5 s interval the last one settles anywhere up to about 5 s after it was accepted. The 0.6–0.7 s figure needs
  `TALLY_MAINTENANCE_INTERVAL=1`. With the default, fewer settlement batches run during the acceptance window,
  which is why acceptance is faster there and end-to-end throughput slower.
