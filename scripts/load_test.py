#!/usr/bin/env python3
"""Load test for the Tally API. Standard library only.

Three scenarios:

  spread           random transfers between many wallets: little lock contention
  hotspot          every customer pays the same merchant with a transfer: every
                   request needs the same row lock, the worst case for a ledger
  hotspot, batched the same payments as holds settled in batches
                   (settle_to_account_id): only the payer's row is locked per
                   request; the merchant is credited once per batch, up to one
                   maintenance interval later (TALLY_MAINTENANCE_INTERVAL on the
                   server), and the run waits for that to finish

What each number measures:
  accepted     requests answered 201 per second of the scenario's wall time. For a
               transfer that is a completed transfer; for a payment it is only a
               reservation, settled later
  latency      client side, per request: from just before the request is sent to the
               moment its response has been read (for a payment: until accepted)
  settlement   (batched) what became of every accepted payment: captured, voided,
               expired or still active, read back from the API afterwards
  completed    (batched) captured payments per second, from the first request sent
               to the moment the last payment was settled: end-to-end throughput
  lock wait    mean time to acquire account row locks, from the server's /metrics

After each run it checks the things that must never break under load:
  * every request got a real answer: a transfer or INSUFFICIENT_FUNDS (a wallet
    can run dry), never a 5xx, a timeout or a dropped connection
  * /v1/reconciliation is clean (every balance equals the sum of its entries)
  * money is conserved: the wallets hold exactly what was deposited

It exits 1 if any check fails, so CI can run it as a gate.

Usage (the key comes from `tally create-client <name>`):
  python3 scripts/load_test.py --api-key tly_... --base-url http://localhost:8000 --requests 5000 --concurrency 50
  add --json results.json to keep the raw numbers, with --note describing the server's setup
"""

import argparse
import http.client
import json
import os
import random
import statistics
import sys
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

_local = threading.local()
API_KEY = ""  # set from --api-key / TALLY_API_KEY in main()

# The only refusal a correct ledger gives under this load: a wallet ran out of money.
EXPECTED = {"ok", "INSUFFICIENT_FUNDS"}
SETTLEMENT_TIMEOUT_SECONDS = 120


def call(base, method, path, body=None, idempotent=False):
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {API_KEY}"}
    if idempotent:
        # Generated once per logical request: if the connection drops and we retry,
        # the same key guarantees the transfer still happens at most once.
        headers["Idempotency-Key"] = str(uuid.uuid4())
    for attempt in range(2):
        conn = getattr(_local, "conn", None)
        if conn is None:
            conn = _local.conn = http.client.HTTPConnection(base.hostname, base.port or 80, timeout=30)
        try:
            conn.request(method, path, body=json.dumps(body) if body is not None else None, headers=headers)
            response = conn.getresponse()
            data = response.read()
            return response.status, json.loads(data) if data else None
        except (http.client.HTTPException, OSError):
            # A keep-alive connection the server closed while idle, or a timeout: reconnect
            # and retry once. The same Idempotency-Key makes a retried transfer safe.
            conn.close()
            _local.conn = None
            if attempt == 1:
                raise


def create_account(base, name, kind):
    status, body = call(base, "POST", "/v1/accounts", {"name": name, "type": kind, "currency": "USD"})
    assert status == 201, body
    return body["id"]


def setup(base, wallets, deposit):
    cash = create_account(base, "Load test cash", "asset")
    ids = [create_account(base, f"wallet {i}", "liability") for i in range(wallets)]
    merchant = create_account(base, "Load test merchant", "liability")
    for wallet in ids:
        status, body = call(
            base,
            "POST",
            "/v1/transfers",
            {"debit_account_id": cash, "credit_account_id": wallet, "amount": deposit},
            idempotent=True,
        )
        assert status == 201, body
    return ids, merchant


def lock_waits(base) -> tuple[float, float]:
    """(total seconds, count) of account lock acquisitions so far, from the Prometheus text."""
    conn = http.client.HTTPConnection(base.hostname, base.port or 80, timeout=30)
    conn.request("GET", "/metrics")
    text = conn.getresponse().read().decode()
    conn.close()
    totals = {"_sum": 0.0, "_count": 0.0}
    for line in text.splitlines():
        for suffix in totals:
            if line.startswith(f"tally_account_lock_wait_seconds{suffix}"):
                totals[suffix] += float(line.rsplit(" ", 1)[1])
    return totals["_sum"], totals["_count"]


def run(base, label, requests, concurrency, pick, batched=False) -> dict:
    """Fires the requests; returns the scenario's measurements (see the module docstring)."""

    def one(_):
        debit, credit, amount = pick()
        if batched:  # a payment: reserve on the payer, settled to the merchant later
            path, body = "/v1/holds", {"account_id": debit, "amount": amount, "settle_to_account_id": credit}
        else:
            path, body = "/v1/transfers", {"debit_account_id": debit, "credit_account_id": credit, "amount": amount}
        started = time.perf_counter()
        try:
            status, answer = call(base, "POST", path, body, idempotent=True)
            outcome = "ok" if status == 201 else (answer or {}).get("code", f"HTTP {status}")
        except (http.client.HTTPException, OSError) as error:
            outcome, answer = f"no answer ({type(error).__name__})", None
        latency = (time.perf_counter() - started) * 1000
        return outcome, latency, amount if outcome == "ok" else 0, answer["id"] if outcome == "ok" else None

    waits_before = lock_waits(base)
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        results = list(pool.map(one, range(requests)))
    elapsed = time.perf_counter() - started
    latencies = sorted(ms for _, ms, _, _ in results)
    pct = lambda p: latencies[min(len(latencies) - 1, int(p / 100 * len(latencies)))]  # noqa: E731
    outcomes = Counter(o for o, _, _, _ in results)
    accepted = outcomes["ok"]
    result = {
        "scenario": label,
        "requests": requests,
        "concurrency": concurrency,
        "seconds": round(elapsed, 3),
        "accepted": accepted,
        "accepted_per_second": round(accepted / elapsed, 1),
        "latency_ms": {
            "p50": round(statistics.median(latencies), 1),
            "p95": round(pct(95), 1),
            "p99": round(pct(99), 1),
        },
        "outcomes": dict(sorted(outcomes.items())),
        "paid": sum(paid for _, _, paid, _ in results),
        "hold_ids": [hold_id for _, _, _, hold_id in results if hold_id] if batched else [],
        "_started": started,
    }
    what = "payments accepted (reserved, not yet settled)" if batched else "transfers completed"
    print(f"\n[{label}] {requests} requests, concurrency {concurrency}")
    print(f"  accepted    {accepted} {what} in {elapsed:.2f} s: {accepted / elapsed:,.0f} /s")
    print(
        f"  latency     p50 {result['latency_ms']['p50']} ms   p95 {result['latency_ms']['p95']} ms"
        f"   p99 {result['latency_ms']['p99']} ms   (request sent -> response read)"
    )
    print("  outcomes    " + ", ".join(f"{k}={v}" for k, v in sorted(outcomes.items())))
    if not batched:
        result["completed"] = accepted
        result["completed_per_second"] = result["accepted_per_second"]
        record_lock_waits(base, result, waits_before)
    return result


def record_lock_waits(base, result, before) -> None:
    after = lock_waits(base)
    count = after[1] - before[1]
    result["lock_wait_mean_ms"] = round((after[0] - before[0]) / count * 1000, 1) if count else None
    print(f"  lock wait   mean {result['lock_wait_mean_ms']} ms over {count:.0f} lock acquisitions (server /metrics)")


def accounts_held(base) -> int:
    held, cursor = 0, None
    while True:
        path = "/v1/accounts?limit=200" + (f"&cursor={cursor}" if cursor else "")
        page = call(base, "GET", path)[1]
        held += sum(account["held"] for account in page["data"])
        if not (cursor := page.get("next_cursor")):
            return held


def wait_for_settlement(base, result, wallets, waits_before) -> bool:
    """Waits until no payment is still reserved, then reads back what became of each one."""
    accepted_until = time.perf_counter()
    while accounts_held(base) > 0:
        if time.perf_counter() - accepted_until > SETTLEMENT_TIMEOUT_SECONDS:
            print(f"  settlement  NOT finished within {SETTLEMENT_TIMEOUT_SECONDS} s")
            return False
        time.sleep(0.05)
    settled_at = time.perf_counter()
    record_lock_waits(base, result, waits_before)  # settlement batches lock accounts too

    ours, statuses = set(result["hold_ids"]), Counter()
    for wallet in wallets:
        cursor = None
        while True:
            path = f"/v1/accounts/{wallet}/holds?limit=200" + (f"&cursor={cursor}" if cursor else "")
            page = call(base, "GET", path)[1]
            statuses.update(hold["status"] for hold in page["data"] if hold["id"] in ours)
            if not (cursor := page.get("next_cursor")):
                break
    end_to_end = settled_at - result["_started"]
    result["holds"] = {s: statuses[s] for s in ("captured", "voided", "expired", "active")}
    result["not_accepted"] = result["requests"] - result["accepted"]
    result["completed"] = statuses["captured"]
    result["end_to_end_seconds"] = round(end_to_end, 3)
    result["completed_per_second"] = round(statuses["captured"] / end_to_end, 1)
    result["settled_after_last_accepted_seconds"] = round(settled_at - accepted_until, 3)
    print(
        "  settlement  "
        + " ".join(f"{s}={n}" for s, n in result["holds"].items())
        + f"   (not accepted: {result['not_accepted']})"
    )
    print(
        f"  completed   {statuses['captured']} payments credited {end_to_end:.2f} s after the first request: "
        f"{result['completed_per_second']:,.0f} /s end to end"
    )
    lag = result["settled_after_last_accepted_seconds"]
    print(f"              the last settled {lag:.2f} s after the last was accepted")
    return statuses["captured"] == result["accepted"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--wallets", type=int, default=200)
    parser.add_argument("--requests", type=int, default=5000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--api-key", default=os.environ.get("TALLY_API_KEY"), help="default: $TALLY_API_KEY")
    parser.add_argument("--json", help="also write the raw measurements to this file")
    parser.add_argument("--note", default="", help="recorded in --json: the server's setup, e.g. workers")
    args = parser.parse_args()
    if not args.api_key:
        parser.error("an API key is required: --api-key, or TALLY_API_KEY (create one with `tally create-client`)")
    global API_KEY
    API_KEY = args.api_key
    base = urlparse(args.base_url)
    deposit = 10_000

    wallets, merchant = setup(base, args.wallets, deposit)
    print(f"{args.wallets} wallets funded with {deposit} each")

    def spread():
        debit, credit = random.sample(wallets, 2)
        return debit, credit, random.randint(1, 500)

    def hotspot():
        return random.choice(wallets), merchant, random.randint(1, 50)

    scenarios = [
        run(base, "spread", args.requests, args.concurrency, spread),
        run(base, "hotspot", args.requests, args.concurrency, hotspot),
    ]
    waits_before = lock_waits(base)
    batched = run(base, "hotspot, batched", args.requests, args.concurrency, hotspot, batched=True)
    scenarios.append(batched)
    unexpected = Counter()
    for result in scenarios:
        unexpected.update({k: v for k, v in result["outcomes"].items() if k not in EXPECTED})
    if not wait_for_settlement(base, batched, wallets, waits_before):
        unexpected["payments not captured"] += 1

    _, report = call(base, "GET", "/v1/reconciliation")
    held = sum(call(base, "GET", f"/v1/accounts/{a}")[1]["balance"] for a in [*wallets, merchant])
    conserved = held == deposit * len(wallets)
    print(f"\nreconciliation ok: {report['ok']}   money conserved: {conserved} ({held} == {deposit * len(wallets)})")
    # Conservation alone proves little: if every transfer failed, no money would move either.
    print(f"unexpected outcomes: {dict(unexpected) or 'none'}")
    if args.json:
        for result in scenarios:
            result.pop("_started")
            result.pop("hold_ids")
        raw = {
            "when": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "note": args.note,
            "base_url": args.base_url,
            "wallets": args.wallets,
            "deposit": deposit,
            "client_python": sys.version.split()[0],
            "scenarios": scenarios,
            "reconciliation_ok": report["ok"],
            "money_conserved": conserved,
            "unexpected": dict(unexpected),
        }
        with open(args.json, "w") as out:
            json.dump(raw, out, indent=2)
    sys.exit(0 if report["ok"] and conserved and not unexpected else 1)


if __name__ == "__main__":
    main()
