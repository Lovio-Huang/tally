"""Request IDs, JSON logs and Prometheus metrics.

Every request gets an ID: the caller's X-Request-ID if it is a sane token, else a
fresh one. It is echoed in the response, included in error bodies and attached to
every log line and audit record written while the request runs, so one ID ties a
client's report to everything the server did about it.

Metrics: under `tally serve --workers N` each worker is a separate process with its
own counters. The CLI points PROMETHEUS_MULTIPROC_DIR at a fresh directory before
the workers start, and /metrics then reports the sum over all of them.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import traceback
import uuid
from contextvars import ContextVar

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram, generate_latest
from prometheus_client.multiprocess import MultiProcessCollector

request_id: ContextVar[str | None] = ContextVar("request_id", default=None)

REQUEST_SECONDS = Histogram(
    "tally_http_request_duration_seconds", "HTTP request latency", ["method", "route", "status"]
)
LOCK_WAIT_SECONDS = Histogram(
    "tally_account_lock_wait_seconds",
    "Time spent acquiring account row locks (SELECT ... FOR NO KEY UPDATE); grows with contention on hot accounts",
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
DB_RETRIES = Counter(
    "tally_db_retries_total", "Transactions retried after a deadlock or serialization failure", ["error"]
)
PROBLEMS = Counter("tally_problems_total", "Error responses, by problem code", ["code"])
SETTLED = Counter("tally_settled_payments_total", "Payments (holds with a destination) settled in batches")
VOIDED = Counter(
    "tally_settlement_voided_payments_total", "Payments settlement refused and voided (see holds.void_reason)"
)

# A caller-supplied ID ends up in logs and headers, so only plain tokens are accepted.
REQUEST_ID_FORMAT = re.compile(r"[A-Za-z0-9._-]{1,64}")
QUIET_PATHS = {"/health", "/metrics"}  # polled every few seconds; metrics only, no access log

access_log = logging.getLogger("tally.access")

# Attributes every LogRecord has; anything else was passed in `extra=` and is logged as a field.
_STANDARD = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """One JSON object per line, with the current request ID and any `extra=` fields."""

    def format(self, record: logging.LogRecord) -> str:
        line = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "request_id": request_id.get(),
            **{k: v for k, v in record.__dict__.items() if k not in _STANDARD},
        }
        if record.exc_info:
            line["exc"] = "".join(traceback.format_exception(*record.exc_info))
        return json.dumps(line, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)


def metrics_text() -> bytes:
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
        return generate_latest(registry)
    return generate_latest(REGISTRY)


class RequestContext:
    """ASGI middleware: request ID, one access log line and a latency sample per request."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        given = dict(scope["headers"]).get(b"x-request-id", b"").decode("latin-1")
        rid = given if REQUEST_ID_FORMAT.fullmatch(given) else uuid.uuid4().hex
        token = request_id.set(rid)
        status = 500
        started = time.perf_counter()

        async def send_with_id(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message["headers"] = [*message.get("headers", []), (b"x-request-id", rid.encode())]
            await send(message)

        try:
            await self.app(scope, receive, send_with_id)
        finally:
            seconds = time.perf_counter() - started
            # The route template, not the raw path: /v1/accounts/{account_id} is one series, not one per id.
            route = getattr(scope.get("route"), "path", "unmatched")
            REQUEST_SECONDS.labels(scope["method"], route, str(status)).observe(seconds)
            if scope["path"] not in QUIET_PATHS:
                actor = scope.get("state", {}).get("actor")  # set by authentication, if it ran
                fields = {"method": scope["method"], "route": route, "status": status, "ms": round(seconds * 1000, 1)}
                if actor:
                    fields |= {"client_id": actor.client_id, "api_key_id": actor.key_id}
                access_log.info("%s %s %s", scope["method"], scope["path"], status, extra=fields)
            request_id.reset(token)
