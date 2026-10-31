"""HTTP API.

Every /v1 route needs an API key (``Authorization: Bearer <key>``). The key decides
whose books the request sees: another client's accounts, transactions and holds look
exactly like ones that don't exist. Every change is recorded in the audit log, in the
same database transaction as the change itself (by triggers, migration 006).

Money-moving endpoints require an ``Idempotency-Key`` header; creating accounts and
voiding holds accept one. Repeating a request with the same key returns the original
response (with ``Idempotent-Replayed: true``) instead of doing the work again.

Every error is an RFC 9457 problem document with a stable ``code``. Errors that a
retry can fix (a busy row, a slow query, an exhausted pool, an unreachable database)
are 503 with ``Retry-After``; anything unexpected is a 500 that reveals nothing
about the internals.
"""

from __future__ import annotations

import base64
import json
import logging
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID

import psycopg
import psycopg_pool
from fastapi import APIRouter, Depends, FastAPI, Header, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST
from pydantic import AwareDatetime
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__, auth, events, idempotency
from .config import Settings
from .db import Database, migrate
from .errors import LedgerError, unprocessable
from .idempotency import Outcome, run_idempotent
from .ledger import EXPIRY_BATCH_SIZE, SETTLE_BATCH_SIZE, Ledger, Leg
from .observability import PROBLEMS, RequestContext, configure_logging, metrics_text, request_id
from .reconciliation import reconcile
from .schemas import (
    DEFAULT_HOLD_SECONDS,
    AccountIn,
    AccountOut,
    BalanceOut,
    CaptureIn,
    CaptureOut,
    EventOut,
    HoldIn,
    HoldOut,
    Page,
    Problem,
    ReversalIn,
    StatementLine,
    TransactionIn,
    TransactionOut,
    TransferIn,
)

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

# The console loads nothing from anywhere else and never needs inline script. If a
# value ever slipped past its HTML escaping, the browser would still refuse to run it.
CONSOLE_CSP = (
    "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
    "connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
)

# The largest legitimate request (100 entries plus 4 KB of metadata) is well under this.
MAX_BODY_BYTES = 64 * 1024

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Unique per logical request; retries reuse it",
    ),
]
OptionalIdempotencyKey = Annotated[
    str | None,
    Header(alias="Idempotency-Key", min_length=1, max_length=255, description="Optional: makes retries safe"),
]

# Module level, not inside create_app: with postponed annotations FastAPI resolves
# these names from the module's globals.
Limit = Annotated[int, Query(ge=1, le=200)]
Cursor = Annotated[str | None, Query(max_length=200, description="next_cursor from the previous page")]

bearer = HTTPBearer(auto_error=False, description="An API key from `tally create-client` or `tally create-key`")


def require_key(request: Request, credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]) -> None:
    """Takes the key from the request. It is checked inside the request's own database
    transaction (see ``run`` in create_app): a separate lookup would cost every request
    another pool checkout and transaction, measured at about a fifth of throughput."""
    if credentials is None:
        raise LedgerError(401, "UNAUTHENTICATED", "Send an API key: Authorization: Bearer <key>")
    request.state.api_key = credentials.credentials


# Failures a retry can fix, as (code, message, seconds to wait). Matched by exception
# class, most specific first, so a lock timeout isn't reported as "database down".
TRANSIENT_ERRORS: dict[type[Exception], tuple[str, str, int]] = {
    psycopg.errors.LockNotAvailable: ("LOCK_TIMEOUT", "An account involved is busy; retry shortly", 1),
    psycopg.errors.QueryCanceled: ("QUERY_TIMEOUT", "The database took too long to answer; retry shortly", 5),
    psycopg_pool.PoolTimeout: ("SERVER_BUSY", "No database connection became free in time; retry shortly", 2),
    psycopg.OperationalError: ("DATABASE_UNAVAILABLE", "The database can't be reached right now; retry shortly", 5),
}


def problem(status: int, code: str, detail: str, headers: dict | None = None, **extra) -> JSONResponse:
    """RFC 9457 problem details, with a stable ``code`` clients can switch on."""
    PROBLEMS.labels(code).inc()
    body = {"type": "about:blank", "title": code, "status": status, "detail": detail, "code": code, **extra}
    if rid := request_id.get():
        body["request_id"] = rid  # what a client quotes when reporting the failure
    return JSONResponse(body, status_code=status, media_type="application/problem+json", headers=headers)


def respond(outcome: Outcome, location: Callable[[dict], str] | None = None) -> JSONResponse:
    headers = {"Idempotent-Replayed": str(outcome.replayed).lower()}
    if location and outcome.status_code == 201:
        headers["Location"] = location(outcome.body)
    return JSONResponse(outcome.body, status_code=outcome.status_code, headers=headers)


def encode_cursor(*values: Any) -> str:
    """An opaque page token. Clients pass it back unchanged, so its contents can change freely."""
    return base64.urlsafe_b64encode(json.dumps(jsonable_encoder(values)).encode()).decode().rstrip("=")


def decode_cursor(cursor: str | None, *types: Callable[[Any], Any]) -> tuple | None:
    if cursor is None:
        return None
    try:
        values = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        if len(values) != len(types):
            raise ValueError("wrong shape")
        return tuple(kind(value) for kind, value in zip(types, values, strict=True))
    except (ValueError, TypeError) as exc:
        raise unprocessable("INVALID_CURSOR", "cursor is not one this API issued") from exc


class RevalidatedStaticFiles(StaticFiles):
    """Static files that the browser revalidates before each use: a cheap 304 when unchanged.

    There is no build step, so file names carry no content hash. Without this, a
    browser could keep a cached app.js after a deploy and run it against the new
    index.html (or the other way round), which breaks the page in confusing ways.
    """

    def file_response(self, *args, **kwargs):
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "no-cache"
        return response


class BodyTooLarge(StarletteHTTPException):
    # An HTTPException on purpose: FastAPI's body parser turns any other exception
    # raised while reading the body into a generic 400, but re-raises these.
    def __init__(self, max_bytes: int) -> None:
        super().__init__(413, f"Request bodies are limited to {max_bytes} bytes")


class BodySizeLimit:
    """ASGI middleware that refuses request bodies over ``max_bytes`` with 413.

    Checks the declared Content-Length first, so an honest oversized request is
    refused before any of it is read. It also counts the bytes actually received,
    so a chunked request without a length can't get around it.
    """

    def __init__(self, app, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        declared = dict(scope["headers"]).get(b"content-length")
        if declared and declared.isdigit() and int(declared) > self.max_bytes:
            await http_problem(None, BodyTooLarge(self.max_bytes))(scope, receive, send)
            return
        received = 0

        async def counted_receive():
            nonlocal received
            message = await receive()
            received += len(message.get("body", b""))
            if received > self.max_bytes:
                # Raised inside the app, whose HTTPException handler renders the 413.
                raise BodyTooLarge(self.max_bytes)
            return message

        await self.app(scope, counted_receive, send)


# Codes for the errors the framework raises itself. Spelled out rather than taken from
# HTTPStatus(...).name: Python 3.13 renamed several (413 was REQUEST_ENTITY_TOO_LARGE in
# 3.11), and a client's `code` mustn't change with the server's Python version.
FRAMEWORK_CODES = {
    400: "BAD_REQUEST",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    413: "CONTENT_TOO_LARGE",
    416: "RANGE_NOT_SATISFIABLE",
}


def http_problem(request: Request | None, exc: StarletteHTTPException) -> JSONResponse:
    """Framework errors (unknown route, wrong method, body too large) as problem documents too."""
    code = FRAMEWORK_CODES.get(exc.status_code, f"HTTP_{exc.status_code}")
    return problem(exc.status_code, code, str(exc.detail), headers=exc.headers)


def run_maintenance(db: Database, ledger: Ledger) -> None:
    """One maintenance pass. Each task fails on its own: a settlement that can't reach the
    database, or a bug in one job, still lets holds expire and old keys be purged."""
    # Payments first: they're what someone is waiting for.
    tasks = [
        ("settle_payments", lambda: db.run(ledger.settle_holds), SETTLE_BATCH_SIZE),
        ("expire_holds", lambda: db.run(ledger.expire_holds), EXPIRY_BATCH_SIZE),
        ("purge_idempotency_keys", lambda: db.run(idempotency.purge_expired), idempotency.PURGE_BATCH_SIZE),
        ("purge_events", lambda: db.run(events.purge_expired), events.PURGE_BATCH_SIZE),
    ]
    for name, task, batch_size in tasks:
        try:
            # A full batch means there may be more: keep going until one comes back short.
            while task() == batch_size:
                pass
        except Exception:  # keep the other tasks and the loop alive; the next tick retries
            log.exception("maintenance task failed: %s", name)


def _maintenance_loop(db: Database, ledger: Ledger, interval: float, stop: threading.Event) -> None:
    while not stop.wait(interval):
        run_maintenance(db, ledger)


def serve_app() -> FastAPI:
    """The app as `tally serve` runs it: each worker process logs JSON lines, then builds the app."""
    configure_logging()
    return create_app()


def create_app(settings: Settings | None = None, *, run_migrations: bool = True, maintenance: bool = True) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if run_migrations:
            migrate(settings.database_url)
        db = Database(
            settings.database_url,
            max_size=settings.pool_size,
            lock_timeout_ms=settings.lock_timeout_ms,
            statement_timeout_ms=settings.statement_timeout_ms,
        )
        app.state.db = db
        stop = threading.Event()
        if maintenance:
            threading.Thread(
                target=_maintenance_loop,
                # No client: settlement and expiry sweep every client's holds.
                args=(db, Ledger(), settings.maintenance_interval_seconds, stop),
                daemon=True,
                name="tally-maintenance",
            ).start()
        yield
        stop.set()
        db.close()

    app = FastAPI(
        title="Tally",
        version=__version__,
        summary="A double-entry ledger API where the database guarantees the books always balance.",
        lifespan=lifespan,
        # Every operation documents the error shape in /docs.
        responses={"default": {"model": Problem, "description": "Error, as RFC 9457 problem details"}},
    )
    app.add_middleware(BodySizeLimit, max_bytes=MAX_BODY_BYTES)
    app.add_middleware(RequestContext)  # outermost: even a refused body gets a request ID

    app.add_exception_handler(StarletteHTTPException, http_problem)

    @app.exception_handler(LedgerError)
    async def ledger_error(request: Request, exc: LedgerError) -> JSONResponse:
        headers = {"WWW-Authenticate": "Bearer"} if exc.status == 401 else None
        return problem(exc.status, exc.code, exc.message, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()]
        return problem(422, "VALIDATION_ERROR", "The request is invalid", errors=errors)

    def transient_error(code: str, detail: str, retry_after: int):
        async def handle(request: Request, exc: Exception) -> JSONResponse:
            log.warning("%s on %s %s: %s", code, request.method, request.url.path, exc)
            return problem(503, code, detail, headers={"Retry-After": str(retry_after)})

        return handle

    for error_type, (code, detail, retry_after) in TRANSIENT_ERRORS.items():
        app.add_exception_handler(error_type, transient_error(code, detail, retry_after))

    @app.exception_handler(psycopg.errors.IntegrityError)
    async def integrity_error(request: Request, exc: psycopg.errors.IntegrityError) -> JSONResponse:
        # Should be unreachable: the service validates first. If it happens, the database
        # caught a bug before it could corrupt the books. The constraint's name and the
        # failing row go to the log, not to the client.
        log.error("database constraint rejected a write: %s", exc)
        return problem(409, "CONSTRAINT_VIOLATION", "The request conflicts with a ledger rule")

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error on %s %s", request.method, request.url.path)
        return problem(500, "INTERNAL_ERROR", "Something went wrong on our side")

    def db(request: Request) -> Database:
        return request.app.state.db

    def ledger(request: Request) -> Ledger:
        return Ledger(request.state.actor.client_id)

    def scoped(request: Request, operation: str) -> str:
        # Idempotency keys are per client: two clients may pick the same key.
        return f"{request.state.actor.client_id}:{operation}"

    def run(request: Request, work):
        """Runs ``work(conn)`` in one transaction, after checking the request's key in that same transaction."""

        def authenticated(conn):
            actor = auth.find_key(conn, request.state.api_key, request_id.get())
            if actor is None:
                raise LedgerError(401, "UNAUTHENTICATED", "Unknown or revoked API key")
            request.state.actor = actor
            return work(conn)

        return db(request).run(authenticated)

    v1 = APIRouter(prefix="/v1", dependencies=[Depends(require_key)])

    def page(rows: list[dict], limit: int, key: Callable[[dict], tuple]) -> dict:
        return {"data": rows, "next_cursor": encode_cursor(*key(rows[-1])) if len(rows) == limit else None}

    # --- accounts -----------------------------------------------------------------

    @v1.post("/accounts", status_code=201, response_model=AccountOut, tags=["accounts"])
    def create_account(body: AccountIn, request: Request, key: OptionalIdempotencyKey = None):
        def create(conn):
            return ledger(request).create_account(conn, **body.model_dump())

        def work(conn):
            if key is None:
                return Outcome(201, jsonable_encoder(create(conn)), replayed=False)
            return run_idempotent(conn, scoped(request, "accounts"), key, body, lambda: create(conn))

        outcome = run(request, work)
        return respond(outcome, location=lambda account: f"/v1/accounts/{account['id']}")

    @v1.get("/accounts", response_model=Page[AccountOut], tags=["accounts"])
    def list_accounts(request: Request, limit: Limit = 50, cursor: Cursor = None):
        after = decode_cursor(cursor, int)
        rows = run(request, lambda conn: ledger(request).list_accounts(conn, limit, after[0] if after else None))
        return page(rows, limit, key=lambda account: (account["id"],))

    @v1.get("/accounts/{account_id}", response_model=AccountOut, tags=["accounts"])
    def get_account(account_id: int, request: Request):
        return run(request, lambda conn: ledger(request).get_account(conn, account_id))

    @v1.get("/accounts/{account_id}/entries", response_model=Page[StatementLine], tags=["accounts"])
    def statement(account_id: int, request: Request, limit: Limit = 50, cursor: Cursor = None):
        before = decode_cursor(cursor, int)
        rows = run(
            request, lambda conn: ledger(request).statement(conn, account_id, limit, before[0] if before else None)
        )
        return page(rows, limit, key=lambda entry: (entry["id"],))

    @v1.get("/accounts/{account_id}/balance", response_model=BalanceOut, tags=["accounts"])
    def balance_at(
        account_id: int,
        request: Request,
        # Aware only: a time without a zone would be read in the database session's
        # time zone and silently answer for a different moment.
        as_of: Annotated[AwareDatetime | None, Query(description="ISO 8601 with a time zone; defaults to now")] = None,
    ):
        moment = as_of or datetime.now(UTC)
        balance = run(request, lambda conn: ledger(request).balance_at(conn, account_id, moment))
        return {"account_id": account_id, "as_of": moment, "balance": balance}

    @v1.get("/accounts/{account_id}/holds", response_model=Page[HoldOut], tags=["holds"])
    def account_holds(
        account_id: int,
        request: Request,
        status: Literal["active", "captured", "voided", "expired"] | None = None,
        limit: Limit = 50,
        cursor: Cursor = None,
    ):
        before = decode_cursor(cursor, datetime.fromisoformat, UUID)
        rows = run(request, lambda conn: ledger(request).list_holds(conn, account_id, status, limit, before))
        return page(rows, limit, key=lambda hold: (hold["created_at"], hold["id"]))

    # --- transactions -------------------------------------------------------------

    def transaction_location(transaction: dict) -> str:
        return f"/v1/transactions/{transaction['id']}"

    @v1.post("/transactions", status_code=201, response_model=TransactionOut, tags=["transactions"])
    def post_transaction(body: TransactionIn, key: IdempotencyKey, request: Request):
        legs = [Leg(e.account_id, e.direction, e.amount) for e in body.entries]

        def work(conn):
            return run_idempotent(
                conn,
                scoped(request, "transactions"),
                key,
                body,
                lambda: ledger(request).post(conn, legs, body.description, body.metadata),
            )

        return respond(run(request, work), transaction_location)

    @v1.post("/transfers", status_code=201, response_model=TransactionOut, tags=["transactions"])
    def transfer(body: TransferIn, key: IdempotencyKey, request: Request):
        def move(conn):
            return ledger(request).transfer(
                conn, body.debit_account_id, body.credit_account_id, body.amount, body.description, body.metadata
            )

        def work(conn):
            return run_idempotent(conn, scoped(request, "transfers"), key, body, lambda: move(conn))

        return respond(run(request, work), transaction_location)

    @v1.get("/transactions", response_model=Page[TransactionOut], tags=["transactions"])
    def recent_transactions(request: Request, limit: Annotated[int, Query(ge=1, le=100)] = 20, cursor: Cursor = None):
        """Most recent transactions first."""
        before = decode_cursor(cursor, datetime.fromisoformat, UUID)
        rows = run(request, lambda conn: ledger(request).recent_transactions(conn, limit, before))
        return page(rows, limit, key=lambda transaction: (transaction["posted_at"], transaction["id"]))

    @v1.get("/transactions/{transaction_id}", response_model=TransactionOut, tags=["transactions"])
    def get_transaction(transaction_id: UUID, request: Request):
        return run(request, lambda conn: ledger(request).get_transaction(conn, transaction_id))

    @v1.post(
        "/transactions/{transaction_id}/reversal",
        status_code=201,
        response_model=TransactionOut,
        tags=["transactions"],
    )
    def reverse(transaction_id: UUID, body: ReversalIn, key: IdempotencyKey, request: Request):
        payload = {"transaction_id": transaction_id, **body.model_dump()}

        def work(conn):
            return run_idempotent(
                conn,
                scoped(request, "reversals"),
                key,
                payload,
                lambda: ledger(request).reverse(conn, transaction_id, body.description),
            )

        return respond(run(request, work), transaction_location)

    # --- holds --------------------------------------------------------------------

    @v1.post("/holds", status_code=201, response_model=HoldOut, tags=["holds"])
    def create_hold(body: HoldIn, key: IdempotencyKey, request: Request):
        def create(conn):
            seconds = body.expires_in_seconds
            if seconds is None and body.settle_to_account_id is None:
                seconds = DEFAULT_HOLD_SECONDS
            # A payment has no expiry; if one was sent anyway, the ledger refuses it.
            expires_at = None if seconds is None else datetime.now(UTC) + timedelta(seconds=seconds)
            return ledger(request).create_hold(
                conn, body.account_id, body.amount, expires_at, body.description, body.settle_to_account_id
            )

        def work(conn):
            return run_idempotent(conn, scoped(request, "holds"), key, body, lambda: create(conn))

        return respond(run(request, work), location=lambda hold: f"/v1/holds/{hold['id']}")

    @v1.get("/holds/{hold_id}", response_model=HoldOut, tags=["holds"])
    def get_hold(hold_id: UUID, request: Request):
        return run(request, lambda conn: ledger(request).get_hold(conn, hold_id))

    @v1.post("/holds/{hold_id}/capture", response_model=CaptureOut, tags=["holds"])
    def capture(hold_id: UUID, body: CaptureIn, key: IdempotencyKey, request: Request):
        payload = {"hold_id": hold_id, **body.model_dump()}

        def settle(conn):
            return ledger(request).capture_hold(
                conn, hold_id, body.destination_account_id, body.amount, body.description
            )

        def work(conn):
            return run_idempotent(conn, scoped(request, "captures"), key, payload, lambda: settle(conn), 200)

        return respond(run(request, work))

    @v1.post("/holds/{hold_id}/void", response_model=HoldOut, tags=["holds"])
    def void(hold_id: UUID, request: Request, key: OptionalIdempotencyKey = None):
        # Without a key, a retried void whose first attempt succeeded gets HOLD_NOT_ACTIVE.
        def work(conn):
            if key is None:
                return Outcome(200, jsonable_encoder(ledger(request).void_hold(conn, hold_id)), replayed=False)
            return run_idempotent(
                conn,
                scoped(request, "voids"),
                key,
                {"hold_id": hold_id},
                lambda: ledger(request).void_hold(conn, hold_id),
                200,
            )

        return respond(run(request, work))

    # --- events -------------------------------------------------------------------

    @v1.get("/events", response_model=Page[EventOut], tags=["events"])
    def list_events(request: Request, limit: Limit = 100, cursor: Cursor = None):
        """Every change to the caller's books, oldest first, in an order that never skips one.

        Poll with ``next_cursor``. Unlike the other lists it never runs out: a short or
        empty page means you're caught up, so keep the cursor and ask again later.
        Events are kept for 7 days.
        """
        after = decode_cursor(cursor, int, int)
        position = (str(after[0]), after[1]) if after else None
        rows = run(request, lambda conn: events.read(conn, request.state.actor.client_id, position, limit))
        data = [{"id": r["id"], "type": r["type"], "created_at": r["created_at"], "data": r["payload"]} for r in rows]
        return {"data": data, "next_cursor": encode_cursor(int(rows[-1]["txid"]), rows[-1]["id"]) if rows else cursor}

    # --- operations ---------------------------------------------------------------

    @v1.get("/reconciliation", tags=["operations"])
    def reconciliation(request: Request):
        """Re-derives the caller's books from raw entries and holds and reports any disagreement.

        Synchronous: fine for small and medium ledgers. For big ones run ``tally
        reconcile`` (which checks every client's books) on a schedule instead.
        """
        # Two transactions: reconcile() must start its own (REPEATABLE READ has to be its first statement).
        run(request, lambda conn: None)
        client_id = request.state.actor.client_id
        return jsonable_encoder(db(request).run(lambda conn: reconcile(conn, client_id=client_id)))

    app.include_router(v1)

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return Response(metrics_text(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/health", tags=["operations"])
    def health(request: Request):
        db(request).run(lambda conn: conn.execute("SELECT 1"))
        return {"status": "ok"}

    if STATIC_DIR.is_dir():
        app.mount("/static", RevalidatedStaticFiles(directory=STATIC_DIR), name="static")

        @app.get("/", include_in_schema=False)
        def console():
            headers = {"Content-Security-Policy": CONSOLE_CSP, "Cache-Control": "no-cache"}
            return FileResponse(STATIC_DIR / "index.html", headers=headers)

    return app
