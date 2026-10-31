"""Request and response models. These also drive the OpenAPI docs at /docs."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated, Any, Generic, Literal, TypeVar
from uuid import UUID

from pydantic import AfterValidator, BaseModel, Field

T = TypeVar("T")

# The database's CHECK allows 4096 bytes of jsonb text; staying a little under it means
# small formatting differences (jsonb renders numbers its own way) can't push a value
# that passed here over the database limit.
MAX_METADATA_BYTES = 4000


def _small_enough(metadata: dict[str, Any]) -> dict[str, Any]:
    # Measured like jsonb::text: ", " and ": " separators, UTF-8 bytes.
    size = len(json.dumps(metadata, ensure_ascii=False).encode())
    if size > MAX_METADATA_BYTES:
        raise ValueError(f"metadata is {size} bytes as JSON; the limit is {MAX_METADATA_BYTES}")
    return metadata


Metadata = Annotated[dict[str, Any], AfterValidator(_small_enough)]

# Money is always an integer number of minor units (cents for USD), never a float.
Amount = Annotated[int, Field(gt=0, le=10**15, description="Minor units, e.g. 1050 = $10.50")]
AccountId = Annotated[int, Field(gt=0)]
Description = Annotated[str, Field(max_length=500)]
Direction = Literal["debit", "credit"]


class AccountIn(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=200)]
    type: Literal["asset", "liability", "equity", "revenue", "expense"]
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$", examples=["USD"])]
    allow_negative: bool = Field(False, description="Whether the balance may go below zero")
    metadata: Metadata = Field(default_factory=dict)


class AccountOut(BaseModel):
    id: int
    name: str
    type: str
    normal_side: Direction
    currency: str
    allow_negative: bool
    balance: int
    held: int
    available: int
    metadata: dict[str, Any]
    created_at: datetime


class EntryIn(BaseModel):
    account_id: AccountId
    direction: Direction
    amount: Amount


class EntryOut(BaseModel):
    id: int
    account_id: int
    direction: Direction
    amount: int
    currency: str
    balance_after: int
    posted_at: datetime


class TransactionIn(BaseModel):
    entries: Annotated[list[EntryIn], Field(min_length=2, max_length=100)]
    description: Description = ""
    metadata: Metadata = Field(default_factory=dict)


class TransferIn(BaseModel):
    debit_account_id: AccountId
    credit_account_id: AccountId
    amount: Amount
    description: Description = ""
    metadata: Metadata = Field(default_factory=dict)


class TransactionOut(BaseModel):
    id: UUID
    description: str
    metadata: dict[str, Any]
    reverses_id: UUID | None
    posted_at: datetime
    entries: list[EntryOut]
    reversed_by: UUID | None = None


class ReversalIn(BaseModel):
    description: Description = ""


DEFAULT_HOLD_SECONDS = 7 * 24 * 3600


class HoldIn(BaseModel):
    account_id: AccountId
    amount: Amount
    expires_in_seconds: Annotated[int, Field(ge=1, le=30 * 24 * 3600)] | None = Field(
        None,
        description=f"Ordinary holds only (default {DEFAULT_HOLD_SECONDS}, 7 days). A payment has no expiry: "
        "sending this with settle_to_account_id is a 422 PAYMENTS_DO_NOT_EXPIRE",
    )
    description: Description = ""
    settle_to_account_id: AccountId | None = Field(
        None,
        description="Makes the hold a payment to this account, settled with others in the next batch: "
        "for paying hot accounts (merchants) without queueing on their row lock. A payment waits for "
        "settlement however long it takes and ends captured, or voided (by its owner, or by settlement "
        "with void_reason); it can't be captured by hand (409 PAYMENT_SETTLES_AUTOMATICALLY)",
    )


class CaptureIn(BaseModel):
    destination_account_id: AccountId
    amount: Amount | None = Field(None, description="Defaults to the full hold; the rest is released")
    description: Description = ""


class HoldOut(BaseModel):
    id: UUID
    account_id: int
    amount: int
    status: Literal["active", "captured", "voided", "expired"]
    description: str
    expires_at: datetime | None = Field(description="null for payments: they don't expire")
    captured_transaction_id: UUID | None
    settle_to_account_id: int | None
    void_reason: str | None = Field(description="Why settlement voided a payment; null otherwise")
    created_at: datetime
    updated_at: datetime


class CaptureOut(BaseModel):
    hold: HoldOut
    transaction: TransactionOut


class StatementLine(BaseModel):
    id: int
    transaction_id: UUID
    direction: Direction
    amount: int
    currency: str
    balance_after: int
    posted_at: datetime
    description: str


class BalanceOut(BaseModel):
    account_id: int
    as_of: datetime
    balance: int


class EventOut(BaseModel):
    id: int
    type: str = Field(examples=["transaction.posted", "hold.captured", "account.created"])
    created_at: datetime
    data: dict[str, Any] = Field(description="The transaction (with its entries), hold or account, as it is now")


class Page(BaseModel, Generic[T]):
    data: list[T]
    next_cursor: str | None = Field(
        None, description="Opaque; pass it back as ?cursor= to get the next page. Null on the last page."
    )


class Problem(BaseModel):
    """An error, as RFC 9457 problem details. Branch on ``code``; ``detail`` is meant for people."""

    type: str = "about:blank"
    title: str
    status: int
    detail: str
    code: str = Field(examples=["INSUFFICIENT_FUNDS"])
    errors: list[dict[str, Any]] | None = Field(None, description="Per-field problems, for VALIDATION_ERROR")
