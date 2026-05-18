"""
Pydantic schema for a single trade execution event published to crypto.trades.raw.

Each event represents one matched trade on the exchange. This schema is the
canonical contract between the ingestion layer (Phase 2) and all downstream
consumers: Spark Structured Streaming, the schema validator, and tests.

Field mapping from Binance WebSocket stream (normalised by the consumer):
  Binance 't' → trade_id
  Binance 'p' → price
  Binance 'q' → quantity
  Binance 'b' → buyer_order_id
  Binance 'a' → seller_order_id
  Binance 'm' → is_buyer_maker
  Binance 'T' → trade_timestamp

Pipeline position: ingestion/websocket → kafka.schemas → crypto.trades.raw → Spark
"""

from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class TradeEvent(BaseModel):
    """A single normalised trade execution from an exchange."""

    symbol: str

    # Exchange-native trade ID used as the deduplication key in the backfill job.
    # A pipeline-generated UUID would require distributed coordination to guarantee
    # uniqueness; the exchange already guarantees trade_id uniqueness per symbol.
    trade_id: int

    # Decimal instead of float: float arithmetic accumulates rounding errors that
    # compound across OHLCV aggregations (e.g., computing VWAP from thousands of
    # trades). Binance sends prices as JSON strings ("p": "45000.50"), which Pydantic
    # coerces to Decimal exactly. Float inputs are explicitly rejected — see validator.
    price: Decimal
    quantity: Decimal

    buyer_order_id: int
    seller_order_id: int

    # True  → buyer was the passive maker; seller was the aggressor → sell-side trade
    # False → buyer was the aggressor → buy-side trade
    # Used downstream in volume_stream.py to compute buy/sell volume ratio.
    is_buyer_maker: bool

    # trade_timestamp: exchange-side event time (Binance 'T' field in ms, normalised
    # to datetime by the consumer). Used as the event-time clock for Spark watermarks.
    # Must not use ingestion_timestamp for windowing — network/broker lag can make
    # ingestion_timestamp arrive seconds after the actual trade.
    trade_timestamp: datetime

    ingestion_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    model_config = {"frozen": True}

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def reject_float(cls, v: object) -> object:
        # Guard: silently accepting a float would re-introduce the precision loss
        # we are explicitly avoiding. All call sites must pass str or Decimal.
        if isinstance(v, float):
            raise ValueError(
                f"Pass price/quantity as str or Decimal, not float (got {v!r}). "
                "Float-to-Decimal conversion loses precision."
            )
        return v
