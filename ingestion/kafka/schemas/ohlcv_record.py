"""
Pydantic schema for an OHLCV (candlestick) record published to
crypto.ohlcv.streaming or crypto.ohlcv.historical.

The same model is used for both streaming-derived bars (produced by
trade_stream.py) and REST-sourced historical klines (produced by
binance_historical.py). The `source` field distinguishes them.

Design decision — source of truth: int_ohlcv_unified (dbt) deduplicates
on (symbol, interval, open_time) and prefers OHLCVSource.HISTORICAL over
OHLCVSource.STREAMING because REST klines are computed server-side with
no windowing artefacts. Streaming bars can be incomplete if the Spark
watermark evicts late-arriving trades.

Pipeline position:
  trade_stream.py / binance_historical.py
    → kafka.schemas
      → crypto.ohlcv.streaming / crypto.ohlcv.historical
        → dbt staging / intermediate / marts
"""

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator

_DECIMAL_FIELDS = (
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_volume",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
)


class Interval(str, Enum):
    ONE_MIN = "1m"
    FIVE_MIN = "5m"
    ONE_HOUR = "1h"
    FOUR_HOUR = "4h"
    ONE_DAY = "1d"


class OHLCVSource(str, Enum):
    STREAMING = "streaming"
    HISTORICAL = "historical"


class OHLCVRecord(BaseModel):
    """A single OHLCV bar keyed on (symbol, interval, open_time)."""

    symbol: str
    interval: Interval
    open_time: datetime
    close_time: datetime
    # Decimal instead of float — see TradeEvent for the full rationale.
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal         # base-asset volume
    quote_volume: Decimal
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    trade_count: int
    is_closed: bool
    # Distinguishes REST-sourced (historical) from streaming-derived bars.
    # int_ohlcv_unified uses this to prefer historical when both exist for the
    # same grain — see module docstring for the rationale.
    source: OHLCVSource
    trade_timestamp: datetime   # = open_time; carried separately for Spark event-time
    ingestion_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    model_config = {"frozen": True}

    @field_validator(*_DECIMAL_FIELDS, mode="before")
    @classmethod
    def reject_float(cls, v: object) -> object:
        if isinstance(v, float):
            raise ValueError(
                f"Pass price/volume fields as str or Decimal, not float (got {v!r}). "
                "Float-to-Decimal conversion loses precision."
            )
        return v

    @model_validator(mode="after")
    def high_gte_low(self) -> "OHLCVRecord":
        # Enforce the OHLCV invariant at ingestion time. The same check is applied
        # downstream in the assert_ohlcv_high_gte_low dbt singular test, but catching
        # it here prevents malformed bars from ever entering PostgreSQL.
        if self.high_price < self.low_price:
            raise ValueError(
                f"high_price ({self.high_price}) must be >= low_price ({self.low_price})"
            )
        return self
