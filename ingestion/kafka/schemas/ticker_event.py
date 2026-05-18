"""
Pydantic schema for a 24-hour rolling ticker event published to crypto.tickers.raw.

Captures the 24h statistics snapshot broadcast by the exchange on every trade.
Used downstream in the mart_price_momentum and mart_trade_activity dbt models.

Field mapping from Binance WebSocket 24hrTicker stream (normalised by consumer):
  Binance 's' → symbol
  Binance 'p' → price_change
  Binance 'P' → price_change_percent
  Binance 'w' → weighted_avg_price
  Binance 'o' → open_price
  Binance 'h' → high_price
  Binance 'l' → low_price
  Binance 'c' → last_price
  Binance 'Q' → last_qty
  Binance 'b' → best_bid_price
  Binance 'B' → best_bid_qty
  Binance 'a' → best_ask_price
  Binance 'A' → best_ask_qty
  Binance 'v' → base_volume
  Binance 'q' → quote_volume
  Binance 'n' → trade_count
  Binance 'O' → open_time  (ms epoch)
  Binance 'C' → close_time (ms epoch)
  Binance 'E' → trade_timestamp (event time)

Pipeline position: ingestion/websocket → kafka.schemas → crypto.tickers.raw → dbt
"""

from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator

_DECIMAL_FIELDS = (
    "price_change",
    "price_change_percent",
    "weighted_avg_price",
    "open_price",
    "high_price",
    "low_price",
    "last_price",
    "last_qty",
    "best_bid_price",
    "best_bid_qty",
    "best_ask_price",
    "best_ask_qty",
    "base_volume",
    "quote_volume",
)


class TickerEvent(BaseModel):
    """24-hour rolling statistics snapshot for a single trading symbol."""

    symbol: str
    # See TradeEvent for the Decimal-vs-float rationale; it applies here equally.
    price_change: Decimal
    price_change_percent: Decimal
    weighted_avg_price: Decimal
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    last_price: Decimal
    last_qty: Decimal
    best_bid_price: Decimal
    best_bid_qty: Decimal
    best_ask_price: Decimal
    best_ask_qty: Decimal
    base_volume: Decimal   # total traded base-asset volume in the 24h window
    quote_volume: Decimal  # total traded quote-asset volume in the 24h window
    trade_count: int
    open_time: datetime
    close_time: datetime
    # See TradeEvent for the trade_timestamp / ingestion_timestamp rationale.
    trade_timestamp: datetime
    ingestion_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    model_config = {"frozen": True}

    @field_validator(*_DECIMAL_FIELDS, mode="before")
    @classmethod
    def reject_float(cls, v: object) -> object:
        if isinstance(v, float):
            raise ValueError(
                f"Pass numeric fields as str or Decimal, not float (got {v!r}). "
                "Float-to-Decimal conversion loses precision."
            )
        return v
