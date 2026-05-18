"""
Generates synthetic trade and ticker events that match the live Binance schema
exactly. Drop-in replacement for binance_consumer.py — toggled via
DATA_SOURCE=synthetic. Requires no network access.

Prices follow a per-symbol random walk (±0.1 % per tick) seeded around
realistic base prices so downstream Spark aggregations and dbt models see
plausible numbers without connecting to the exchange.

The public API mirrors binance_consumer:  ``await run(producer, symbols)``.
Entrypoint calls both consumers identically; only the import differs based
on DATA_SOURCE.

Pipeline position: synthetic_consumer → CryptoProducer → Kafka (same topics as live)
"""

import asyncio
import logging
import random
from datetime import datetime, timezone
from decimal import Decimal

from ingestion.kafka import topics
from ingestion.kafka.producer import CryptoProducer
from ingestion.kafka.schemas.ticker_event import TickerEvent
from ingestion.kafka.schemas.trade_event import TradeEvent

logger = logging.getLogger(__name__)

# Seed prices approximate realistic USD values for the configured symbols.
_BASE_PRICES: dict[str, Decimal] = {
    "BTCUSDT": Decimal("45000"),
    "ETHUSDT": Decimal("2500"),
    "BNBUSDT": Decimal("350"),
    "SOLUSDT": Decimal("100"),
    "ADAUSDT": Decimal("0.55"),
}
_DEFAULT_PRICE = Decimal("100")
_TICK_VOLATILITY = Decimal("0.001")  # ±0.1 % per tick

_trade_id_counter: dict[str, int] = {}


def _next_trade_id(symbol: str) -> int:
    _trade_id_counter[symbol] = _trade_id_counter.get(symbol, 1_000_000) + 1
    return _trade_id_counter[symbol]


def _random_walk(price: Decimal) -> Decimal:
    """Apply a small random multiplicative step to *price*."""
    # Multiplicative walk keeps prices positive and relative volatility constant
    # regardless of the price level (important for ADAUSDT at ~$0.55 vs BTC at ~$45k).
    factor = Decimal(str(1 + random.uniform(-float(_TICK_VOLATILITY), float(_TICK_VOLATILITY))))
    return (price * factor).quantize(Decimal("0.00000001"))


def _make_trade_event(symbol: str, price: Decimal) -> TradeEvent:
    """Build a synthetic TradeEvent for *symbol* at *price*."""
    quantity = Decimal(str(round(random.uniform(0.0001, 2.0), 6)))
    return TradeEvent(
        symbol=symbol,
        trade_id=_next_trade_id(symbol),
        price=str(price),
        quantity=str(quantity),
        buyer_order_id=random.randint(1, 10_000_000),
        seller_order_id=random.randint(1, 10_000_000),
        is_buyer_maker=random.random() < 0.5,
        trade_timestamp=datetime.now(tz=timezone.utc),
    )


def _make_ticker_event(symbol: str, price: Decimal) -> TickerEvent:
    """Build a synthetic 24hrTicker TickerEvent for *symbol* at *price*."""
    spread = price * Decimal("0.0001")
    price_change = price * Decimal(str(round(random.uniform(-0.02, 0.02), 6)))
    open_price = price - price_change

    now = datetime.now(tz=timezone.utc)
    open_time = now.replace(hour=0, minute=0, second=0, microsecond=0)

    return TickerEvent(
        symbol=symbol,
        price_change=str(price_change.quantize(Decimal("0.00000001"))),
        price_change_percent=str((price_change / open_price * 100).quantize(Decimal("0.00001"))),
        weighted_avg_price=str(price.quantize(Decimal("0.00000001"))),
        open_price=str(open_price.quantize(Decimal("0.00000001"))),
        high_price=str((price * Decimal("1.005")).quantize(Decimal("0.00000001"))),
        low_price=str((price * Decimal("0.995")).quantize(Decimal("0.00000001"))),
        last_price=str(price.quantize(Decimal("0.00000001"))),
        last_qty=str(Decimal(str(round(random.uniform(0.001, 1.0), 6)))),
        best_bid_price=str((price - spread).quantize(Decimal("0.00000001"))),
        best_bid_qty=str(Decimal(str(round(random.uniform(0.1, 10.0), 4)))),
        best_ask_price=str((price + spread).quantize(Decimal("0.00000001"))),
        best_ask_qty=str(Decimal(str(round(random.uniform(0.1, 10.0), 4)))),
        base_volume=str(Decimal(str(round(random.uniform(100, 5000), 2)))),
        quote_volume=str((price * Decimal(str(round(random.uniform(100, 5000), 2)))).quantize(Decimal("0.01"))),
        trade_count=random.randint(1000, 50000),
        open_time=open_time,
        close_time=now,
        trade_timestamp=now,
    )


async def run(
    producer: CryptoProducer,
    symbols: list[str],
    tick_interval: float = 0.1,
) -> None:
    """Emit synthetic trade and ticker events indefinitely.

    Publishes one TradeEvent per symbol per tick.  A TickerEvent is emitted
    for each symbol with ~10 % probability per tick to approximate the lower
    frequency of 24hrTicker updates from the live stream.

    *tick_interval* controls events-per-second (default 0.1 s → ~10 trades/s
    across all symbols, equivalent to a quiet real-market period).
    """
    prices = {s: _BASE_PRICES.get(s, _DEFAULT_PRICE) for s in symbols}
    logger.info("Synthetic consumer started — symbols=%s tick=%.2fs", symbols, tick_interval)

    while True:
        for symbol in symbols:
            prices[symbol] = _random_walk(prices[symbol])

            trade = _make_trade_event(symbol, prices[symbol])
            producer.publish(topics.TRADES_RAW, trade)

            # Ticker less frequent than trades — matches live stream behaviour.
            if random.random() < 0.1:
                ticker = _make_ticker_event(symbol, prices[symbol])
                producer.publish(topics.TICKERS_RAW, ticker)

        await asyncio.sleep(tick_interval)
