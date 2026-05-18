"""
Connects to the Binance WebSocket combined stream and publishes normalised
trade and ticker events to Kafka. Activated when DATA_SOURCE=live.

Uses Binance's /stream?streams=... combined endpoint so all symbols share a
single WebSocket connection instead of one connection per symbol — reduces
connection overhead and simplifies reconnection logic.

Field mapping from Binance @trade stream:
  s → symbol          t → trade_id        p → price (str)     q → quantity (str)
  b → buyer_order_id  a → seller_order_id  m → is_buyer_maker  T → trade_timestamp (ms)

Field mapping from Binance 24hrTicker stream:
  s → symbol    p → price_change        P → price_change_percent
  w → weighted_avg_price  o → open_price  h → high_price  l → low_price
  c → last_price  Q → last_qty  b → best_bid_price  B → best_bid_qty
  a → best_ask_price  A → best_ask_qty  v → base_volume  q → quote_volume
  n → trade_count  O → open_time (ms)  C → close_time (ms)  E → event_time (ms)

Pipeline position: Binance WebSocket → binance_consumer → CryptoProducer → Kafka
"""

import asyncio
import dataclasses
import json
import logging
from datetime import datetime, timezone

import websockets
import websockets.exceptions
from pydantic import ValidationError

from ingestion.kafka import topics
from ingestion.kafka.producer import CryptoProducer
from ingestion.kafka.schemas.ticker_event import TickerEvent
from ingestion.kafka.schemas.trade_event import TradeEvent
from ingestion.validators.schema_validator import DLQEnvelope

logger = logging.getLogger(__name__)

_RECONNECT_DELAY_S = 5


def _normalize_trade(data: dict) -> TradeEvent:
    """Map a Binance @trade payload dict → TradeEvent.

    Prices arrive as JSON strings ("p": "45000.50") which Pydantic coerces to
    Decimal exactly — no float precision loss at this step.
    """
    return TradeEvent(
        symbol=data["s"],
        trade_id=data["t"],
        price=data["p"],
        quantity=data["q"],
        buyer_order_id=data["b"],
        seller_order_id=data["a"],
        is_buyer_maker=data["m"],
        trade_timestamp=datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc),
    )


def _normalize_ticker(data: dict) -> TickerEvent:
    """Map a Binance 24hrTicker payload dict → TickerEvent."""
    return TickerEvent(
        symbol=data["s"],
        price_change=data["p"],
        price_change_percent=data["P"],
        weighted_avg_price=data["w"],
        open_price=data["o"],
        high_price=data["h"],
        low_price=data["l"],
        last_price=data["c"],
        last_qty=data["Q"],
        best_bid_price=data["b"],
        best_bid_qty=data["B"],
        best_ask_price=data["a"],
        best_ask_qty=data["A"],
        base_volume=data["v"],
        quote_volume=data["q"],
        trade_count=data["n"],
        open_time=datetime.fromtimestamp(data["O"] / 1000, tz=timezone.utc),
        close_time=datetime.fromtimestamp(data["C"] / 1000, tz=timezone.utc),
        trade_timestamp=datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc),
    )


def _publish_dlq(raw: str, topic: str, error: str, producer: CryptoProducer) -> None:
    """Wrap an unprocessable message in a DLQEnvelope and publish it."""
    envelope = DLQEnvelope(original_payload=raw, topic=topic, error=error)
    payload = json.dumps(dataclasses.asdict(envelope), default=str).encode()
    producer._producer.send(topics.TRADES_DLQ, value=payload)
    logger.warning("DLQ routed: topic=%s error=%s", topic, error)


def _handle_message(raw: str, producer: CryptoProducer) -> None:
    """Parse one Binance WebSocket frame and route it to the correct Kafka topic.

    Binance wraps each event in {"stream": "<name>", "data": {...}}.  We
    unwrap, normalise to the canonical Pydantic schema, then publish.  Any
    failure (bad JSON, missing field, validation error) goes to the DLQ so no
    message is silently dropped.
    """
    try:
        msg = json.loads(raw)
        stream_name: str = msg.get("stream", "")
        data: dict = msg.get("data", {})
    except (json.JSONDecodeError, AttributeError) as exc:
        _publish_dlq(raw, topics.TRADES_DLQ, f"JSON decode error: {exc}", producer)
        return

    try:
        if "@trade" in stream_name:
            producer.publish(topics.TRADES_RAW, _normalize_trade(data))
        elif "@ticker" in stream_name:
            producer.publish(topics.TICKERS_RAW, _normalize_ticker(data))
        else:
            logger.debug("Unrecognised stream: %s", stream_name)
    except (KeyError, ValidationError) as exc:
        _publish_dlq(raw, topics.TRADES_DLQ, f"Normalisation failed: {exc}", producer)


async def run(
    producer: CryptoProducer,
    symbols: list[str],
    ws_base: str,
) -> None:
    """Connect to Binance combined stream and publish events indefinitely.

    Reconnects automatically on any connection error.  *ws_base* is the
    Binance WebSocket base URL (e.g. ``wss://stream.binance.com:9443/ws``);
    the combined-stream path replaces the trailing ``/ws`` with ``/stream``.
    """
    streams = [f"{s.lower()}@trade" for s in symbols] + [
        f"{s.lower()}@ticker" for s in symbols
    ]
    # Binance combined endpoint: /stream?streams=s1/s2/...
    # Per-symbol endpoint is /ws/<stream>. Combined keeps one TCP connection
    # for all symbols — simpler reconnection and lower broker load.
    base = ws_base.removesuffix("/ws")
    url = f"{base}/stream?streams={'/'.join(streams)}"
    logger.info("Connecting to %s", url)

    while True:
        try:
            async with websockets.connect(url) as ws:
                logger.info("WebSocket connected")
                async for raw in ws:
                    _handle_message(raw, producer)
        except websockets.exceptions.ConnectionClosed as exc:
            logger.warning(
                "Connection closed (%s); reconnecting in %ss", exc, _RECONNECT_DELAY_S
            )
        except Exception as exc:
            logger.error(
                "Unexpected error (%s); reconnecting in %ss", exc, _RECONNECT_DELAY_S
            )
        await asyncio.sleep(_RECONNECT_DELAY_S)
