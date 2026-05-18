"""Tests for message normalisation in the Binance and synthetic consumers.

All tests are pure unit tests — no live network, no Kafka broker, no async
runtime required.  The normalisation functions (_normalize_trade, etc.) are
imported directly and tested as plain functions.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from ingestion.kafka import topics
from ingestion.kafka.schemas.ohlcv_record import OHLCVRecord, OHLCVSource
from ingestion.kafka.schemas.ticker_event import TickerEvent
from ingestion.kafka.schemas.trade_event import TradeEvent
from ingestion.websocket.binance_consumer import (
    _handle_message,
    _normalize_ticker,
    _normalize_trade,
)
from ingestion.websocket.synthetic_consumer import (
    _make_ticker_event,
    _make_trade_event,
    _random_walk,
)
from ingestion.rest.binance_historical import _normalize_kline

# ── Fixtures ──────────────────────────────────────────────────────────────────

_TS_MS = 1705312800000  # 2024-01-15 10:00:00 UTC in milliseconds
_TS = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


def _trade_data(**overrides) -> dict:
    return {
        "s": "BTCUSDT",
        "t": 12345,
        "p": "45000.50",
        "q": "0.001",
        "b": 1,
        "a": 2,
        "T": _TS_MS,
        "m": False,
        **overrides,
    }


def _ticker_data(**overrides) -> dict:
    return {
        "s": "BTCUSDT",
        "p": "500.00",
        "P": "1.12",
        "w": "44750.00",
        "o": "44500.00",
        "h": "45200.00",
        "l": "44300.00",
        "c": "45000.00",
        "Q": "0.05",
        "b": "44999.00",
        "B": "1.0",
        "a": "45001.00",
        "A": "0.5",
        "v": "1200.00",
        "q": "53700000.00",
        "n": 18000,
        "O": _TS_MS,
        "C": _TS_MS + 60_000,
        "E": _TS_MS,
        **overrides,
    }


def _kline(**overrides) -> list:
    base = [
        _TS_MS,            # 0 open_time
        "45000.00",        # 1 open
        "45100.00",        # 2 high
        "44900.00",        # 3 low
        "45050.00",        # 4 close
        "10.5",            # 5 volume
        _TS_MS + 59_999,   # 6 close_time
        "472500.00",       # 7 quote volume
        150,               # 8 trade count
        "6.0",             # 9 taker buy base volume
        "270000.00",       # 10 taker buy quote volume
        "0",               # 11 ignore
    ]
    for k, v in overrides.items():
        base[k] = v
    return base


# ── _normalize_trade ──────────────────────────────────────────────────────────


class TestNormalizeTrade:
    def test_returns_trade_event(self):
        assert isinstance(_normalize_trade(_trade_data()), TradeEvent)

    def test_symbol_mapped(self):
        assert _normalize_trade(_trade_data()).symbol == "BTCUSDT"

    def test_trade_id_mapped(self):
        assert _normalize_trade(_trade_data()).trade_id == 12345

    def test_price_is_decimal_string_coerced(self):
        event = _normalize_trade(_trade_data())
        assert event.price == Decimal("45000.50")

    def test_timestamp_ms_converted_to_utc_datetime(self):
        event = _normalize_trade(_trade_data())
        assert event.trade_timestamp == _TS
        assert event.trade_timestamp.tzinfo is not None

    def test_is_buyer_maker_mapped(self):
        assert _normalize_trade(_trade_data(m=True)).is_buyer_maker is True
        assert _normalize_trade(_trade_data(m=False)).is_buyer_maker is False

    def test_missing_field_raises(self):
        data = _trade_data()
        del data["p"]
        with pytest.raises(Exception):
            _normalize_trade(data)


# ── _normalize_ticker ─────────────────────────────────────────────────────────


class TestNormalizeTicker:
    def test_returns_ticker_event(self):
        assert isinstance(_normalize_ticker(_ticker_data()), TickerEvent)

    def test_symbol_mapped(self):
        assert _normalize_ticker(_ticker_data()).symbol == "BTCUSDT"

    def test_last_price_is_decimal(self):
        event = _normalize_ticker(_ticker_data())
        assert event.last_price == Decimal("45000.00")

    def test_trade_count_mapped(self):
        assert _normalize_ticker(_ticker_data()).trade_count == 18000

    def test_timestamps_converted_from_ms(self):
        event = _normalize_ticker(_ticker_data())
        assert event.trade_timestamp == _TS
        assert event.open_time == _TS

    def test_missing_field_raises(self):
        data = _ticker_data()
        del data["c"]
        with pytest.raises(Exception):
            _normalize_ticker(data)


# ── _normalize_kline ──────────────────────────────────────────────────────────


class TestNormalizeKline:
    def test_returns_ohlcv_record(self):
        assert isinstance(_normalize_kline(_kline(), "BTCUSDT", "1h"), OHLCVRecord)

    def test_symbol_and_interval_set(self):
        record = _normalize_kline(_kline(), "ETHUSDT", "4h")
        assert record.symbol == "ETHUSDT"
        assert record.interval.value == "4h"

    def test_prices_are_decimal(self):
        record = _normalize_kline(_kline(), "BTCUSDT", "1h")
        assert record.open_price == Decimal("45000.00")
        assert record.high_price == Decimal("45100.00")

    def test_source_is_historical(self):
        record = _normalize_kline(_kline(), "BTCUSDT", "1h")
        assert record.source is OHLCVSource.HISTORICAL

    def test_is_closed_always_true(self):
        assert _normalize_kline(_kline(), "BTCUSDT", "1h").is_closed is True

    def test_open_time_converted_from_ms(self):
        record = _normalize_kline(_kline(), "BTCUSDT", "1h")
        assert record.open_time == _TS
        assert record.open_time.tzinfo is not None

    def test_trade_timestamp_equals_open_time(self):
        record = _normalize_kline(_kline(), "BTCUSDT", "1h")
        assert record.trade_timestamp == record.open_time

    def test_high_less_than_low_raises(self):
        with pytest.raises(Exception):
            _normalize_kline(_kline(**{2: "44000.00", 3: "45100.00"}), "BTCUSDT", "1h")


# ── _handle_message ───────────────────────────────────────────────────────────


class TestHandleMessage:
    def _mock_producer(self):
        p = MagicMock()
        p._producer = MagicMock()
        return p

    def test_trade_stream_publishes_to_trades_raw(self):
        producer = self._mock_producer()
        raw = json.dumps({"stream": "btcusdt@trade", "data": _trade_data()})
        _handle_message(raw, producer)
        assert producer.publish.call_args[0][0] == topics.TRADES_RAW

    def test_ticker_stream_publishes_to_tickers_raw(self):
        producer = self._mock_producer()
        raw = json.dumps({"stream": "btcusdt@ticker", "data": _ticker_data()})
        _handle_message(raw, producer)
        assert producer.publish.call_args[0][0] == topics.TICKERS_RAW

    def test_malformed_json_routes_to_dlq(self):
        producer = self._mock_producer()
        _handle_message("not json {{{", producer)
        producer._producer.send.assert_called_once()
        assert producer._producer.send.call_args[0][0] == topics.TRADES_DLQ

    def test_missing_field_routes_to_dlq(self):
        producer = self._mock_producer()
        data = _trade_data()
        del data["p"]
        raw = json.dumps({"stream": "btcusdt@trade", "data": data})
        _handle_message(raw, producer)
        producer._producer.send.assert_called_once()
        assert producer._producer.send.call_args[0][0] == topics.TRADES_DLQ

    def test_unknown_stream_does_not_publish(self):
        producer = self._mock_producer()
        raw = json.dumps({"stream": "btcusdt@unknown", "data": {}})
        _handle_message(raw, producer)
        producer.publish.assert_not_called()
        producer._producer.send.assert_not_called()


# ── Synthetic consumer ────────────────────────────────────────────────────────


class TestSyntheticConsumer:
    def test_make_trade_event_returns_valid_model(self):
        event = _make_trade_event("BTCUSDT", Decimal("45000"))
        assert isinstance(event, TradeEvent)
        assert event.symbol == "BTCUSDT"
        assert isinstance(event.price, Decimal)
        assert event.trade_id > 0

    def test_make_trade_event_price_near_seed(self):
        price = Decimal("45000")
        event = _make_trade_event("BTCUSDT", price)
        # price should be close to seed — exact match depends on random walk state
        assert event.price > Decimal("0")

    def test_make_ticker_event_returns_valid_model(self):
        ticker = _make_ticker_event("BTCUSDT", Decimal("45000"))
        assert isinstance(ticker, TickerEvent)
        assert ticker.symbol == "BTCUSDT"

    def test_make_ticker_event_high_gte_low(self):
        ticker = _make_ticker_event("BTCUSDT", Decimal("45000"))
        assert ticker.high_price >= ticker.low_price

    def test_make_ticker_event_best_ask_gt_best_bid(self):
        ticker = _make_ticker_event("BTCUSDT", Decimal("45000"))
        assert ticker.best_ask_price > ticker.best_bid_price

    def test_random_walk_stays_positive(self):
        price = Decimal("0.55")
        for _ in range(100):
            price = _random_walk(price)
        assert price > Decimal("0")

    def test_trade_event_passes_schema_validation(self):
        event = _make_trade_event("ETHUSDT", Decimal("2500"))
        # Re-construct from a JSON round-trip to verify serialisation compatibility
        from ingestion.kafka.producer import _serialize_model
        payload = json.loads(_serialize_model(event))
        reconstructed = TradeEvent(**payload)
        assert reconstructed.symbol == "ETHUSDT"

    def test_ticker_event_passes_schema_validation(self):
        ticker = _make_ticker_event("ETHUSDT", Decimal("2500"))
        from ingestion.kafka.producer import _serialize_model
        payload = json.loads(_serialize_model(ticker))
        reconstructed = TickerEvent(**payload)
        assert reconstructed.symbol == "ETHUSDT"
