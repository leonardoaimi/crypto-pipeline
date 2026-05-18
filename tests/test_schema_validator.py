"""Tests for Pydantic event schemas and schema_validator DLQ routing."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from ingestion.kafka import topics
from ingestion.kafka.schemas.ohlcv_record import Interval, OHLCVRecord, OHLCVSource
from ingestion.kafka.schemas.ticker_event import TickerEvent
from ingestion.kafka.schemas.trade_event import TradeEvent
from ingestion.validators.schema_validator import DLQEnvelope, validate_message

_TS = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
_TS_CLOSE = datetime(2024, 1, 15, 10, 1, 0, tzinfo=timezone.utc)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _trade_dict(**overrides) -> dict:
    return {
        "symbol": "BTCUSDT",
        "trade_id": 123456,
        "price": "45000.50",
        "quantity": "0.001",
        "buyer_order_id": 1,
        "seller_order_id": 2,
        "is_buyer_maker": False,
        "trade_timestamp": _TS.isoformat(),
        **overrides,
    }


def _trade_payload(**overrides) -> bytes:
    return json.dumps(_trade_dict(**overrides)).encode()


def _ohlcv_dict(**overrides) -> dict:
    return {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "open_time": _TS.isoformat(),
        "close_time": _TS_CLOSE.isoformat(),
        "open_price": "45000.00",
        "high_price": "45100.00",
        "low_price": "44900.00",
        "close_price": "45050.00",
        "volume": "10.5",
        "quote_volume": "472500.00",
        "taker_buy_base_volume": "6.0",
        "taker_buy_quote_volume": "270000.00",
        "trade_count": 150,
        "is_closed": True,
        "source": "streaming",
        "trade_timestamp": _TS.isoformat(),
        **overrides,
    }


def _ohlcv_payload(**overrides) -> bytes:
    return json.dumps(_ohlcv_dict(**overrides)).encode()


# ── TradeEvent ────────────────────────────────────────────────────────────────


class TestTradeEvent:
    def test_valid_event_creates_successfully(self):
        event = TradeEvent(
            symbol="BTCUSDT",
            trade_id=123456,
            price=Decimal("45000.50"),
            quantity=Decimal("0.001"),
            buyer_order_id=1,
            seller_order_id=2,
            is_buyer_maker=False,
            trade_timestamp=_TS,
        )
        assert event.price == Decimal("45000.50")
        assert event.trade_id == 123456
        assert event.symbol == "BTCUSDT"

    def test_string_price_coerced_to_decimal_without_loss(self):
        event = TradeEvent(
            symbol="ETHUSDT",
            trade_id=1,
            price="3000.123456789",
            quantity="1",
            buyer_order_id=1,
            seller_order_id=2,
            is_buyer_maker=True,
            trade_timestamp=_TS,
        )
        # Decimal("3000.123456789") preserves all digits; float would round
        assert event.price == Decimal("3000.123456789")
        assert str(event.price) == "3000.123456789"

    def test_float_price_raises(self):
        with pytest.raises(Exception):
            TradeEvent(
                symbol="BTCUSDT",
                trade_id=1,
                price=45000.50,  # float — must be rejected
                quantity="0.001",
                buyer_order_id=1,
                seller_order_id=2,
                is_buyer_maker=False,
                trade_timestamp=_TS,
            )

    def test_float_quantity_raises(self):
        with pytest.raises(Exception):
            TradeEvent(
                symbol="BTCUSDT",
                trade_id=1,
                price="45000",
                quantity=0.001,  # float — must be rejected
                buyer_order_id=1,
                seller_order_id=2,
                is_buyer_maker=False,
                trade_timestamp=_TS,
            )

    def test_ingestion_timestamp_defaults_to_utc_now(self):
        before = datetime.now(tz=timezone.utc)
        event = TradeEvent(
            symbol="BTCUSDT",
            trade_id=1,
            price="45000",
            quantity="1",
            buyer_order_id=1,
            seller_order_id=2,
            is_buyer_maker=False,
            trade_timestamp=_TS,
        )
        after = datetime.now(tz=timezone.utc)
        assert before <= event.ingestion_timestamp <= after

    def test_model_is_immutable(self):
        event = TradeEvent(
            symbol="BTCUSDT",
            trade_id=1,
            price="45000",
            quantity="1",
            buyer_order_id=1,
            seller_order_id=2,
            is_buyer_maker=False,
            trade_timestamp=_TS,
        )
        with pytest.raises(Exception):
            event.price = Decimal("99999")  # type: ignore[misc]

    def test_missing_required_field_raises(self):
        with pytest.raises(Exception):
            TradeEvent(symbol="BTCUSDT", trade_id=1, price="45000")  # type: ignore[call-arg]


# ── TickerEvent ───────────────────────────────────────────────────────────────


class TestTickerEvent:
    def _make(self, **overrides) -> TickerEvent:
        defaults = dict(
            symbol="BTCUSDT",
            price_change="500.00",
            price_change_percent="1.12",
            weighted_avg_price="44750.00",
            open_price="44500.00",
            high_price="45200.00",
            low_price="44300.00",
            last_price="45000.00",
            last_qty="0.05",
            best_bid_price="44999.00",
            best_bid_qty="1.0",
            best_ask_price="45001.00",
            best_ask_qty="0.5",
            base_volume="1200.00",
            quote_volume="53700000.00",
            trade_count=18000,
            open_time=_TS,
            close_time=_TS_CLOSE,
            trade_timestamp=_TS,
        )
        defaults.update(overrides)
        return TickerEvent(**defaults)

    def test_valid_ticker_creates_successfully(self):
        ticker = self._make()
        assert ticker.symbol == "BTCUSDT"
        assert ticker.last_price == Decimal("45000.00")

    def test_float_field_raises(self):
        with pytest.raises(Exception):
            self._make(last_price=45000.0)

    def test_ingestion_timestamp_set(self):
        ticker = self._make()
        assert ticker.ingestion_timestamp is not None
        assert ticker.ingestion_timestamp.tzinfo is not None


# ── OHLCVRecord ───────────────────────────────────────────────────────────────


class TestOHLCVRecord:
    def _make(self, **overrides) -> OHLCVRecord:
        defaults = dict(
            symbol="BTCUSDT",
            interval=Interval.ONE_MIN,
            open_time=_TS,
            close_time=_TS_CLOSE,
            open_price="45000",
            high_price="45100",
            low_price="44900",
            close_price="45050",
            volume="10.5",
            quote_volume="472500",
            taker_buy_base_volume="6",
            taker_buy_quote_volume="270000",
            trade_count=150,
            is_closed=True,
            source=OHLCVSource.STREAMING,
            trade_timestamp=_TS,
        )
        defaults.update(overrides)
        return OHLCVRecord(**defaults)

    def test_valid_record_creates_successfully(self):
        record = self._make()
        assert record.high_price > record.low_price
        assert record.source == OHLCVSource.STREAMING

    def test_high_less_than_low_raises(self):
        with pytest.raises(Exception, match="high_price"):
            self._make(high_price="44000", low_price="45000")

    def test_high_equal_to_low_is_valid(self):
        # A doji candle (open == high == low == close) is legitimate
        record = self._make(high_price="45000", low_price="45000")
        assert record.high_price == record.low_price

    def test_interval_string_coerced_to_enum(self):
        record = self._make(interval="5m")
        assert record.interval is Interval.FIVE_MIN

    def test_source_string_coerced_to_enum(self):
        record = self._make(source="historical")
        assert record.source is OHLCVSource.HISTORICAL

    def test_float_price_raises(self):
        with pytest.raises(Exception):
            self._make(open_price=45000.0)

    def test_model_is_immutable(self):
        record = self._make()
        with pytest.raises(Exception):
            record.close_price = Decimal("0")  # type: ignore[misc]


# ── validate_message ──────────────────────────────────────────────────────────


class TestValidateMessage:
    def test_valid_trade_returns_model(self):
        model, dlq = validate_message(topics.TRADES_RAW, _trade_payload())
        assert model is not None
        assert dlq is None
        assert isinstance(model, TradeEvent)
        assert model.symbol == "BTCUSDT"

    def test_valid_ohlcv_streaming_returns_model(self):
        model, dlq = validate_message(topics.OHLCV_STREAMING, _ohlcv_payload())
        assert model is not None
        assert dlq is None
        assert isinstance(model, OHLCVRecord)

    def test_valid_ohlcv_historical_returns_model(self):
        model, dlq = validate_message(
            topics.OHLCV_HISTORICAL,
            _ohlcv_payload(source="historical"),
        )
        assert model is not None
        assert isinstance(model, OHLCVRecord)
        assert model.source is OHLCVSource.HISTORICAL

    def test_ohlcv_historical_and_streaming_use_same_schema(self):
        m_s, _ = validate_message(topics.OHLCV_STREAMING, _ohlcv_payload())
        m_h, _ = validate_message(topics.OHLCV_HISTORICAL, _ohlcv_payload())
        assert type(m_s) is type(m_h)

    def test_malformed_json_returns_dlq(self):
        model, dlq = validate_message(topics.TRADES_RAW, b"not json {{{")
        assert model is None
        assert dlq is not None
        assert isinstance(dlq, DLQEnvelope)
        assert "JSON" in dlq.error

    def test_missing_required_field_returns_dlq(self):
        data = _trade_dict()
        del data["trade_id"]
        model, dlq = validate_message(topics.TRADES_RAW, json.dumps(data).encode())
        assert model is None
        assert dlq is not None
        assert "Validation" in dlq.error

    def test_float_price_routes_to_dlq(self):
        # Float prices violate the Decimal precision contract and must be rejected
        model, dlq = validate_message(topics.TRADES_RAW, _trade_payload(price=45000.50))
        assert model is None
        assert dlq is not None

    def test_ohlcv_high_less_than_low_routes_to_dlq(self):
        model, dlq = validate_message(
            topics.OHLCV_STREAMING,
            _ohlcv_payload(high_price="44000", low_price="45000"),
        )
        assert model is None
        assert dlq is not None

    def test_unknown_topic_returns_dlq(self):
        model, dlq = validate_message("crypto.unknown.topic", b"{}")
        assert model is None
        assert dlq is not None
        assert "No schema" in dlq.error

    def test_dlq_preserves_original_payload(self):
        raw = b'{"bad": true}'
        _, dlq = validate_message("crypto.unknown.topic", raw)
        assert dlq is not None
        assert dlq.original_payload == '{"bad": true}'

    def test_dlq_has_ingestion_timestamp(self):
        _, dlq = validate_message("bad.topic", b"x")
        assert dlq is not None
        assert isinstance(dlq.ingestion_timestamp, datetime)
        assert dlq.ingestion_timestamp.tzinfo is not None

    def test_dlq_records_topic(self):
        _, dlq = validate_message(topics.TRADES_RAW, b"bad json")
        assert dlq is not None
        assert dlq.topic == topics.TRADES_RAW

    def test_empty_payload_returns_dlq(self):
        model, dlq = validate_message(topics.TRADES_RAW, b"")
        assert model is None
        assert dlq is not None
