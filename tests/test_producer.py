"""Tests for the CryptoProducer Kafka wrapper and model serialisation."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from ingestion.kafka import topics
from ingestion.kafka.producer import CryptoProducer, _serialize_model
from ingestion.kafka.schemas.ohlcv_record import Interval, OHLCVRecord, OHLCVSource
from ingestion.kafka.schemas.trade_event import TradeEvent

_TS = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
_TS_CLOSE = datetime(2024, 1, 15, 10, 1, 0, tzinfo=timezone.utc)


# ── _serialize_model ──────────────────────────────────────────────────────────


class TestSerializeModel:
    def _trade(self) -> TradeEvent:
        return TradeEvent(
            symbol="BTCUSDT",
            trade_id=1,
            price="45000.50",
            quantity="0.001",
            buyer_order_id=1,
            seller_order_id=2,
            is_buyer_maker=False,
            trade_timestamp=_TS,
        )

    def test_decimal_serialised_as_string(self):
        payload = json.loads(_serialize_model(self._trade()))
        assert isinstance(payload["price"], str)
        assert payload["price"] == "45000.50"

    def test_decimal_precision_preserved(self):
        trade = TradeEvent(
            symbol="ETHUSDT",
            trade_id=2,
            price="3000.123456789",
            quantity="1",
            buyer_order_id=1,
            seller_order_id=2,
            is_buyer_maker=True,
            trade_timestamp=_TS,
        )
        payload = json.loads(_serialize_model(trade))
        # Must round-trip without float truncation
        assert payload["price"] == "3000.123456789"

    def test_datetime_serialised_as_iso8601(self):
        payload = json.loads(_serialize_model(self._trade()))
        assert payload["trade_timestamp"] == _TS.isoformat()

    def test_enum_serialised_as_value(self):
        record = OHLCVRecord(
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
        payload = json.loads(_serialize_model(record))
        assert payload["interval"] == "1m"
        assert payload["source"] == "streaming"

    def test_returns_bytes(self):
        assert isinstance(_serialize_model(self._trade()), bytes)

    def test_round_trips_through_schema(self):
        original = self._trade()
        payload = json.loads(_serialize_model(original))
        # The JSON must be reloadable into the same schema without errors
        reconstructed = TradeEvent(**payload)
        assert reconstructed.price == original.price
        assert reconstructed.symbol == original.symbol


# ── CryptoProducer ────────────────────────────────────────────────────────────


@patch("ingestion.kafka.producer.KafkaProducer")
class TestCryptoProducer:
    def _trade(self) -> TradeEvent:
        return TradeEvent(
            symbol="BTCUSDT",
            trade_id=1,
            price="45000",
            quantity="0.001",
            buyer_order_id=1,
            seller_order_id=2,
            is_buyer_maker=False,
            trade_timestamp=_TS,
        )

    def test_publish_calls_send_with_correct_topic(self, mock_kp_cls):
        mock_kp = MagicMock()
        mock_kp_cls.return_value = mock_kp
        mock_kp.send.return_value = MagicMock()

        producer = CryptoProducer(bootstrap_servers="localhost:9092")
        producer.publish(topics.TRADES_RAW, self._trade())

        assert mock_kp.send.call_args[0][0] == topics.TRADES_RAW

    def test_publish_payload_is_bytes(self, mock_kp_cls):
        mock_kp = MagicMock()
        mock_kp_cls.return_value = mock_kp
        mock_kp.send.return_value = MagicMock()

        producer = CryptoProducer(bootstrap_servers="localhost:9092")
        producer.publish(topics.TRADES_RAW, self._trade())

        payload = mock_kp.send.call_args[1]["value"]
        assert isinstance(payload, bytes)
        json.loads(payload)  # must be valid JSON

    def test_publish_decimal_not_float_in_payload(self, mock_kp_cls):
        mock_kp = MagicMock()
        mock_kp_cls.return_value = mock_kp
        mock_kp.send.return_value = MagicMock()

        producer = CryptoProducer(bootstrap_servers="localhost:9092")
        producer.publish(topics.TRADES_RAW, self._trade())

        payload = json.loads(mock_kp.send.call_args[1]["value"])
        # price must be a JSON string — not a number — to survive reject_float
        assert isinstance(payload["price"], str)

    def test_flush_delegates_to_underlying_producer(self, mock_kp_cls):
        mock_kp = MagicMock()
        mock_kp_cls.return_value = mock_kp

        producer = CryptoProducer(bootstrap_servers="localhost:9092")
        producer.flush()

        mock_kp.flush.assert_called_once()

    def test_close_delegates_to_underlying_producer(self, mock_kp_cls):
        mock_kp = MagicMock()
        mock_kp_cls.return_value = mock_kp

        producer = CryptoProducer(bootstrap_servers="localhost:9092")
        producer.close()

        mock_kp.close.assert_called_once()

    def test_context_manager_closes_on_exit(self, mock_kp_cls):
        mock_kp = MagicMock()
        mock_kp_cls.return_value = mock_kp

        with CryptoProducer(bootstrap_servers="localhost:9092"):
            pass

        mock_kp.close.assert_called_once()

    def test_bootstrap_servers_from_arg(self, mock_kp_cls):
        mock_kp_cls.return_value = MagicMock()
        CryptoProducer(bootstrap_servers="broker:9092")
        _, kwargs = mock_kp_cls.call_args
        assert kwargs["bootstrap_servers"] == "broker:9092"

    def test_bootstrap_servers_from_env(self, mock_kp_cls, monkeypatch):
        mock_kp_cls.return_value = MagicMock()
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "envbroker:9092")
        CryptoProducer()
        _, kwargs = mock_kp_cls.call_args
        assert kwargs["bootstrap_servers"] == "envbroker:9092"
