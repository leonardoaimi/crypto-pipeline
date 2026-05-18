"""
Validates raw Kafka message payloads against their registered Pydantic schemas.

Messages that fail JSON parsing or schema validation are not silently dropped.
They are wrapped in a DLQEnvelope and returned to the caller, which is
responsible for publishing the envelope to crypto.trades.dlq for forensic
review. This function is intentionally side-effect-free: no Kafka I/O happens
here, making it straightforward to unit-test without a running broker.

Pipeline position: ingestion consumers → schema_validator → Kafka topics / DLQ
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pydantic import BaseModel, ValidationError

from ingestion.kafka import topics as t
from ingestion.kafka.schemas.ohlcv_record import OHLCVRecord
from ingestion.kafka.schemas.ticker_event import TickerEvent
from ingestion.kafka.schemas.trade_event import TradeEvent

# Maps each data topic to its canonical Pydantic schema.
# The DLQ topic is intentionally excluded — its messages are already envelopes,
# not raw exchange payloads.
_TOPIC_SCHEMA: dict[str, type[BaseModel]] = {
    t.TRADES_RAW: TradeEvent,
    t.OHLCV_STREAMING: OHLCVRecord,
    t.OHLCV_HISTORICAL: OHLCVRecord,
    t.TICKERS_RAW: TickerEvent,
}


@dataclass
class DLQEnvelope:
    """Wraps an unprocessable message for publication to the dead-letter queue."""

    original_payload: str
    topic: str
    error: str
    ingestion_timestamp: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


def validate_message(
    topic: str,
    payload: bytes,
) -> tuple[BaseModel | None, DLQEnvelope | None]:
    """
    Validate a raw Kafka payload against the schema registered for *topic*.

    Returns ``(model, None)`` on success or ``(None, DLQEnvelope)`` on any
    failure. The caller decides whether to commit the Kafka offset before or
    after publishing the DLQ envelope — no offset management happens here.
    """
    schema = _TOPIC_SCHEMA.get(topic)
    if schema is None:
        return None, DLQEnvelope(
            original_payload=payload.decode(errors="replace"),
            topic=topic,
            error=f"No schema registered for topic '{topic}'",
        )

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        return None, DLQEnvelope(
            original_payload=payload.decode(errors="replace"),
            topic=topic,
            error=f"JSON decode error: {exc}",
        )

    try:
        return schema(**data), None
    except ValidationError as exc:
        return None, DLQEnvelope(
            original_payload=payload.decode(errors="replace"),
            topic=topic,
            error=f"Validation failed ({exc.error_count()} error(s)): {exc}",
        )
