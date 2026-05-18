"""
Kafka producer wrapper used by all ingestion sources to serialise and publish
events to the appropriate topics.

All Pydantic models are serialised to JSON bytes with Decimal fields converted
to strings (not floats) so that downstream consumers can reconstruct them with
full precision through the same reject_float validators in the schemas.

Pipeline position: ingestion consumers → CryptoProducer → Kafka topics
"""

import json
import logging
import os
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _serialize_model(model: BaseModel) -> bytes:
    """Convert a Pydantic model to JSON bytes suitable for Kafka.

    Decimal → str  preserves precision; float would lose it (reject_float rule).
    datetime → ISO-8601 str matches what Binance sends and what Pydantic accepts.
    Enum → value  (e.g. Interval.ONE_MIN → "1m").
    """

    def _enc(obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        raise TypeError(f"Object of type {type(obj)!r} is not JSON serialisable")

    return json.dumps(model.model_dump(), default=_enc).encode()


class CryptoProducer:
    """Thread-safe Kafka producer for all ingestion sources.

    Usage::

        with CryptoProducer() as producer:
            producer.publish(topics.TRADES_RAW, event)
    """

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        # acks="all": every in-sync replica must acknowledge before we return.
        # A missed trade event is unrecoverable; a few extra milliseconds of
        # latency per message is acceptable at ingestion throughput (~5 symbols).
        #
        # retries=5: handles transient broker restarts without message loss.
        #
        # linger_ms=5: micro-batches messages within a 5 ms window to improve
        # throughput without meaningfully increasing end-to-end latency.
        self._producer = KafkaProducer(
            bootstrap_servers=servers,
            acks="all",
            retries=5,
            linger_ms=5,
        )

    def publish(self, topic: str, model: BaseModel) -> None:
        """Serialise *model* and send it to *topic*; blocks until broker ACKs."""
        payload = _serialize_model(model)
        try:
            self._producer.send(topic, value=payload).get(timeout=10)
        except KafkaError as exc:
            logger.error("Failed to publish to %s: %s", topic, exc)
            raise

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.close()

    def __enter__(self) -> "CryptoProducer":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
