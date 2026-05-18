"""
Canonical Kafka topic names and per-topic configuration for the crypto pipeline.

All producers and Spark consumers import topic names from this module to avoid
magic strings scattered across the codebase. Topic creation at cluster startup
is handled by infra/kafka/setup_topics.sh (docker-compose). A programmatic
create_topics() helper will be added in Phase 3 (feature/kafka-pipeline) once
the AdminClient dependency is wired in.

Pipeline position: shared constant module — imported by ingestion and processing
"""

from dataclasses import dataclass

# ── Topic name constants ──────────────────────────────────────────────────────

TRADES_RAW = "crypto.trades.raw"
TRADES_DLQ = "crypto.trades.dlq"
OHLCV_STREAMING = "crypto.ohlcv.streaming"
OHLCV_HISTORICAL = "crypto.ohlcv.historical"
TICKERS_RAW = "crypto.tickers.raw"

ALL_TOPICS = (
    TRADES_RAW,
    TRADES_DLQ,
    OHLCV_STREAMING,
    OHLCV_HISTORICAL,
    TICKERS_RAW,
)

# ── Per-topic configuration ───────────────────────────────────────────────────


@dataclass(frozen=True)
class TopicConfig:
    name: str
    partitions: int
    replication_factor: int


TOPIC_CONFIGS: tuple[TopicConfig, ...] = (
    TopicConfig(TRADES_RAW, partitions=3, replication_factor=1),
    # DLQ uses 1 partition — lower throughput, needs ordered processing for forensics
    TopicConfig(TRADES_DLQ, partitions=1, replication_factor=1),
    TopicConfig(OHLCV_STREAMING, partitions=3, replication_factor=1),
    TopicConfig(OHLCV_HISTORICAL, partitions=3, replication_factor=1),
    TopicConfig(TICKERS_RAW, partitions=3, replication_factor=1),
)
