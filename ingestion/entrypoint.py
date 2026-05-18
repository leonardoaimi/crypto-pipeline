"""
Ingestion service entrypoint.  Reads DATA_SOURCE from the environment and
dispatches to the appropriate consumer:

  DATA_SOURCE=live       → binance_consumer.run()   (real Binance WebSocket)
  DATA_SOURCE=synthetic  → synthetic_consumer.run() (no network, drop-in schema)

Designed to be the Docker CMD so the same image runs in both live and CI/CD
environments — only the DATA_SOURCE env var changes.

Pipeline position: Docker container start → entrypoint → consumer → Kafka
"""

import asyncio
import logging
import os
from pathlib import Path

import yaml

from ingestion.kafka.producer import CryptoProducer
from ingestion.websocket import binance_consumer, synthetic_consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _load_config() -> dict:
    config_path = Path(__file__).parent.parent / "config.yml"
    with config_path.open() as f:
        return yaml.safe_load(f)


async def _main() -> None:
    config = _load_config()
    # DATA_SOURCE env var overrides config.yml so docker-compose can toggle
    # between live and synthetic without rebuilding the image.
    data_source = os.environ.get("DATA_SOURCE", config.get("data_source", "live"))
    symbols: list[str] = config["symbols"]
    ws_base: str = config["exchange"]["websocket_base"]

    logger.info("data_source=%s symbols=%s", data_source, symbols)

    with CryptoProducer() as producer:
        if data_source == "synthetic":
            await synthetic_consumer.run(producer, symbols)
        else:
            await binance_consumer.run(producer, symbols, ws_base)


if __name__ == "__main__":
    asyncio.run(_main())
