"""
Fetches historical OHLCV klines from the Binance REST API and publishes them
to crypto.ohlcv.historical for downstream Spark batch jobs.

Binance kline array layout (per element):
  [0]  Open time          (ms epoch int)
  [1]  Open price         (str)
  [2]  High price         (str)
  [3]  Low price          (str)
  [4]  Close price        (str)
  [5]  Volume             (str, base asset)
  [6]  Close time         (ms epoch int)
  [7]  Quote asset volume (str)
  [8]  Number of trades   (int)
  [9]  Taker buy base volume   (str)
  [10] Taker buy quote volume  (str)
  [11] Ignore

Idempotency: each kline is keyed on (symbol, interval, open_time) in
int_ohlcv_unified.  Re-running this job with the same time range produces
duplicate Kafka messages, but the dbt dedup layer (DISTINCT ON) ensures the
PostgreSQL tables remain idempotent.

Rate limiting: Binance allows ~1 200 requests/minute on the public REST API.
We sleep 100 ms between pages, giving ~10 req/s which is well within limits
even for all 5 symbols × 3 intervals in one run.

Pipeline position: Binance REST API → binance_historical → CryptoProducer → Kafka
"""

import logging
import time
from datetime import datetime, timezone

import httpx

from ingestion.kafka import topics
from ingestion.kafka.producer import CryptoProducer
from ingestion.kafka.schemas.ohlcv_record import OHLCVRecord, OHLCVSource

logger = logging.getLogger(__name__)

_KLINES_LIMIT = 1000  # Binance maximum per request
_REQUEST_SLEEP_S = 0.1  # 100 ms between pages to respect rate limits


def _normalize_kline(kline: list, symbol: str, interval: str) -> OHLCVRecord:
    """Map a Binance kline array → OHLCVRecord.

    All REST klines are fully closed bars — is_closed=True always.
    trade_timestamp is set to open_time (the start of the bar) so Spark
    can use it as an event-time clock consistent with streaming bars.
    """
    open_time = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)
    close_time = datetime.fromtimestamp(kline[6] / 1000, tz=timezone.utc)
    return OHLCVRecord(
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        close_time=close_time,
        open_price=kline[1],
        high_price=kline[2],
        low_price=kline[3],
        close_price=kline[4],
        volume=kline[5],
        quote_volume=kline[7],
        taker_buy_base_volume=kline[9],
        taker_buy_quote_volume=kline[10],
        trade_count=kline[8],
        is_closed=True,
        # Historical preferred over streaming in int_ohlcv_unified because REST
        # klines are computed server-side with no windowing artefacts; see dbt
        # model docstring for the full rationale.
        source=OHLCVSource.HISTORICAL,
        trade_timestamp=open_time,
    )


def _fetch_klines(
    client: httpx.Client,
    rest_base: str,
    symbol: str,
    interval: str,
    start_ms: int,
    limit: int = _KLINES_LIMIT,
) -> list[list]:
    """Fetch one page of klines from the Binance REST API."""
    response = client.get(
        f"{rest_base}/klines",
        params={
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "limit": limit,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def fetch_and_publish(
    producer: CryptoProducer,
    symbol: str,
    interval: str,
    days: int,
    rest_base: str,
) -> int:
    """Fetch *days* of klines for (*symbol*, *interval*) and publish to Kafka.

    Paginates through the full time range in 1 000-kline pages.  Returns the
    total number of records published.  Designed to be idempotent — running
    twice over the same range publishes duplicates that dbt deduplicates.
    """
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    start_ms = now_ms - days * 86_400_000
    end_ms = now_ms
    count = 0

    logger.info(
        "Fetching %s/%s — %d days (%d ms → %d ms)",
        symbol, interval, days, start_ms, end_ms,
    )

    with httpx.Client() as client:
        while start_ms < end_ms:
            klines = _fetch_klines(client, rest_base, symbol, interval, start_ms)
            if not klines:
                break

            for kline in klines:
                record = _normalize_kline(kline, symbol, interval)
                producer.publish(topics.OHLCV_HISTORICAL, record)
                count += 1

            # Advance start to the close_time of the last kline + 1 ms so we
            # don't re-fetch the same bar on the next page.
            start_ms = klines[-1][6] + 1
            time.sleep(_REQUEST_SLEEP_S)

    logger.info("Published %d records for %s/%s", count, symbol, interval)
    return count


def run(
    producer: CryptoProducer,
    symbols: list[str],
    intervals: list[str],
    days: int,
    rest_base: str,
) -> None:
    """Backfill all symbol/interval combinations.  Called by the entrypoint."""
    for symbol in symbols:
        for interval in intervals:
            fetch_and_publish(producer, symbol, interval, days, rest_base)
