"""Kafka trending.windows.{1m,5m} + trending.bots.5m -> OpenSearch daily indices.

- Flushes every BATCH_SIZE docs *or* FLUSH_SECONDS, whichever comes first.
- Commits Kafka offsets only after a successful bulk write (at-least-once;
  stable document ids make redelivery harmless).
- Failed documents are logged; transient bulk failures are retried.
"""

import os
import sys
import time

import ujson
from kafka import KafkaConsumer
from opensearchpy import OpenSearch, helpers

from docs import to_action

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPICS = os.getenv("INPUT_TOPICS", "trending.windows.1m,trending.windows.5m,trending.bots.5m").split(",")
OS_URL = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
INDEX_1M = os.getenv("INDEX_1M", "trending-1m")
INDEX_5M = os.getenv("INDEX_5M", "trending-5m")
INDEX_BOTS = os.getenv("INDEX_BOTS", "trending-bots-5m")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "2"))
GROUP_ID = os.getenv("GROUP_ID", "trending-indexer")


def log(msg: str) -> None:
    print(f"[trending-indexer] {msg}", file=sys.stderr, flush=True)


def flush(client: OpenSearch, buf: list[dict]) -> None:
    for attempt in range(1, 6):
        try:
            ok, errors = helpers.bulk(client, buf, raise_on_error=False, raise_on_exception=False)
            if errors:
                log(f"{len(errors)} docs rejected, first: {ujson.dumps(errors[0])[:300]}")
            return
        except Exception as e:
            log(f"bulk failed (attempt {attempt}): {e!r}")
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError("bulk indexing failed repeatedly")


def main() -> None:
    consumer = KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: ujson.loads(v.decode("utf-8")),
    )
    client = OpenSearch(OS_URL, timeout=30, max_retries=3, retry_on_timeout=True)
    log(f"consuming {INPUT_TOPICS} -> {OS_URL}")

    buf: list[dict] = []
    last_flush = time.monotonic()
    indexed = 0
    last_report = time.monotonic()
    while True:
        polled = consumer.poll(timeout_ms=500, max_records=BATCH_SIZE)
        for records in polled.values():
            for msg in records:
                if isinstance(msg.value, dict):
                    buf.append(to_action(msg.value, INDEX_1M, INDEX_5M, INDEX_BOTS))
        due = buf and (len(buf) >= BATCH_SIZE or time.monotonic() - last_flush >= FLUSH_SECONDS)
        if due:
            flush(client, buf)
            consumer.commit()
            indexed += len(buf)
            buf.clear()
            last_flush = time.monotonic()
        if time.monotonic() - last_report >= 60:
            log(f"indexed {indexed} docs in the last minute")
            indexed, last_report = 0, time.monotonic()


if __name__ == "__main__":
    main()
