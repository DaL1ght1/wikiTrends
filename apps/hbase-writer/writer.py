"""Kafka trending.windows.{1m,5m} -> HBase table (long-term page history).

Uses the HBase Thrift API (port 9090) through happybase. The previous REST
approach failed because port 8085 on the HBase image is the REST server's
*info UI*, not the REST API, hence the HTTP 405s.

Creates the table on startup if missing, writes in batches and commits Kafka
offsets only after a batch is stored.
"""

import os
import sys
import time

import happybase
import ujson
from kafka import KafkaConsumer

from rows import cells, row_key

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPICS = os.getenv("INPUT_TOPICS", "trending.windows.1m,trending.windows.5m").split(",")
HBASE_HOST = os.getenv("HBASE_THRIFT_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
TABLE = os.getenv("HBASE_TABLE", "trending_windows")
TTL_SECONDS = int(os.getenv("HBASE_TTL_SECONDS", str(90 * 24 * 3600)))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))


def log(msg: str) -> None:
    print(f"[hbase-writer] {msg}", file=sys.stderr, flush=True)


def connect() -> happybase.Connection:
    for attempt in range(1, 31):
        try:
            conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=30000, autoconnect=True)
            existing = {t.decode() for t in conn.tables()}
            if TABLE not in existing:
                conn.create_table(TABLE, {
                    "m": dict(max_versions=1, time_to_live=TTL_SECONDS, bloom_filter_type="ROW"),
                    "i": dict(max_versions=1, time_to_live=TTL_SECONDS),
                })
                log(f"created table {TABLE}")
            return conn
        except Exception as e:
            log(f"HBase not ready ({e!r}), retry {attempt}/30")
            time.sleep(5)
    raise RuntimeError("could not connect to HBase Thrift")


def rewind(consumer: KafkaConsumer) -> None:
    """Seek every assigned partition back to its last committed offset."""
    for tp in consumer.assignment():
        committed = consumer.committed(tp)
        if committed is not None:
            consumer.seek(tp, committed)


def main() -> None:
    conn = connect()
    table = conn.table(TABLE)
    consumer = KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=BOOTSTRAP,
        group_id=os.getenv("GROUP_ID", "hbase-writer"),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: ujson.loads(v.decode("utf-8")),
    )
    log(f"consuming {INPUT_TOPICS} -> hbase table {TABLE}")
    written, last_report = 0, time.monotonic()
    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
        n = 0
        if polled:
            try:
                with table.batch(batch_size=BATCH_SIZE) as b:
                    for records in polled.values():
                        for msg in records:
                            if isinstance(msg.value, dict) and msg.value.get("ts_bucket"):
                                b.put(row_key(msg.value), cells(msg.value))
                                n += 1
                consumer.commit()
                written += n
            except Exception as e:
                # Reconnect and let Kafka redeliver the uncommitted batch.
                log(f"write failed: {e!r}; reconnecting")
                conn = connect()
                table = conn.table(TABLE)
                rewind(consumer)
        if time.monotonic() - last_report >= 60:
            log(f"wrote {written} rows in the last minute")
            written, last_report = 0, time.monotonic()


if __name__ == "__main__":
    main()
