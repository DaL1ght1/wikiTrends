"""Wikimedia EventStreams (SSE) -> Kafka `wikimedia.recentchange.raw`.

- Resumes from the last event id after reconnects *and* container restarts
  (the id is persisted to STATE_FILE).
- Keys messages by `wiki|title` so a page's events stay ordered in one partition.
- Reports delivery failures through producer callbacks (send() is async).
"""

import json
import os
import random
import signal
import sys
import time

import requests
import ujson
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from sseclient import SSEClient
from urllib3.util.retry import Retry

from events import Filters, is_interesting, record_key, to_record

EVENT_URL = os.getenv("EVENTSTREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wikimedia.recentchange.raw")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config.yml")
STATE_FILE = os.getenv("STATE_FILE", "/state/last_event_id")
USER_AGENT = os.getenv("USER_AGENT", "wikitrends-pipeline/2.0 (https://github.com/DaL1ght1/wikiTrends)")
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "15"))
READ_TIMEOUT = float(os.getenv("READ_TIMEOUT", "300"))  # finite, to detect dead sockets
STATS_EVERY = float(os.getenv("STATS_EVERY_SECONDS", "60"))

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/event-stream",
    "Cache-Control": "no-cache",
    "Accept-Encoding": "identity",
}


def log(msg: str) -> None:
    print(f"[sse-ingestor] {msg}", file=sys.stderr, flush=True)


class Stats:
    def __init__(self):
        self.seen = self.sent = self.failed = self.skipped = 0
        self.t0 = time.monotonic()

    def maybe_report(self):
        dt = time.monotonic() - self.t0
        if dt >= STATS_EVERY:
            log(
                f"last {dt:.0f}s: seen={self.seen} sent={self.sent} skipped={self.skipped} "
                f"failed={self.failed} rate={self.sent / dt:.1f}/s"
            )
            self.__init__()


def new_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k,
        value_serializer=lambda v: ujson.dumps(v).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=50,
        compression_type="gzip",
        max_in_flight_requests_per_connection=1,  # keep per-partition order on retries
    )


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=5, connect=5, read=0, status=5, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504), allowed_methods=frozenset({"GET"}),
        raise_on_status=False, respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def load_last_id() -> str | None:
    try:
        with open(STATE_FILE, encoding="utf-8") as fh:
            return fh.read().strip() or None
    except OSError:
        return None


def save_last_id(last_id: str) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(last_id)
        os.replace(tmp, STATE_FILE)
    except OSError as e:
        log(f"could not persist last event id: {e}")


def open_stream(session: requests.Session, last_id: str | None) -> SSEClient:
    headers = dict(HEADERS)
    if last_id:
        headers["Last-Event-ID"] = last_id
    resp = session.get(EVENT_URL, stream=True, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    return SSEClient(resp)


def main() -> None:
    filters = Filters.from_yaml(CONFIG_PATH)
    log(f"filters: types={filters.types} include={len(filters.include_wikis)} "
        f"exclude={len(filters.exclude_wikis)} namespaces={filters.namespaces}")
    session = build_session()
    producer = new_producer()
    stats = Stats()

    def on_error(exc):
        stats.failed += 1
        log(f"kafka delivery failed: {exc}")

    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    last_id = load_last_id()
    if last_id:
        log("resuming from persisted event id")
    backoff, max_backoff = 1.0, 120.0
    since_save = 0

    while not stop:
        try:
            client = open_stream(session, last_id)
            backoff = 1.0
            log(f"connected to {EVENT_URL}")
            for msg in client.events():
                if stop:
                    break
                if not msg or not msg.data:
                    continue
                if msg.id:
                    last_id = msg.id
                stats.seen += 1
                try:
                    rc = json.loads(msg.data)
                except ValueError:
                    stats.skipped += 1
                    continue
                if not is_interesting(rc, filters):
                    stats.skipped += 1
                    continue
                rec = to_record(rc)
                producer.send(TOPIC, key=record_key(rec), value=rec).add_errback(on_error)
                stats.sent += 1
                since_save += 1
                if since_save >= 200 and last_id:
                    save_last_id(last_id)
                    since_save = 0
                stats.maybe_report()
        except Exception as e:  # network / HTTP / parse errors -> reconnect
            log(f"stream error: {e!r}; reconnecting in {backoff:.0f}s")
            time.sleep(backoff + random.random() * 0.3)
            backoff = min(backoff * 2.0, max_backoff)

    log("shutting down")
    if last_id:
        save_last_id(last_id)
    producer.flush(10)
    producer.close(10)
    session.close()


if __name__ == "__main__":
    main()
