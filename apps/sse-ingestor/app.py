import os, sys, time, json, random, signal
import requests
from sseclient import SSEClient
from kafka import KafkaProducer
from kafka.errors import KafkaError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import ujson

EVENT_URL = os.getenv("EVENTSTREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC     = os.getenv("KAFKA_TOPIC", "wikimedia.recentchange.raw")

USER_AGENT = os.getenv(
    "USER_AGENT",
    "wikimedia-trending-pipeline/1.0 (student project; contact=khalil.krifi@ensi-uma.tn)"
)

# timeouts can be tuned via env if needed
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "15"))   # seconds
READ_TIMEOUT    = float(os.getenv("READ_TIMEOUT", "300"))     # seconds; finite to detect dead sockets

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/event-stream",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    # 'identity' avoids gzip; simpler for chunked SSE
    "Accept-Encoding": "identity",
}

def new_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: ujson.dumps(v).encode("utf-8"),
        linger_ms=50,
        retries=5,
        acks=1,  # integer, not string
    )

def build_session():
    """
    Reuse TCP/TLS, add limited retries on initial GET (not on streaming read).
    """
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=5,
        connect=5,
        read=0,                   # do not retry mid-stream reads; we'll handle reconnect ourselves
        status=5,
        backoff_factor=0.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def is_interesting(rc: dict) -> bool:
    # Skip canaries as recommended by WMF docs
    meta = rc.get("meta") or {}
    if meta.get("domain") == "canary":
        return False
    if rc.get("type") != "edit":
        return False
    if not rc.get("title"):
        return False
    return True

def open_stream(session: requests.Session, last_id: str | None):
    """
    Open an SSE stream. If last_id is provided, resume from there.
    """
    headers = dict(HEADERS)
    if last_id:
        headers["Last-Event-ID"] = last_id  # EventStreams will resume based on id/timestamp
    resp = session.get(
        EVENT_URL,
        stream=True,
        headers=headers,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    resp.raise_for_status()
    return SSEClient(resp)

def main():
    session = build_session()
    producer = new_producer()

    stop = False
    def _stop(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    last_id = None         # remember across reconnects
    backoff = 1.0          # seconds
    MAX_BACKOFF = 120.0    # cap

    while not stop:
        try:
            client = open_stream(session, last_id)
            # if we connected, reset backoff
            backoff = 1.0

            for msg in client.events():
                if stop:
                    break
                # We only care about regular message events with data
                if not msg or not msg.data:
                    continue

                # Update resume pointer ASAP so even if parsing fails we can resume
                if msg.id:
                    last_id = msg.id

                try:
                    rc = json.loads(msg.data)
                except Exception:
                    # malformed JSON or keepalive weirdness: skip
                    continue

                if not is_interesting(rc):
                    continue

                out = {
                    "wiki": rc.get("wiki"),
                    "lang": (rc.get("server_name") or "").split(".")[0],
                    "title": rc.get("title"),
                    "rev_id": rc.get("rev_id"),
                    "user": rc.get("user"),
                    "minor": bool(rc.get("minor")),
                    "bot": bool(rc.get("bot")),
                    "timestamp": rc.get("timestamp"),  # epoch seconds
                    "comment": rc.get("comment", ""),
                    "length_new": (rc.get("length") or {}).get("new"),
                    "length_old": (rc.get("length") or {}).get("old"),
                    "is_revert": bool(rc.get("is_revert")),
                }

                try:
                    producer.send(TOPIC, out)
                except KafkaError as ke:
                    # Broker may have dropped; recreate producer and retry once
                    print(f"[sse-ingestor] kafka send error: {ke}; recreating producer...", file=sys.stderr, flush=True)
                    try:
                        producer.close(timeout=5)
                    except Exception:
                        pass
                    producer = new_producer()
                    producer.send(TOPIC, out)

        except (requests.RequestException, KafkaError, Exception) as e:
            # Any network / HTTP / Kafka error -> back off and reconnect with Last-Event-ID
            print(f"[sse-ingestor] stream error: {e}", file=sys.stderr, flush=True)
            time.sleep(backoff + random.random() * 0.3)
            backoff = min(backoff * 2.0, MAX_BACKOFF)

    # graceful shutdown
    try:
        producer.flush(5)
    finally:
        try:
            producer.close(5)
        except Exception:
            pass
        session.close()

if __name__ == "__main__":
    main()
