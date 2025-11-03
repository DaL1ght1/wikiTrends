# Project 1 — Wikimedia Trending Topics

This doc gives you a **ready-to-run `docker-compose.yml`** and the **file architecture** you’ll implement (with each component’s role). You can drop this into a clean folder and start wiring your code inside `apps/…`.

---

## 1) `docker-compose.yml`

> Minimal-but-complete stack: Kafka (KRaft, no ZooKeeper), Spark (master + worker), OpenSearch, OpenSearch Dashboards, Grafana, plus three app containers you will implement: `sse-ingestor` (SSE→Kafka), `stream-aggregator` (Kafka→Kafka windows), and `trending-indexer` (Kafka→OpenSearch). A one-off `baseline-batch` is provided behind a profile.

```yaml
version: "3.9"

services:
  # -------------------------------------------------------
  # Core Data Infrastructure
  # -------------------------------------------------------
  kafka:
    image: bitnami/kafka:3.7
    container_name: kafka
    ports:
      - "29092:29092"   # Host access to Kafka
    environment:
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,PLAINTEXT_HOST://:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=false
      - KAFKA_CFG_LOG_RETENTION_HOURS=72
    volumes:
      - kafka-data:/bitnami/kafka
    networks: [core]

  kafka-init:
    image: bitnami/kafka:3.7
    container_name: kafka-init
    depends_on:
      - kafka
    entrypoint: ["/bin/bash","/scripts/create-topics.sh"]
    volumes:
      - ./conf/kafka:/scripts
    networks: [core]

  opensearch:
    image: opensearchproject/opensearch:2.14.0
    container_name: opensearch
    environment:
      - discovery.type=single-node
      - DISABLE_SECURITY_PLUGIN=true   # dev only
      - OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g
    ulimits:
      memlock:
        soft: -1
        hard: -1
    ports:
      - "9200:9200"
    volumes:
      - opensearch-data:/usr/share/opensearch/data
      - ./conf/opensearch:/usr/share/opensearch/config-extra
    networks: [core]

  dashboards:
    image: opensearchproject/opensearch-dashboards:2.14.0
    container_name: opensearch-dashboards
    depends_on:
      - opensearch
    environment:
      - OPENSEARCH_HOSTS=["http://opensearch:9200"]
      - DISABLE_SECURITY_DASHBOARDS_PLUGIN=true
    ports:
      - "5601:5601"
    networks: [core]

  grafana:
    image: grafana/grafana:10.4.3
    container_name: grafana
    depends_on:
      - opensearch
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
    ports:
      - "3000:3000"
    volumes:
      - ./conf/grafana/provisioning:/etc/grafana/provisioning
      - ./conf/grafana/dashboards:/var/lib/grafana/dashboards
    networks: [core]

  # -------------------------------------------------------
  # Spark Runtime (for streaming + batch)
  # -------------------------------------------------------
  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_NO_DAEMONIZE=true
    ports:
      - "7077:7077"  # Spark cluster
      - "8080:8080"  # Spark master UI
    networks: [core]

  spark-worker-1:
    image: bitnami/spark:3.5
    container_name: spark-worker-1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=4G
      - SPARK_WORKER_CORES=2
      - SPARK_NO_DAEMONIZE=true
    depends_on:
      - spark-master
    networks: [core]

  # -------------------------------------------------------
  # Your Application Containers (you implement)
  # -------------------------------------------------------
  sse-ingestor:
    build: ./apps/sse-ingestor
    container_name: sse-ingestor
    depends_on:
      - kafka
    environment:
      - EVENTSTREAM_URL=https://stream.wikimedia.org/v2/stream/recentchange
      - KAFKA_BOOTSTRAP=kafka:9092
      - KAFKA_TOPIC=wikimedia.recentchange.raw
    networks: [core]

  stream-aggregator:
    build: ./apps/stream-aggregator
    container_name: stream-aggregator
    depends_on:
      - kafka
      - spark-master
      - spark-worker-1
    environment:
      - KAFKA_BOOTSTRAP=kafka:9092
      - INPUT_TOPIC=wikimedia.recentchange.raw
      - CLEAN_TOPIC=wikimedia.recentchange.clean
      - TREND_TOPIC_1M=trending.windows.1m
      - TREND_TOPIC_5M=trending.windows.5m
      - CHECKPOINT_DIR=/checkpoints/stream-aggregator
    volumes:
      - spark-checkpoints:/checkpoints
    networks: [core]

  trending-indexer:
    build: ./apps/trending-indexer
    container_name: trending-indexer
    depends_on:
      - kafka
      - opensearch
    environment:
      - KAFKA_BOOTSTRAP=kafka:9092
      - INPUT_TOPICS=trending.windows.1m,trending.windows.5m
      - OPENSEARCH_URL=http://opensearch:9200
      - INDEX_1M=trending-1m
      - INDEX_5M=trending-5m
    networks: [core]

  # One-off batch job (run with: `docker compose --profile batch up baseline-batch --build`)
  baseline-batch:
    profiles: ["batch"]
    build: ./apps/baseline-batch
    container_name: baseline-batch
    depends_on:
      - spark-master
      - spark-worker-1
    environment:
      - AQS_BASE=https://wikimedia.org/api/rest_v1/metrics/pageviews
      - LAKE_ROOT=/lake
    volumes:
      - lake:/lake
    networks: [core]

networks:
  core:
    driver: bridge

volumes:
  kafka-data:
  opensearch-data:
  spark-checkpoints:
  lake:
```

### Topic bootstrap script (mounted into `kafka-init`)

Create `conf/kafka/create-topics.sh` (executable) to create the topics you’ll use:

```bash
#!/usr/bin/env bash
set -euo pipefail
KAFKA_BIN="/opt/bitnami/kafka/bin/kafka-topics.sh"
BOOTSTRAP="kafka:9092"

# Wait for Kafka
until /opt/bitnami/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe >/dev/null 2>&1; do
  echo "Waiting for Kafka…"; sleep 2;
endonecho

echo "Creating topics…"
$KAFKA_BIN --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic wikimedia.recentchange.raw --partitions 3 --replication-factor 1
$KAFKA_BIN --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic wikimedia.recentchange.clean --partitions 3 --replication-factor 1
$KAFKA_BIN --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic trending.windows.1m --partitions 3 --replication-factor 1
$KAFKA_BIN --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic trending.windows.5m --partitions 3 --replication-factor 1

echo "Topics ready."
```

> **Note:** keep `PLAINTEXT` for a class/demo. For production, secure Kafka and OpenSearch.

---

## 2) File/Folder Architecture (what you must code and why)

```
.
├─ docker-compose.yml
├─ .env                         # optional: centralize env overrides
├─ conf/
│  ├─ kafka/
│  │  └─ create-topics.sh      # creates the 4 topics used in the pipeline
│  ├─ opensearch/
│  │  ├─ index-templates/
│  │  │  ├─ trending-1m.json   # template/mappings for 1m buckets (you provide)
│  │  │  └─ trending-5m.json   # template/mappings for 5m buckets (you provide)
│  │  └─ pipelines/            # optional ingest pipelines (e.g., timestamp parsing)
│  └─ grafana/
│     ├─ provisioning/
│     │  ├─ datasources/
│     │  │  └─ opensearch.yml  # points Grafana → OpenSearch
│     │  └─ dashboards/
│     │     └─ dashboards.yml  # auto-loads dashboards from ../dashboards
│     └─ dashboards/
│        └─ trending.json      # ready-made dashboard export (you create)
│
├─ apps/
│  ├─ sse-ingestor/
│  │  ├─ Dockerfile            # tiny image (python:slim). Installs deps: requests/sseclient, confluent-kafka or kafka-python
│  │  ├─ app.py                # **YOU**: consume Wikimedia SSE → produce to Kafka topic `wikimedia.recentchange.raw`
│  │  └─ config.yml            # optional: filters (include/exclude wikis, namespaces, bot/minor flags)
│  │
│  ├─ stream-aggregator/
│  │  ├─ Dockerfile            # based on bitnami/spark or openjdk/python with spark-submit
│  │  ├─ main.py               # **YOU**: Spark Structured Streaming job
│  │  │                        #   - reads `wikimedia.recentchange.raw`
│  │  │                        #   - filters (drop bots/minor if desired)
│  │  │                        #   - windowed agg (1m, 5m) by (wiki/title)
│  │  │                        #   - computes metrics (edits, unique editors, abs bytes, entropy, etc.)
│  │  │                        #   - computes z-scores vs. rolling/batch baseline (optional)
│  │  │                        #   - writes to `wikimedia.recentchange.clean` + `trending.windows.{1m,5m}`
│  │  ├─ requirements.txt      # if using pyspark helpers; otherwise package with the image
│  │  └─ submit.sh             # convenience script to run spark-submit inside the container
│  │
│  ├─ trending-indexer/
│  │  ├─ Dockerfile            # python:slim or node:alpine
│  │  ├─ indexer.py            # **YOU**: Kafka consumer → transform → index docs to OpenSearch (bulk API)
│  │  └─ mappings/             # optional: keep index mappings/settings versions here
│  │
│  └─ baseline-batch/
│     ├─ Dockerfile            # spark job image for batch baseline builds
│     ├─ job.py                # **YOU**: pulls AQS pageviews + historical Parquet → compute μ/σ per (wiki,title,hod,dow)
│     └─ requirements.txt
│
├─ scripts/
│  ├─ create_indices.sh        # bootstrap OpenSearch indices & templates
│  ├─ seed_baselines.sh        # run the first baseline-batch job
│  └─ wait-for.sh              # generic wait helper (optional)
│
├─ lake/                       # mounted volume for Parquet (batch layer)
├─ data/                       # if you want to store small samples locally
└─ README.md                   # quickstart & runbook for your graders
```

### Component roles (at a glance)

* **`sse-ingestor/app.py`** — Connects to `https://stream.wikimedia.org/v2/stream/recentchange` (SSE), parses JSON events, applies lightweight hygiene (timestamps, nulls, keep fields you need), and produces to Kafka `wikimedia.recentchange.raw` with keys like `(wiki|title)` or `(wiki|rev_id)`.
* **`stream-aggregator/main.py`** — Spark Structured Streaming job: reads raw topic, optional filters (`bot`, `minor`, namespaces), computes **windowed aggregates** (1m/5m sliding) per `(wiki,title)`; calculates metrics (edit_count, unique_editors, sum_abs_delta, editor_entropy, revert_count) and a **trend score** (z-score / composite). Writes results to `trending.windows.{1m,5m}` and a cleaned stream to `wikimedia.recentchange.clean`.
* **`trending-indexer/indexer.py`** — Lightweight consumer that bulk-indexes each window’s top-N to OpenSearch indices `trending-1m`, `trending-5m` with fields you’ll visualize (scores, metrics JSON, page URL, language, ts_bucket).
* **`baseline-batch/job.py`** — Batch job (run daily or on demand) that reads historical Parquet from `/lake`, calls AQS pageviews for context, and writes a **baseline table** (μ/σ per `(wiki,title, hour-of-day, weekday)`) back to `/lake`. Your streaming job can join/lookup these stats.
* **`conf/opensearch/index-templates/*.json`** — Mappings/settings for indices (number/date formats, keyword vs text, refresh interval, ILM if you want).
* **`conf/grafana/dashboards/trending.json`** — Prebuilt dashboard (Top-20 global & per-lang; sparklines for the last 30–60 min; per-metric panels).

---

## 3) How to run (quick start)

1. **Make scripts executable**

   ```bash
   chmod +x conf/kafka/create-topics.sh scripts/*.sh
   ```
2. **Start the core + apps**

   ```bash
   docker compose up -d --build kafka opensearch dashboards grafana spark-master spark-worker-1 kafka-init
   docker compose up -d --build sse-ingestor stream-aggregator trending-indexer
   ```
3. **(Optional) Initialize indices & dashboards**

   ```bash
   ./scripts/create_indices.sh
   ```
4. **(Optional) Generate baselines**

   ```bash
   docker compose --profile batch up --build baseline-batch
   ```

### Health UIs

* Kafka not directly exposed (you can `kcat` to `localhost:29092`).
* Spark Master UI: [http://localhost:8080](http://localhost:8080)
* OpenSearch: [http://localhost:9200](http://localhost:9200)
* Dashboards: [http://localhost:5601](http://localhost:5601)
* Grafana: [http://localhost:3000](http://localhost:3000)

---

## 4) Environment knobs (tune for your demo)

* **Kafka partitions**: start with 3; bump to 6 if you see lag.
* **Spark**: worker cores/memory set in compose; adjust to match your laptop/VM.
* **OpenSearch**: for faster UI, set index `refresh_interval` to `5-10s` and map numerics properly (scores as `float`).
* **Latency target**: end-to-end < 60s with 10s slide windows is very achievable.

---

## 5) Deliverables mapping → your report

* **Collecte temps réel**: `sse-ingestor` + Kafka
* **Traitement temps réel**: `stream-aggregator` (fenêtrage + score)
* **Traitement batch**: `baseline-batch` (μ/σ & pageviews)
* **SGBD NoSQL**: OpenSearch (serving) + Parquet in `/lake` (historique)
* **Démo**: Dashboards live + Spark UI + quick exploration of indexed docs

> Ping me when you want the OpenSearch index template skeletons or a Grafana dashboard JSON starter—happy to add them.
