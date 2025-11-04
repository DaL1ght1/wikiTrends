# Project 1 — Wikimedia Trending Topics

This doc gives you a **ready-to-run `docker-compose.yml`** and the **file architecture** you’ll implement (with each component’s role). You can drop this into a clean folder and start wiring your code inside `apps/…`.

---

## 1) `docker-compose.yml`

> Minimal-but-complete stack: Kafka (KRaft, no ZooKeeper), Spark (master + worker), OpenSearch, OpenSearch Dashboards, Grafana, plus three app containers you will implement: `sse-ingestor` (SSE→Kafka), `stream-aggregator` (Kafka→Kafka windows), and `trending-indexer` (Kafka→OpenSearch). A one-off `baseline-batch` is provided behind a profile.

```yaml
name: BIG-DATA

services:
  # -----------------------------
  # Core Data Infrastructure
  # -----------------------------
  zookeeper:
      container_name: zookeeper
      image: zookeeper:latest
      ports:
        - "2181:2181"
      # Added environment variable to enable 4LW commands for healthcheck
      environment:
        ZOO_4LW_COMMANDS_WHITELIST: ruok, stat
      networks: [core]
      healthcheck:
        # More robust check using the 'ruok' command
        test: ["CMD", "bash", "-c", "echo ruok | nc localhost 2181 | grep imok"]
        interval: 10s
        timeout: 5s
        retries: 5
        start_period: 10s

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "29092:29092"     # host access
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      # internal for containers, external for host
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_RETENTION_HOURS: 72
    networks: [core]
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 12



  kafka-init:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka-init
    depends_on:
      kafka:
        condition: service_healthy
    volumes:
      - ./conf/kafka:/conf:ro
    entrypoint: ["bash","-lc","/conf/create-topics.sh"]
    networks: [core]
    restart: "no"


  opensearch:
    image: opensearchproject/opensearch:2.14.0
    container_name: opensearch
    environment:
      discovery.type: single-node
      DISABLE_SECURITY_PLUGIN: "true"
      OPENSEARCH_JAVA_OPTS: "-Xms1g -Xmx1g"
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
    ports:
      - "9200:9200"
    volumes:
      - opensearch-data:/usr/share/opensearch/data
      - ./conf/opensearch:/usr/share/opensearch/config-extra
    networks:
      - core
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:9200/_cluster/health || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10

  opensearch-dashboards:
    image: opensearchproject/opensearch-dashboards:2.14.0
    container_name: opensearch-dashboards
    depends_on:
      opensearch:
        condition: service_healthy
    environment:
      OPENSEARCH_HOSTS: '["http://opensearch:9200"]'
      DISABLE_SECURITY_DASHBOARDS_PLUGIN: "true"
    ports:
      - "5601:5601"
    networks:
      - core

  grafana:
    image: grafana/grafana:10.4.3
    container_name: grafana
    depends_on:
      opensearch:
        condition: service_healthy
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
    ports:
      - "3000:3000"
    volumes:
      - ./conf/grafana/provisioning:/etc/grafana/provisioning
      - ./conf/grafana/dashboards:/var/lib/grafana/dashboards
      - grafana-data:/var/lib/grafana
    networks:
      - core

  # -----------------------------
  # Spark Runtime
  # -----------------------------
  spark-master:
    image: apache/spark:3.5.3
    container_name: spark-master
    environment:
      SPARK_MODE: master
      SPARK_NO_DAEMONIZE: "true"
    ports:
      - "7077:7077"
      - "8080:8080"
    networks:
      - core
    # Add this command to start the master process in the foreground
    command: [ "/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master" ]
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:8080" ]
      interval: 10s
      timeout: 5s
      retries: 5

  spark-worker-1:
    image: apache/spark:3.5.3
    container_name: spark-worker-1
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 6G
      SPARK_WORKER_CORES: "3"
      SPARK_NO_DAEMONIZE: "true"
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
    depends_on:
      spark-master:
        condition: service_healthy
    networks:
      - core

  # -----------------------------
  # Hadoop (HDFS) + HBase Standalone
  # -----------------------------
  # Using bde2020 Hadoop images - more reliable and modular
  hadoop-namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: hadoop-namenode
    environment:
      CLUSTER_NAME: wikimedia-cluster
      CORE_CONF_fs_defaultFS: hdfs://hadoop-namenode:9000
      CORE_CONF_hadoop_http_staticuser_user: root
      HDFS_CONF_dfs_webhdfs_enabled: "true"
      HDFS_CONF_dfs_permissions_enabled: "false"
      HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check: "false"
    ports:
      - "9870:9870"    # NameNode Web UI
      - "9000:9000"    # HDFS
    volumes:
      - hadoop-namenode:/hadoop/dfs/name
    networks:
      - core
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9870"]
      interval: 10s
      timeout: 5s
      retries: 10

  hadoop-datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: hadoop-datanode
    environment:
      CORE_CONF_fs_defaultFS: hdfs://hadoop-namenode:9000
      CORE_CONF_hadoop_http_staticuser_user: root
      HDFS_CONF_dfs_webhdfs_enabled: "true"
      HDFS_CONF_dfs_permissions_enabled: "false"
    ports:
      - "9864:9864"    # DataNode Web UI
    volumes:
      - hadoop-datanode:/hadoop/dfs/data
    depends_on:
      hadoop-namenode:
        condition: service_healthy
    networks:
      - core

  # Using standalone HBase image with embedded components
  hbase:
    image: harisekhon/hbase:2.1
    container_name: hbase
    hostname: hbase
    environment:
      HBASE_MANAGES_ZK: "true"
    ports:
      - "16010:16010"  # Master UI
      - "16030:16030"  # RegionServer UI
      - "8085:8085"    # REST
      - "9090:9090"    # Thrift
    networks:
      - core
    # ✅ Use an endpoint that returns 200
    healthcheck:
      test: ["CMD-SHELL", "grep -q 'Registered as active master' /hbase/logs/hbase--master-hbase.log"]
      interval: 10s
      timeout: 5s
      retries: 40
      start_period: 90s


      # -----------------------------
  # Your Application Containers
  # -----------------------------
  sse-ingestor:
    build: ./apps/sse-ingestor
    container_name: sse-ingestor
    depends_on:
      kafka-init:
        condition: service_completed_successfully
    environment:
      EVENTSTREAM_URL: ${EVENTSTREAM_URL:-https://stream.wikimedia.org/v2/stream/recentchange}
      KAFKA_BOOTSTRAP: ${KAFKA_BOOTSTRAP:-kafka:9092}
      KAFKA_TOPIC: ${INPUT_TOPIC:-wikimedia.recentchange.raw}
      USER_AGENT: "Siwar-krifi-trending-pipeline/1.1 (contact=khalil.krifi@ensi-uma.tn)"
    networks:
      - core
    restart: unless-stopped


  stream-aggregator:
    build: ./apps/stream-aggregator
    container_name: stream-aggregator
    depends_on:
      kafka:
        condition: service_healthy
      spark-master:
        condition: service_healthy
      hbase:
        condition: service_healthy
    environment:
      KAFKA_BOOTSTRAP: ${KAFKA_BOOTSTRAP:-kafka:9092}
      INPUT_TOPIC: ${INPUT_TOPIC:-wikimedia.recentchange.raw}
      CLEAN_TOPIC: ${CLEAN_TOPIC:-wikimedia.recentchange.clean}
      TREND_TOPIC_1M: ${TREND_TOPIC_1M:-trending.windows.1m}
      TREND_TOPIC_5M: ${TREND_TOPIC_5M:-trending.windows.5m}
      HBASE_REST_URL: http://hbase:8085
      CHECKPOINT_DIR: /checkpoints/stream-agg
    volumes:
      - spark-checkpoints:/checkpoints
      - ivy-cache:/root/.ivy2
    networks:
      - core
    restart: unless-stopped

  trending-indexer:
    build: ./apps/trending-indexer
    container_name: trending-indexer
    depends_on:
      kafka:
        condition: service_healthy
      opensearch:
        condition: service_healthy
    environment:
      KAFKA_BOOTSTRAP: ${KAFKA_BOOTSTRAP:-kafka:9092}
      INPUT_TOPICS: ${INPUT_TOPICS:-trending.windows.1m,trending.windows.5m}
      OPENSEARCH_URL: ${OPENSEARCH_URL:-http://opensearch:9200}
      INDEX_1M: ${INDEX_1M:-trending-1m}
      INDEX_5M: ${INDEX_5M:-trending-5m}


    networks:
      - core
    restart: unless-stopped

  baseline-batch:
    profiles:
      - batch
    build: ./apps/baseline-batch
    container_name: baseline-batch
    depends_on:
      spark-master:
        condition: service_healthy
      hbase:
        condition: service_healthy
    environment:
      AQS_BASE: ${AQS_BASE:-https://wikimedia.org/api/rest_v1/metrics/pageviews}
      LAKE_ROOT: /lake
      SPARK_MASTER_URL: spark://spark-master:7077
    volumes:
      - lake:/lake
    networks:
      - core

  hbase-writer:
    build: ./apps/hbase-writer
    depends_on:
      kafka:
        condition: service_healthy
      hbase:
        condition: service_healthy
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      INPUT_TOPICS: trending.windows.1m,trending.windows.5m
      HBASE_REST_URL: http://hbase:8085
      HBASE_TABLE: rc_clean_minute
    networks: [ core ]
    restart: unless-stopped


networks:
  core:
    driver: bridge

volumes:
  kafka-data:
  opensearch-data:
  spark-checkpoints:
  lake:
  grafana-data:
  hadoop-namenode:
  hadoop-datanode:
  ivy-cache:
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
