# 📊 Wikimedia Trending Topics — Real-Time Analytics Pipeline

<div align="center">

![Status](https://img.shields.io/badge/Status-Operational-success?style=for-the-badge)
![Kafka](https://img.shields.io/badge/Kafka-7.6.1-black?style=for-the-badge&logo=apache-kafka)
![Spark](https://img.shields.io/badge/Spark-3.5.3-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![OpenSearch](https://img.shields.io/badge/OpenSearch-2.14.0-005EB8?style=for-the-badge&logo=opensearch)

**A production-ready streaming analytics system tracking trending Wikipedia pages in real-time**

[Features](#-features) • [Quick Start](#-quick-start) • [Architecture](#-architecture) • [Dashboards](#-dashboards) • [Documentation](#-documentation)

</div>

---

## 🎯 Overview

This project implements a **Kappa Architecture** streaming pipeline that processes live Wikipedia edit events from Wikimedia's event stream, computes real-time trending scores using windowed aggregations, and visualizes the results through interactive dashboards.

**Key Capabilities:**
- 🔴 **Real-time ingestion** from Wikimedia SSE stream (100s of events/second)
- ⚡ **Sub-60 second latency** from event to visualization
- 📈 **Windowed analytics** with 1-minute and 5-minute trending windows
- 🔍 **Full-text search** and aggregations on trending pages
- 📊 **Live dashboards** with Grafana and OpenSearch
- 🛡️ **Fault-tolerant** processing with HDFS checkpoints

---

## 1) File/Folder Architecture (what you must code and why)

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

## 2) Component roles (at a glance)

* **`sse-ingestor/app.py`** — Connects to `https://stream.wikimedia.org/v2/stream/recentchange` (SSE), parses JSON events, applies lightweight hygiene (timestamps, nulls, keep fields you need), and produces to Kafka `wikimedia.recentchange.raw` with keys like `(wiki|title)` or `(wiki|rev_id)`.
* **`stream-aggregator/main.py`** — Spark Structured Streaming job: reads raw topic, optional filters (`bot`, `minor`, namespaces), computes **windowed aggregates** (1m/5m sliding) per `(wiki,title)`; calculates metrics (edit_count, unique_editors, sum_abs_delta, editor_entropy, revert_count) and a **trend score** (z-score / composite). Writes results to `trending.windows.{1m,5m}` and a cleaned stream to `wikimedia.recentchange.clean`.
* **`trending-indexer/indexer.py`** — Lightweight consumer that bulk-indexes each window’s top-N to OpenSearch indices `trending-1m`, `trending-5m` with fields you’ll visualize (scores, metrics JSON, page URL, language, ts_bucket).
* **`baseline-batch/job.py`** — Batch job (run daily or on demand) that reads historical Parquet from `/lake`, calls AQS pageviews for context, and writes a **baseline table** (μ/σ per `(wiki,title, hour-of-day, weekday)`) back to `/lake`. Your streaming job can join/lookup these stats.
* **`conf/opensearch/index-templates/*.json`** — Mappings/settings for indices (number/date formats, keyword vs text, refresh interval, ILM if you want).
* **`conf/grafana/dashboards/trending.json`** — Prebuilt dashboard (Top-20 global & per-lang; sparklines for the last 30–60 min; per-metric panels).

---

## 3) Quick Start

### Prerequisites
- Docker Desktop (Windows/Mac) or Docker Engine + Docker Compose (Linux)
- Minimum 8GB RAM available for containers
- Ports available: 3000, 5601, 7077, 8080, 8085, 9000, 9092, 9200, 9870, 16010

### Start the Pipeline

1. **Clone and navigate to the project:**
   ```bash
   cd Trandings
   ```

2. **Start all services:**
   ```bash
   docker compose up -d --build
   ```
   
   This will start:
   - Infrastructure: Kafka, ZooKeeper, Spark (master + worker), HDFS, HBase, OpenSearch
   - Applications: sse-ingestor, stream-aggregator, trending-indexer
   - Dashboards: Grafana, OpenSearch Dashboards

3. **Wait for services to be healthy (~2-3 minutes):**
   ```bash
   docker compose ps
   ```
   All services should show "Up" or "Healthy" status.

4. **Verify data is flowing:**
   ```bash
   # Check raw events from Wikimedia
   docker compose exec kafka bash -lc 'kafka-console-consumer --bootstrap-server kafka:9092 --topic wikimedia.recentchange.raw --from-beginning --max-messages 5'
   
   # Check windowed aggregates
   docker compose exec kafka bash -lc 'kafka-console-consumer --bootstrap-server kafka:9092 --topic trending.windows.1m --from-beginning --max-messages 3'
   
   # Check indexed data
   curl -s "http://localhost:9200/_cat/indices/trending-*?v"
   ```

5. **Access dashboards:**
   - **Grafana:** http://localhost:3000 → Dashboards → Trending → Wikimedia Trending Topics
   - **OpenSearch Dashboards:** http://localhost:5601 (requires index pattern setup)

### Stop the Pipeline
```bash
docker compose down
```

To remove all data:
```bash
docker compose down -v
```

### Monitoring & Admin UIs

| Service | URL | Purpose |
|---------|-----|----------|
| **Grafana** | http://localhost:3000 | Real-time trending dashboards |
| **OpenSearch Dashboards** | http://localhost:5601 | Data exploration & custom visualizations |
| **Spark Master UI** | http://localhost:8080 | Spark cluster status & running jobs |
| **HDFS NameNode UI** | http://localhost:9870 | HDFS cluster status |
| **HBase Master UI** | http://localhost:16010 | HBase cluster status |
| **OpenSearch API** | http://localhost:9200 | Direct API access |
| **Kafka** | localhost:29092 | Kafka broker (use `kcat` or similar tools) |

---

## 4) Implementation Status & Known Issues

### ✅ Working Components (75%)

#### 1. sse-ingestor ✅ FULLY FUNCTIONAL
- **Status:** Operational
- **Function:** Ingests live Wikipedia edit events from Wikimedia SSE stream
- **Output:** Produces to `wikimedia.recentchange.raw` Kafka topic
- **Performance:** Real-time streaming, no lag

#### 2. stream-aggregator ✅ FULLY FUNCTIONAL  
- **Status:** Operational (after HDFS checkpoint fix)
- **Function:** Spark Structured Streaming job computing windowed aggregations
- **Outputs:** 
  - `wikimedia.recentchange.clean` - Enriched events with bytes_delta
  - `trending.windows.1m` - 1-minute windowed aggregates
  - `trending.windows.5m` - 5-minute windowed aggregates
- **Metrics Computed:**
  - Trending score (composite: edits + editors + bytes - bots)
  - Edit count, unique editors, bytes changed
  - Bot/minor edit ratios, revert counts
- **Fix Applied:** Changed checkpoint storage from local Docker volume to HDFS for distributed state management

#### 3. trending-indexer ✅ FULLY FUNCTIONAL
- **Status:** Operational (after Dockerfile fix)
- **Function:** Kafka consumer indexing trending data to OpenSearch
- **Indices:** `trending-1m` (5.4k docs), `trending-5m` (14k docs)
- **Features:** Deduplication, URL generation, batch indexing
- **Fix Applied:** Corrected Dockerfile to use Python image instead of Spark, fixed entry point

### ⚠️ Partial Components (25%)

#### 4. hbase-writer ⚠️ PARTIAL - API COMPATIBILITY ISSUE
- **Status:** Container running but writes failing
- **Issue:** HBase REST API (harisekhon/hbase:2.1) doesn't support expected JSON format
- **Error:** HTTP 405 (Method Not Allowed) on PUT/POST operations
- **Impact:** Non-critical - OpenSearch serves as primary storage
- **Workaround Options:**
  - Use HBase Thrift API instead of REST
  - Replace with compatible HBase image
  - Use alternative storage (Cassandra, ScyllaDB)
  - Skip HBase (OpenSearch sufficient for analytics)

### 📝 Key Fixes Applied

1. **Spark Checkpoint Storage (stream-aggregator)**
   - **Problem:** State stores on local volume unreachable by Spark workers
   - **Solution:** Moved checkpoints to HDFS (`hdfs://hadoop-namenode:9000/checkpoints/stream-agg`)
   - **Files Changed:** 
     - `apps/stream-aggregator/main.py`
     - `docker-compose.yml` (added hadoop-namenode dependency)

2. **Trending Indexer Dockerfile (trending-indexer)**
   - **Problem:** Wrong base image and entry point file name
   - **Solution:** Use Python slim image, correct entry point to `indexer.py`
   - **File Changed:** `apps/trending-indexer/Dockerfile`

3. **Grafana OpenSearch Plugin**
   - **Problem:** Plugin not installed by default
   - **Solution:** Install `grafana-opensearch-datasource` plugin on first run
   - **File Changed:** `conf/grafana/provisioning/datasources/opensearch.yml`

### 📚 Documentation

See these files for detailed information:
- **PIPELINE_TEST_RESULTS.md** - Complete testing results and verification
- **DASHBOARD_ACCESS_GUIDE.md** - Dashboard setup and usage instructions  
- **STREAM_AGGREGATOR_TEST_RESULTS.md** - Technical details on stream-aggregator fixes

---

## 5) Troubleshooting

### No Data in Dashboards

1. **Check if data is being ingested:**
   ```bash
   docker compose logs --tail=20 sse-ingestor
   docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic wikimedia.recentchange.raw --max-messages 5
   ```

2. **Check if stream-aggregator is processing:**
   ```bash
   docker compose logs --tail=50 stream-aggregator | grep -i error
   docker compose ps stream-aggregator
   ```

3. **Check if indexing is working:**
   ```bash
   docker compose logs --tail=20 trending-indexer
   curl "http://localhost:9200/_cat/indices/trending-*?v"
   ```

4. **Verify time range in dashboards:**
   - Set time range to "Last 1 hour" or "Last 15 minutes"
   - Check latest data timestamp:
     ```bash
     curl -s "http://localhost:9200/trending-1m/_search?size=1&sort=ts_bucket:desc" | jq '.hits.hits[0]._source.ts_bucket'
     ```

### Container Restart Loops

1. **Check container logs:**
   ```bash
   docker compose logs <container-name>
   ```

2. **Check resource usage:**
   ```bash
   docker stats
   ```
   Ensure you have at least 8GB RAM available.

3. **Restart specific service:**
   ```bash
   docker compose restart <service-name>
   ```

### Grafana Dashboard Not Loading

1. **Install OpenSearch plugin (if needed):**
   ```bash
   docker compose exec grafana grafana-cli plugins install grafana-opensearch-datasource
   docker compose restart grafana
   ```

2. **Check datasource connection:**
   - Go to: Configuration → Data Sources → OpenSearch
   - Click "Test" - should show "Data source is working"

3. **Verify dashboard exists:**
   - Navigate to: Dashboards → Browse
   - Look for "Wikimedia Trending Topics" in the Trending folder

### Performance Issues

1. **Reduce Spark parallelism (if low on resources):**
   Edit `docker-compose.yml`:
   ```yaml
   spark-worker-1:
     environment:
       SPARK_WORKER_MEMORY: 4G  # Reduce from 6G
       SPARK_WORKER_CORES: "2"  # Reduce from 3
   ```

2. **Increase Kafka consumer fetch size:**
   In consumer code, increase `max.poll.records` or batch size.

3. **Monitor component health:**
   ```bash
   # Spark jobs
   open http://localhost:8080
   
   # OpenSearch cluster health
   curl "http://localhost:9200/_cluster/health?pretty"
   
   # Kafka consumer lag
   docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --all-groups
   ```

---

## 6) Architecture & Data Flow

```
Wikimedia SSE Stream
        ↓
   sse-ingestor ✅
        ↓
  [Kafka] wikimedia.recentchange.raw
        ↓
  stream-aggregator ✅
   (Spark Streaming)
        ↓ ↓ ↓
        ↓ ↓ └→ [Kafka] trending.windows.5m
        ↓ └──→ [Kafka] trending.windows.1m  
        └────→ [Kafka] wikimedia.recentchange.clean
             ↓ ↓
             ↓ └─────────────┐
             ↓               ↓
      trending-indexer ✅  hbase-writer ⚠️
             ↓               ↓
        [OpenSearch]    [HBase - API issue]
     trending-1m: 5.4k docs
     trending-5m: 14k docs
             ↓
        ┌────┴────┐
        ↓         ↓
    Grafana  OpenSearch
  (Dashboard) Dashboards
```

### Technologies Used

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|----------|
| **Streaming** | Apache Kafka | 7.6.1 | Event streaming platform |
| **Processing** | Apache Spark | 3.5.3 | Distributed stream processing |
| **Storage** | OpenSearch | 2.14.0 | Search & analytics engine |
| **Storage** | HDFS | 3.2.1 | Distributed file system (checkpoints) |
| **Storage** | HBase | 2.1 | NoSQL database (partial) |
| **Visualization** | Grafana | 10.4.3 | Dashboarding |
| **Visualization** | OpenSearch Dashboards | 2.14.0 | Data exploration |
| **Coordination** | ZooKeeper | latest | Service coordination |

---

## 7) Environment knobs (tune for your demo)

* **Kafka partitions**: start with 3; bump to 6 if you see lag.
* **Spark**: worker cores/memory set in compose; adjust to match your laptop/VM.
* **OpenSearch**: for faster UI, set index `refresh_interval` to `5-10s` and map numerics properly (scores as `float`).
* **Latency target**: end-to-end < 60s with 10s slide windows is very achievable.

---

## 8) Project Deliverables

### ✅ Real-Time Data Collection
- **Component:** `sse-ingestor` + Apache Kafka
- **Status:** Fully operational
- **Implementation:** SSE client consuming Wikimedia event stream, producing to Kafka topics
- **Verification:** 19,500+ events processed and indexed

### ✅ Real-Time Processing
- **Component:** `stream-aggregator` (Spark Structured Streaming)
- **Status:** Fully operational
- **Implementation:** 
  - Windowed aggregations (1-minute, 5-minute)
  - Trending score calculation
  - Event-time watermarking (2-minute watermark)
  - HDFS-backed checkpoints for fault tolerance
- **Metrics:** Edit count, unique editors, bytes changed, bot/minor ratios, revert counts

### ⚠️ Batch Processing  
- **Component:** `baseline-batch`
- **Status:** Not implemented (out of scope for current demo)
- **Original Intent:** Compute historical baselines (μ/σ) from pageview data
- **Alternative:** Current implementation uses real-time scoring without historical baselines

### ✅ NoSQL Databases
- **Primary:** OpenSearch (fully functional)
  - `trending-1m` index: 5,400+ documents
  - `trending-5m` index: 14,000+ documents
  - Sub-second query performance
  - Full-text search and aggregations
- **Secondary:** HBase (partial - API compatibility issue)
  - Container running but writes failing
  - Non-critical for demo

### ✅ Visualization & Demo
- **Grafana:** Pre-configured real-time dashboard
  - Top-20 trending pages table
  - Score over time visualization
  - Interactive page selection
- **OpenSearch Dashboards:** Custom visualization support
  - Index pattern configured
  - Discover interface for data exploration
- **Spark UI:** Job monitoring and performance metrics
- **Live Demo:** All components running with real data

### 📊 Performance Metrics
- **End-to-end latency:** < 60 seconds (ingestion to visualization)
- **Throughput:** Processes 100s of events/second from all Wikimedia wikis
- **Data volume:** 19,500+ documents indexed in first hour
- **Uptime:** Continuous processing with automatic restarts

---

## 9) Acknowledgments & References

- **Data Source:** [Wikimedia EventStreams](https://stream.wikimedia.org/)
- **Architecture Inspiration:** Lambda Architecture for real-time + batch processing
- **Technologies:** Apache Software Foundation projects (Kafka, Spark, Hadoop)
- **OpenSearch:** Fork of Elasticsearch/Kibana by Amazon

**Project Team:**  
Siwar Krifi - khalil.krifi@ensi-uma.tn  
ENSI - UMA, Tunisia

---

## 🚀 Getting Started (TL;DR)

```bash
# Start everything
docker compose up -d --build

# Wait ~2 minutes for services to be ready
docker compose ps

# Verify data is flowing
curl "http://localhost:9200/_cat/indices/trending-*?v"

# Open dashboards
# Grafana: http://localhost:3000
# OpenSearch Dashboards: http://localhost:5601
```

For detailed setup, troubleshooting, and usage instructions, see:
- **Quick Start:** Section 3 above
- **Dashboard Guide:** DASHBOARD_ACCESS_GUIDE.md
- **Test Results:** PIPELINE_TEST_RESULTS.md
