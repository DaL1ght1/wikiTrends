"""Spark Structured Streaming: raw recentchange events -> windowed trending scores.

Outputs
  wikimedia.recentchange.clean   cleaned, enriched events (also the input of the baseline batch job)
  trending.windows.1m            1-minute windows, sliding every 30 s
  trending.windows.5m            5-minute windows, sliding every 60 s
  trending.bots.5m               per-bot-account activity, 5-minute windows

Window rows carry actor=human|bot: the trending list uses human rows, the UI's
bot section uses bot rows.

Window rows are emitted in *update* mode: every trigger re-emits windows that
changed, so the dashboard sees a window while it is still filling instead of
waiting for the watermark to close it. The downstream indexer upserts by a
stable id, so repeated emissions overwrite rather than duplicate.

Each window is joined with the per-page baseline computed by the
baseline-batch job (if present) to get a z-score: how unusual the current edit
rate is for that page. The baseline is re-read whenever a newer version is
published, without restarting the stream.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
RAW_TOPIC = os.getenv("INPUT_TOPIC", "wikimedia.recentchange.raw")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "wikimedia.recentchange.clean")
T1M_TOPIC = os.getenv("TREND_TOPIC_1M", "trending.windows.1m")
T5M_TOPIC = os.getenv("TREND_TOPIC_5M", "trending.windows.5m")
BOT_TOPIC = os.getenv("BOT_TOPIC_5M", "trending.bots.5m")
# v3: window state is now keyed by actor (human/bot) as well.
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "hdfs://hadoop-namenode:9000/checkpoints/stream-agg-v3")
BASELINE_ROOT = os.getenv("BASELINE_ROOT", "hdfs://hadoop-namenode:9000/lake/baselines/edits_hourly")

WINDOWS = {
    # name: (window, slide, minutes, topic)
    "1m": (os.getenv("WINDOW_1M", "1 minute"), os.getenv("SLIDE_1M", "30 seconds"), 1.0, T1M_TOPIC),
    "5m": (os.getenv("WINDOW_5M", "5 minutes"), os.getenv("SLIDE_5M", "60 seconds"), 5.0, T5M_TOPIC),
}
WATERMARK = os.getenv("WATERMARK", "1 minute")
TRIGGER_EVERY = os.getenv("TRIGGER_EVERY", "10 seconds")
MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
# Bot edits are kept and aggregated separately (actor="bot") so the UI can show
# bot activity; the trending list itself only uses human windows.
# TRACK_BOTS=false drops bot edits entirely, as v1 did.
TRACK_BOTS = os.getenv("TRACK_BOTS", "true").lower() in ("1", "true", "yes")
NAMESPACES = [int(n) for n in os.getenv("NAMESPACES", "0").split(",") if n.strip()]
DEBUG_BAD_RECORDS = os.getenv("DEBUG_BAD_RECORDS", "false").lower() in ("1", "true", "yes")

schema = T.StructType([
    T.StructField("wiki", T.StringType()),
    T.StructField("lang", T.StringType()),
    T.StructField("server_name", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("page_url", T.StringType()),
    T.StructField("namespace", T.IntegerType()),
    T.StructField("type", T.StringType()),
    T.StructField("rev_id", T.LongType()),
    T.StructField("user", T.StringType()),
    T.StructField("minor", T.BooleanType()),
    T.StructField("bot", T.BooleanType()),
    T.StructField("timestamp", T.LongType()),  # epoch seconds
    T.StructField("comment", T.StringType()),
    T.StructField("length_new", T.LongType()),
    T.StructField("length_old", T.LongType()),
    T.StructField("is_revert", T.BooleanType()),
])

spark = (
    SparkSession.builder.appName("stream-aggregator")
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "6"))
    .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
    .config("spark.sql.streaming.minBatchesToRetain", "10")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
log4j = spark._jvm.org.apache.log4j.LogManager.getLogger("stream-aggregator")


# --------------------------------------------------------------------------
# Source + cleaning
# --------------------------------------------------------------------------
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", RAW_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

parsed = raw.select(
    F.col("value").cast("string").alias("raw_value"),
    F.from_json(F.col("value").cast("string"), schema).alias("v"),
)

if DEBUG_BAD_RECORDS:
    (parsed.where(F.col("v").isNull()).select("raw_value")
     .writeStream.format("console").option("truncate", "false").start())

events = parsed.where(F.col("v").isNotNull()).select("v.*")
events = events.filter(F.col("wiki").isNotNull() & F.col("title").isNotNull() & F.col("timestamp").isNotNull())
if not TRACK_BOTS:
    events = events.filter(~F.coalesce(F.col("bot"), F.lit(False)))
if NAMESPACES:
    events = events.filter(F.col("namespace").isin(NAMESPACES))

clean = (
    events
    .withColumn("actor", F.when(F.coalesce(F.col("bot"), F.lit(False)), F.lit("bot")).otherwise(F.lit("human")))
    .withColumn("event_ts", F.timestamp_seconds("timestamp"))
    .withColumn("bytes_delta", F.coalesce("length_new", F.lit(0)) - F.coalesce("length_old", F.lit(0)))
    .withColumn("abs_delta", F.abs("bytes_delta"))
    .withColumn("is_new", F.col("type") == "new")
)

# Cleaned event stream (stateless, so no watermark needed).
(
    clean.select(
        F.concat_ws("|", "wiki", "title").cast("binary").alias("key"),
        F.to_json(F.struct(*[c for c in clean.columns if c != "event_ts"])).cast("binary").alias("value"),
    )
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("topic", CLEAN_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/clean")
    .trigger(processingTime=TRIGGER_EVERY)
    .start()
)


# --------------------------------------------------------------------------
# Windowed aggregates
# --------------------------------------------------------------------------
def window_agg(df: DataFrame, window: str, slide: str) -> DataFrame:
    return (
        df.withWatermark("event_ts", WATERMARK)
        .groupBy(F.window("event_ts", window, slide).alias("w"), "wiki", "title", "actor")
        .agg(
            F.first("lang", ignorenulls=True).alias("lang"),
            F.first("server_name", ignorenulls=True).alias("server_name"),
            F.first("page_url", ignorenulls=True).alias("page_url"),
            F.first("namespace", ignorenulls=True).alias("namespace"),
            F.count("*").alias("edit_count"),
            F.approx_count_distinct("user", 0.05).alias("unique_editors"),
            F.sum("abs_delta").alias("sum_abs_delta"),
            F.sum("bytes_delta").alias("net_delta"),
            F.sum(F.col("is_revert").cast("int")).alias("revert_count"),
            F.sum(F.col("is_new").cast("int")).alias("new_page"),
            F.avg(F.col("minor").cast("int")).alias("minor_share"),
            F.avg(F.col("bot").cast("int")).alias("bot_share"),
            F.max("event_ts").alias("last_event_ts"),
        )
        .withColumn("window_start", F.col("w.start"))
        .withColumn("ts_bucket", F.col("w.end"))
        .drop("w")
    )


# --------------------------------------------------------------------------
# Baseline (published by baseline-batch as <root>/v=<epoch>/ + <root>/LATEST)
# --------------------------------------------------------------------------
_baseline = {"version": None, "df": None}


def _read_latest_version() -> str | None:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{BASELINE_ROOT}/LATEST")
    try:
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(path):
            return None
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(fs.open(path)))
        try:
            line = reader.readLine()
        finally:
            reader.close()
        return line.strip() if line else None
    except Exception as e:  # HDFS hiccup: keep the baseline we have
        log4j.warn(f"baseline: cannot read LATEST: {e}")
        return _baseline["version"]


def current_baseline() -> DataFrame | None:
    version = _read_latest_version()
    if version and version != _baseline["version"]:
        try:
            df = spark.read.parquet(f"{BASELINE_ROOT}/v={version}").select(
                "wiki", "title", "mu_edits_hour", "sigma_edits_hour", "hours_observed"
            )
            old = _baseline["df"]
            _baseline.update(version=version, df=df.cache())
            if old is not None:
                old.unpersist()
            log4j.warn(f"baseline: loaded version {version}")
        except Exception as e:
            log4j.warn(f"baseline: cannot load version {version}: {e}")
    return _baseline["df"]


def score(df: DataFrame, minutes: float) -> DataFrame:
    """Composite trending score; see README "Scoring" for the rationale."""
    baseline = current_baseline()
    if baseline is not None:
        df = df.join(baseline, ["wiki", "title"], "left")
    else:
        df = (df.withColumn("mu_edits_hour", F.lit(None).cast("double"))
                .withColumn("sigma_edits_hour", F.lit(None).cast("double"))
                .withColumn("hours_observed", F.lit(None).cast("long")))
    frac = minutes / 60.0
    has_baseline = F.col("mu_edits_hour").isNotNull()
    expected = F.when(has_baseline, F.col("mu_edits_hour") * frac).otherwise(F.lit(0.0))
    # Poisson-ish scaling of the hourly std to the window length, floored at 1 edit.
    sigma = F.greatest(F.when(has_baseline, F.col("sigma_edits_hour") * F.sqrt(F.lit(frac))).otherwise(1.0), F.lit(1.0))
    # Driven by edits and *additional* editors; size and page creation only
    # nudge, so a single large edit can't outrank a page many people are editing.
    activity = (
        F.col("edit_count") * 1.0
        + (F.least("unique_editors", F.lit(20)) - 1) * 1.5
        + F.log1p("sum_abs_delta") * 0.25
        + F.least("new_page", F.lit(1)) * 1.0
        - F.col("revert_count") * 1.0
    )
    # Baselines describe human editing, so z-scores are only meaningful for human rows.
    human = F.col("actor") == "human"
    z = F.when(human, (F.col("edit_count") - expected) / sigma)
    return (
        df.withColumn("expected_edits", F.when(human, expected))
        .withColumn("z_score", z)
        .withColumn("has_baseline", human & has_baseline)
        .withColumn("activity", activity)
        .withColumn("score", activity + F.coalesce(F.least(F.greatest(z, F.lit(0.0)), F.lit(20.0)), F.lit(0.0)) * 0.5)
    )


PAYLOAD_COLS = [
    "ts_bucket", "window_start", "last_event_ts", "window_size", "actor",
    "wiki", "lang", "server_name", "title", "page_url", "namespace",
    "score", "activity", "z_score", "expected_edits", "has_baseline",
    "edit_count", "unique_editors", "sum_abs_delta", "net_delta",
    "revert_count", "new_page", "minor_share", "bot_share",
]


def make_sink(name: str, minutes: float, topic: str):
    def sink(batch_df: DataFrame, batch_id: int) -> None:
        out = score(batch_df, minutes).withColumn("window_size", F.lit(name))
        (
            out.select(
                F.concat_ws("|", "wiki", "title", "window_size", "actor").cast("binary").alias("key"),
                F.to_json(F.struct(*PAYLOAD_COLS)).cast("binary").alias("value"),
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("topic", topic)
            .save()
        )

    return sink


for name, (window, slide, minutes, topic) in WINDOWS.items():
    (
        window_agg(clean, window, slide)
        .writeStream.outputMode("update")
        .foreachBatch(make_sink(name, minutes, topic))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/win{name}")
        .trigger(processingTime=TRIGGER_EVERY)
        .queryName(f"windows-{name}")
        .start()
    )


# --------------------------------------------------------------------------
# Bot accounts: who is editing, how much, on how many pages (5-min windows)
# --------------------------------------------------------------------------
BOT_COLS = [
    "ts_bucket", "window_start", "last_event_ts", "window_size", "kind",
    "wiki", "server_name", "user", "edit_count", "pages", "sum_abs_delta", "new_pages",
]

if TRACK_BOTS:
    bots = (
        clean.where(F.col("actor") == "bot")
        .withWatermark("event_ts", WATERMARK)
        .groupBy(F.window("event_ts", "5 minutes", "60 seconds").alias("w"), "wiki", "user")
        .agg(
            F.first("server_name", ignorenulls=True).alias("server_name"),
            F.count("*").alias("edit_count"),
            F.approx_count_distinct("title", 0.05).alias("pages"),
            F.sum("abs_delta").alias("sum_abs_delta"),
            F.sum(F.col("is_new").cast("int")).alias("new_pages"),
            F.max("event_ts").alias("last_event_ts"),
        )
        .withColumn("window_start", F.col("w.start"))
        .withColumn("ts_bucket", F.col("w.end"))
        .withColumn("window_size", F.lit("5m"))
        .withColumn("kind", F.lit("bot_account"))
        .drop("w")
    )
    (
        bots.select(
            F.concat_ws("|", "wiki", "user").cast("binary").alias("key"),
            F.to_json(F.struct(*BOT_COLS)).cast("binary").alias("value"),
        )
        .writeStream.format("kafka").outputMode("update")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", BOT_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/bots5m")
        .trigger(processingTime=TRIGGER_EVERY)
        .queryName("bots-5m")
        .start()
    )

spark.streams.awaitAnyTermination()
