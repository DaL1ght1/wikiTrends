import os
from pyspark.sql import SparkSession, functions as F, types as T

# -----------------------------
# Env/config (with sensible defaults)
# -----------------------------
BOOTSTRAP       = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
RAW_TOPIC       = os.getenv("INPUT_TOPIC", "wikimedia.recentchange.raw")
CLEAN_TOPIC     = os.getenv("CLEAN_TOPIC", "wikimedia.recentchange.clean")

T1M_TOPIC       = os.getenv("TREND_TOPIC_1M", "trending.windows.1m")
T5M_TOPIC       = os.getenv("TREND_TOPIC_5M", "trending.windows.5m")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR", "hdfs://hadoop-namenode:9000/checkpoints/stream-agg")

# Windows / watermark / trigger
WINDOW_1M       = os.getenv("WINDOW_1M",  "1 minute")
SLIDE_1M        = os.getenv("SLIDE_1M",   "30 seconds")
WINDOW_5M       = os.getenv("WINDOW_5M",  "5 minutes")
SLIDE_5M        = os.getenv("SLIDE_5M",   "60 seconds")
# For quick demos you can set WATERMARK="1 minute".
WATERMARK       = os.getenv("WATERMARK",  "2 minutes")
TRIGGER_EVERY   = os.getenv("TRIGGER_EVERY", "10 seconds")
MAX_OFFSETS     = os.getenv("MAX_OFFSETS_PER_TRIGGER", "2000")
# Start from latest so event time moves quickly to "now"
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")  # was "earliest"

# Optional interactive debugging of windows
DEBUG_WINDOWS   = os.getenv("DEBUG_WINDOWS", "false").lower() in ("1","true","yes")

# -----------------------------
# Schema of the raw recentchange payload
# -----------------------------
schema = T.StructType([
    T.StructField("wiki",        T.StringType()),
    T.StructField("lang",        T.StringType()),
    T.StructField("title",       T.StringType()),
    T.StructField("rev_id",      T.LongType()),
    T.StructField("user",        T.StringType()),
    T.StructField("minor",       T.BooleanType()),
    T.StructField("bot",         T.BooleanType()),
    T.StructField("timestamp",   T.LongType()),  # epoch seconds
    T.StructField("comment",     T.StringType()),
    T.StructField("length_new",  T.LongType()),
    T.StructField("length_old",  T.LongType()),
    T.StructField("is_revert",   T.BooleanType()),
])

# -----------------------------
# Spark session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("stream-aggregator")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
    .config("spark.sql.streaming.minBatchesToRetain", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.debug.maxToStringFields", "2000")

# -----------------------------
# 1) Read from Kafka
# -----------------------------
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", RAW_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .load()
)

# -----------------------------
# 2) Parse JSON safely
# -----------------------------
parsed = raw.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("value").cast("string").alias("raw_value"),
    F.from_json(F.col("value").cast("string"), schema, {"mode": "PERMISSIVE"}).alias("v")
)

json_df = parsed.where(F.col("v").isNotNull()).select("v.*")

# Optional: peek at bad lines to catch schema issues
bad = parsed.where(F.col("v").isNull())
bad_console = (
    bad.select("kafka_key", "raw_value")
       .writeStream.format("console").option("truncate","false")
       .outputMode("append").start()
)

# -----------------------------
# 3) Clean, enrich
# -----------------------------
clean = (
    json_df
    .filter(F.col("wiki").isNotNull() & F.col("title").isNotNull())
    # epoch *seconds* -> timestamp (UTC)  ✅ NO /1000
    .withColumn("event_ts", F.from_unixtime(F.col("timestamp")).cast("timestamp"))
    .withColumn(
        "bytes_delta",
        (F.coalesce(F.col("length_new"), F.lit(0)) - F.coalesce(F.col("length_old"), F.lit(0)))
    )
    .withColumn("abs_delta", F.abs(F.col("bytes_delta")))
    .withWatermark("event_ts", WATERMARK)
)

# -----------------------------
# 4) Write cleaned stream (history/debug)
# -----------------------------
clean_out = (
    clean
    .withColumn("key", F.concat_ws("|", F.col("wiki"), F.col("title")).cast("string"))
    .select(
        F.col("key").cast("binary").alias("key"),
        F.to_json(F.struct(*[c for c in clean.columns if c != "event_ts"])).cast("binary").alias("value")
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("topic", CLEAN_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/clean")
    .outputMode("append")
    .trigger(processingTime=TRIGGER_EVERY)
    .start()
)

# -----------------------------
# 5) Windowed aggregates
# -----------------------------
def window_agg(df, window="1 minute", slide="30 seconds"):
    g = (
        df.groupBy(
            F.window("event_ts", window, slide).alias("w"),
            F.col("wiki"), F.col("lang"), F.col("title"),
        )
        .agg(
            F.count("*").alias("edit_count"),
            F.approx_count_distinct("user", 0.05).alias("unique_editors"),
            F.sum("abs_delta").alias("sum_abs_delta"),
            F.sum(F.col("is_revert").cast("int")).alias("revert_count"),
            F.avg(F.col("minor").cast("int")).alias("minor_share"),
            F.avg(F.col("bot").cast("int")).alias("bot_share"),
        )
        .withColumn("ts_bucket", F.col("w").getField("end"))
        .drop("w")
    )
    # simple composite score
    g = g.withColumn(
        "score",
        F.col("edit_count") * 1.0
        + F.least(F.col("unique_editors"), F.lit(10)) * 0.5
        + (F.col("sum_abs_delta") / 1000.0)
        - F.col("revert_count") * 0.5
        - F.col("bot_share") * 2.0
    )
    return g

win1m = window_agg(clean, WINDOW_1M, SLIDE_1M).withColumn("window_size", F.lit("1m"))
win5m = window_agg(clean, WINDOW_5M, SLIDE_5M).withColumn("window_size", F.lit("5m"))

# -----------------------------
# 6) Sink windowed aggregates to Kafka
# -----------------------------
def aggregate_sink(df, topic, chk_name):
    payload_cols = [
        "ts_bucket","wiki","lang","title","window_size",
        "score","edit_count","unique_editors","sum_abs_delta",
        "revert_count","minor_share","bot_share"
    ]
    keyed = df.withColumn("key", F.concat_ws("|", "wiki", "title", "window_size"))
    return (
        keyed
        .select(
            F.col("key").cast("string").cast("binary").alias("key"),
            F.to_json(F.struct(*payload_cols)).cast("binary").alias("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", topic)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{chk_name}")
        .outputMode("append")  # ✅ append with watermark/windows
        .trigger(processingTime=TRIGGER_EVERY)
        .start()
    )

q1m = aggregate_sink(win1m, T1M_TOPIC, "win1m")
q5m = aggregate_sink(win5m, T5M_TOPIC, "win5m")

# -----------------------------
# 7) Optional: Console debug of windows
# -----------------------------
if DEBUG_WINDOWS:
    (win1m.orderBy(F.desc("ts_bucket"), F.desc("score"))
          .select("ts_bucket","wiki","lang","title","edit_count","unique_editors","score")
          .writeStream.format("console").outputMode("update")
          .option("truncate", "false")
          .option("numRows", "50")
          .start())
    (win5m.orderBy(F.desc("ts_bucket"), F.desc("score"))
          .select("ts_bucket","wiki","lang","title","edit_count","unique_editors","score")
          .writeStream.format("console").outputMode("update")
          .option("truncate", "false")
          .option("numRows", "50")
          .start())

# -----------------------------
# Run until killed
# -----------------------------
spark.streams.awaitAnyTermination()
