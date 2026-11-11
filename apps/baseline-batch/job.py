"""Batch layer: per-page edit-rate baselines from the cleaned event history.

Reads everything currently retained in `wikimedia.recentchange.clean`
(72 h by default), counts edits per (wiki, title, hour) and derives, per page:

  mu_edits_hour     mean edits per hour over the whole observed span
  sigma_edits_hour  standard deviation of edits per hour (hours with 0 edits included)
  hours_observed    length of the observed span in hours

The result is written to a new versioned directory <root>/v=<epoch>/ and then
published by rewriting <root>/LATEST, so the streaming job can swap baselines
atomically while it runs. Older versions beyond KEEP_VERSIONS are deleted.
"""

from __future__ import annotations

import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "wikimedia.recentchange.clean")
BASELINE_ROOT = os.getenv("BASELINE_ROOT", "hdfs://hadoop-namenode:9000/lake/baselines/edits_hourly")
MIN_HOURS = float(os.getenv("BASELINE_MIN_HOURS", "1"))
MIN_EDITS = int(os.getenv("BASELINE_MIN_EDITS", "2"))
KEEP_VERSIONS = int(os.getenv("KEEP_VERSIONS", "3"))

schema = T.StructType([
    T.StructField("wiki", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("timestamp", T.LongType()),
    T.StructField("bot", T.BooleanType()),
])

spark = (
    SparkSession.builder.appName("baseline-batch")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def main() -> int:
    events = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", CLEAN_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
        .select("v.*")
        .where(F.col("wiki").isNotNull() & F.col("title").isNotNull() & F.col("timestamp").isNotNull())
        # baselines describe *human* editing; bot edits are in the clean topic for the bot views
        .where(~F.coalesce(F.col("bot"), F.lit(False)))
        .withColumn("hour", F.floor(F.col("timestamp") / 3600).cast("long"))
    )

    span = events.agg(F.min("hour").alias("lo"), F.max("hour").alias("hi")).first()
    if span is None or span["lo"] is None:
        print("[baseline] no history yet; nothing to do")
        return 0
    hours = float(span["hi"] - span["lo"] + 1)
    if hours < MIN_HOURS:
        print(f"[baseline] only {hours:.0f}h of history (< {MIN_HOURS}); skipping")
        return 0

    hourly = events.groupBy("wiki", "title", "hour").count()
    baseline = (
        hourly.groupBy("wiki", "title")
        .agg(F.sum("count").alias("total"), F.sum(F.col("count") * F.col("count")).alias("sumsq"))
        .where(F.col("total") >= MIN_EDITS)
        .withColumn("mu_edits_hour", F.col("total") / F.lit(hours))
        # population variance including the zero-edit hours: E[x^2] - E[x]^2
        .withColumn(
            "sigma_edits_hour",
            F.sqrt(F.greatest(F.col("sumsq") / F.lit(hours) - F.col("mu_edits_hour") ** 2, F.lit(0.0))),
        )
        .withColumn("hours_observed", F.lit(int(hours)).cast("long"))
        .select("wiki", "title", "mu_edits_hour", "sigma_edits_hour", "hours_observed", "total")
    )

    version = str(int(time.time()))
    target = f"{BASELINE_ROOT}/v={version}"
    baseline.repartition(4).write.mode("overwrite").parquet(target)
    rows = spark.read.parquet(target).count()

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    root = jvm.org.apache.hadoop.fs.Path(BASELINE_ROOT)
    fs = root.getFileSystem(conf)
    out = fs.create(jvm.org.apache.hadoop.fs.Path(f"{BASELINE_ROOT}/LATEST"), True)
    out.write(bytearray(version.encode()))
    out.close()

    versions = sorted(
        (s.getPath().getName() for s in fs.listStatus(root) if s.getPath().getName().startswith("v=")),
        key=lambda n: int(n[2:]),
    )
    for old in versions[:-KEEP_VERSIONS]:
        fs.delete(jvm.org.apache.hadoop.fs.Path(f"{BASELINE_ROOT}/{old}"), True)

    print(f"[baseline] published v={version}: {rows} pages over {hours:.0f}h of history")
    return 0


if __name__ == "__main__":
    sys.exit(main())
