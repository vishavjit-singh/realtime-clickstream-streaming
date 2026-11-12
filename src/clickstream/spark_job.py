"""Spark Structured Streaming job: Kafka (or files) → validated events, dead letters,
1-minute traffic metrics and sessions, all as partitioned Parquet with exactly-once file sinks.

Four streaming queries share one parsed source. Each has its own checkpoint, so each can be
restarted, replayed or scaled on its own:

=================  ==========================================================================
``events``         every valid event (bronze), partitioned by event date
``dead_letter``    rejected records with the rule that rejected them
``metrics_minute`` tumbling 1-minute windows per event type: events, users, revenue
``sessions``       per-user session windows (30-minute gap) with funnel counts and revenue
=================  ==========================================================================

The two aggregations run behind a 10-minute event-time watermark and ``event_id``
deduplication, in append mode: a window is written once, after the watermark has closed it.

Requires ``pyspark`` (see requirements-spark.txt) and, for Kafka, the
``spark-sql-kafka-0-10`` package, which the session pulls in automatically.
"""
from __future__ import annotations

import json
import logging
import os
import time
from functools import reduce
from pathlib import Path

from .config import Settings
from .schema import (BAD_TIMESTAMP, CONTROL_TYPES, EVENT_TYPES, FUTURE_TIMESTAMP, INVALID_JSON, MISSING_FIELD,
                     NEGATIVE_PRICE, REQUIRED, UNKNOWN_EVENT_TYPE)

log = logging.getLogger(__name__)

SPARK_VERSION = "3.5.3"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"
TABLES = ("events", "dead_letter", "metrics_minute", "sessions")


def build_spark(app: str = "realtime-clickstream", kafka: bool = False, master: str = "local[*]"):
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder.appName(app).master(master)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
               .config("spark.ui.enabled", "false"))
    if kafka:
        builder = (builder.config("spark.jars.packages", KAFKA_PACKAGE)
                   .config("spark.jars.ivy", os.environ.get("IVY_HOME", "/tmp/.ivy2")))
    return builder.getOrCreate()


def event_schema():
    from pyspark.sql import types as T

    s = T.StringType()
    return T.StructType([
        T.StructField("event_id", s), T.StructField("user_id", s), T.StructField("event_type", s),
        T.StructField("event_time", s), T.StructField("page", s), T.StructField("product_id", s),
        T.StructField("price", T.DoubleType()), T.StructField("device", s), T.StructField("sent_at", s),
    ])


def read_kafka(spark, bootstrap: str, topic: str, max_offsets: int):
    from pyspark.sql import functions as F

    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", max_offsets)
            .load()
            .select(F.col("value").cast("string").alias("value"),
                    F.col("partition").alias("kafka_partition"),
                    F.col("offset").alias("kafka_offset")))


def read_files(spark, path: str, max_files: int = 1):
    """One JSON message per line; one file per micro-batch, oldest file first."""
    from pyspark.sql import functions as F

    return (spark.readStream.format("text").option("maxFilesPerTrigger", max_files).load(path)
            .select("value", F.lit(None).cast("int").alias("kafka_partition"),
                    F.lit(None).cast("long").alias("kafka_offset")))


def parse(raw, cfg: Settings):
    """Add the parsed struct, typed timestamps and a ``reject_reason`` (null when valid).

    Same rules, same order as :func:`clickstream.schema.validate`.
    """
    from pyspark.sql import functions as F

    e = F.from_json("value", event_schema())
    df = raw.withColumn("e", e)
    et, sa = F.to_timestamp("e.event_time"), F.to_timestamp("e.sent_at")
    all_null = reduce(lambda a, b: a & b, [F.col(f"e.{c}").isNull() for c in REQUIRED])
    any_missing = reduce(lambda a, b: a | b, [F.col(f"e.{c}").isNull() | (F.col(f"e.{c}") == "") for c in REQUIRED])
    skew = cfg.max_future_skew.total_seconds()
    reason = (F.when(F.col("e").isNull() | all_null, INVALID_JSON)
              .when(any_missing, MISSING_FIELD)
              .when(~F.col("e.event_type").isin(*(EVENT_TYPES + CONTROL_TYPES)), UNKNOWN_EVENT_TYPE)
              .when(et.isNull() | sa.isNull(), BAD_TIMESTAMP)
              .when(F.col("e.price") < 0, NEGATIVE_PRICE)
              .when(et.cast("double") - sa.cast("double") > skew, FUTURE_TIMESTAMP))
    return df.withColumn("event_time", et).withColumn("sent_at", sa).withColumn("reject_reason", reason)


def build_queries(parsed, cfg: Settings):
    """Return ``{table: streaming DataFrame}``; nothing is started here."""
    from pyspark.sql import functions as F

    valid = parsed.filter(F.col("reject_reason").isNull()).select(
        "e.event_id", "e.user_id", "e.event_type", "event_time", "sent_at", "e.page", "e.product_id",
        "e.price", "e.device", "kafka_partition", "kafka_offset")

    events = (valid.filter(~F.col("event_type").isin(*CONTROL_TYPES))
              .withColumn("dt", F.date_format("event_time", "yyyy-MM-dd")))
    dead = (parsed.filter(F.col("reject_reason").isNotNull())
            .select(F.col("value").alias("raw_value"), F.col("reject_reason").alias("reason"),
                    "kafka_partition", "kafka_offset", F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("dt")))

    watermarked = (valid.withWatermark("event_time", f"{int(cfg.watermark.total_seconds())} seconds")
                   .dropDuplicatesWithinWatermark(["event_id"])
                   .filter(~F.col("event_type").isin(*CONTROL_TYPES)))
    revenue = F.when(F.col("event_type") == "purchase", F.coalesce("price", F.lit(0.0))).otherwise(F.lit(0.0))

    window = f"{int(cfg.window.total_seconds())} seconds"
    metrics = (watermarked.groupBy(F.window("event_time", window), "event_type")
               .agg(F.count(F.lit(1)).alias("events"),
                    F.approx_count_distinct("user_id").alias("users"),
                    F.round(F.sum(revenue), 2).alias("revenue"))
               .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                       "event_type", "events", "users", "revenue",
                       F.date_format("window.start", "yyyy-MM-dd").alias("dt")))

    gap_s = cfg.session_gap.total_seconds()

    def count_of(t):
        return F.sum(F.when(F.col("event_type") == t, 1).otherwise(0)).cast("long")

    sessions = (watermarked.groupBy("user_id", F.session_window("event_time", f"{int(gap_s)} seconds"))
                .agg(F.count(F.lit(1)).alias("events"), count_of("page_view").alias("page_views"),
                     count_of("add_to_cart").alias("add_to_cart"), count_of("checkout").alias("checkouts"),
                     count_of("purchase").alias("purchases"), F.round(F.sum(revenue), 2).alias("revenue"))
                .select("user_id",
                        F.col("session_window.start").alias("session_start"),
                        F.col("session_window.end").alias("session_end"),
                        (F.col("session_window.end").cast("double") - F.col("session_window.start").cast("double")
                         - F.lit(gap_s)).alias("duration_s"),
                        "events", "page_views", "add_to_cart", "checkouts", "purchases", "revenue",
                        (F.col("purchases") > 0).alias("converted"),
                        F.date_format("session_window.start", "yyyy-MM-dd").alias("dt")))
    return {"events": events, "dead_letter": dead, "metrics_minute": metrics, "sessions": sessions}


def start(frames: dict, lake: str | Path, checkpoint: str | Path, trigger: str = "available_now"):
    queries = []
    for name, df in frames.items():
        writer = (df.writeStream.format("parquet").outputMode("append").queryName(name)
                  .option("path", str(Path(lake) / name))
                  .option("checkpointLocation", str(Path(checkpoint) / name))
                  .partitionBy("dt"))
        writer = writer.trigger(availableNow=True) if trigger == "available_now" else \
            writer.trigger(processingTime=trigger)
        queries.append(writer.start())
    return queries


def _dump_progress(queries, out: Path, seen: set) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a") as fh:
        for q in queries:
            for p in q.recentProgress:
                key = (p["name"], p["batchId"])
                if key not in seen:
                    seen.add(key)
                    fh.write(json.dumps(p) + "\n")


def run(cfg: Settings, source: str, lake: str | Path, checkpoint: str | Path, *, bootstrap: str = "kafka:9092",
        files: str | None = None, trigger: str = "available_now") -> None:
    spark = build_spark(kafka=source == "kafka")
    raw = (read_kafka(spark, bootstrap, cfg.topic, cfg.max_records_per_trigger) if source == "kafka"
           else read_files(spark, files))
    queries = start(build_queries(parse(raw, cfg), cfg), lake, checkpoint, trigger)
    progress_file, seen = Path(checkpoint) / "_progress.jsonl", set()
    try:
        while any(q.isActive for q in queries) and not any(q.exception() for q in queries):
            time.sleep(2)
            _dump_progress(queries, progress_file, seen)
    finally:
        _dump_progress(queries, progress_file, seen)
        for q in queries:
            q.stop()
    failed = [q for q in queries if q.exception()]
    if failed:
        raise RuntimeError(f"streaming query {failed[0].name} failed: {failed[0].exception()}")
    log.info("all queries finished; progress in %s", progress_file)
