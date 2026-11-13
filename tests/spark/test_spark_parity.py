"""Parity: the Spark Structured Streaming job and the local reference engine agree.

Runs only where pyspark and a JVM are available (the ``spark`` workflow in CI). The stream used
here has no events beyond the watermark: Spark documents that such events *may or may not* be
dropped, so they are the one case where two correct engines are allowed to differ.
"""
from dataclasses import replace

import pandas as pd
import pytest

pytest.importorskip("pyspark")

from clickstream import spark_job  # noqa: E402
from clickstream.cli import export_files  # noqa: E402
from clickstream.engine import StreamingQuery  # noqa: E402
from clickstream.generator import generate, heartbeats  # noqa: E402
from clickstream.log import LocalLog  # noqa: E402
from clickstream.sinks import read_table  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def outputs(tmp_path_factory):
    from clickstream.config import GeneratorSettings, load_settings

    tmp = tmp_path_factory.mktemp("parity")
    gen = GeneratorSettings(seed=5, users=120, minutes=45, campaign_start_minute=20, campaign_minutes=5,
                            late_beyond_watermark=0.0, clock_skew_rate=0.0, malformed_rate=0.03)
    cfg = replace(load_settings(), generator=gen, max_records_per_trigger=250)
    records = generate(gen)

    # One partition, so the local engine cuts the same arrival-ordered micro-batches as Spark's file
    # source. Across several partitions its record budget is split by lag, so one partition can run
    # minutes ahead of another and push the watermark past events still queued behind it.
    log = LocalLog(tmp / "src", cfg.topic, 1)
    log.append(records + heartbeats(records[-1].arrival, 1, cfg.watermark + cfg.session_gap))
    StreamingQuery(cfg, log, tmp / "local", tmp / "local_ckpt", fmt="csv").run_available()

    export_files(records, cfg, tmp / "files", 250)
    spark_job.run(cfg, "files", tmp / "spark", tmp / "spark_ckpt", files=str(tmp / "files"))
    return tmp / "local", tmp / "spark"


def _norm(df, cols):
    df = df[cols].copy()
    for c in cols:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return df.sort_values(cols).reset_index(drop=True)


def test_minute_metrics_match(outputs):
    local, spark = (read_table(p, "metrics_minute") for p in outputs)
    assert len(local) > 0
    cols = ["window_start", "window_end", "event_type", "events", "revenue"]
    pd.testing.assert_frame_equal(_norm(local, cols), _norm(spark, cols), check_dtype=False)
    # Spark counts users with HyperLogLog++ (approx_count_distinct). Its documented 5% is a relative
    # standard deviation, not a bound, so allow three of them.
    merged = _norm(local, ["window_start", "event_type", "users"]).merge(
        _norm(spark, ["window_start", "event_type", "users"]), on=["window_start", "event_type"])
    off = merged[(merged.users_x - merged.users_y).abs() > (3 * 0.05 * merged.users_x).clip(lower=1)]
    assert off.empty, f"approx user counts outside tolerance:\n{off.to_string()}"


def test_sessions_match(outputs):
    local, spark = (read_table(p, "sessions") for p in outputs)
    cols = ["user_id", "session_start", "session_end", "events", "page_views", "add_to_cart", "checkouts",
            "purchases", "revenue"]
    pd.testing.assert_frame_equal(_norm(local, cols), _norm(spark, cols), check_dtype=False)


def test_bronze_and_dead_letters_match(outputs):
    le, se = (read_table(p, "events") for p in outputs)
    ld, sd = (read_table(p, "dead_letter") for p in outputs)
    assert sorted(le["event_id"]) == sorted(se["event_id"])
    assert sorted(ld["raw_value"]) == sorted(sd["raw_value"])

    def grouped(df):  # a truncated message may surface as either reason depending on the parser
        return df.assign(reason=df["reason"].replace({"missing_field": "invalid_json"}))[["raw_value", "reason"]] \
            .sort_values(["raw_value", "reason"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(grouped(ld), grouped(sd))
