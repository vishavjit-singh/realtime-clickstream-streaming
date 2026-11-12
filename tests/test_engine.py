from dataclasses import replace

import pandas as pd
import pytest

from clickstream.engine import SimulatedCrash, StreamingQuery, load_progress
from clickstream.log import LocalLog
from clickstream.sinks import read_table


def run(cfg, log, root, **kw):
    q = StreamingQuery(cfg, log, root / "lake", root / "ckpt", fmt="csv", **kw)
    return q, q.run_available()


def snapshot(lake):
    """Order-independent view of every output table."""
    out = {}
    for table, key in [("metrics_minute", ["window_start", "event_type"]), ("sessions", ["user_id", "session_start"]),
                       ("events", ["event_id", "kafka_partition", "kafka_offset"]),
                       ("dead_letter", ["kafka_partition", "kafka_offset"])]:
        df = read_table(lake, table).drop(columns=["ingest_batch", "dt"], errors="ignore")
        out[table] = df.sort_values(key).reset_index(drop=True)
    return out


def assert_same(a, b):
    for table in a:
        pd.testing.assert_frame_equal(a[table], b[table], check_like=True)


def test_every_record_is_accounted_for(cfg, source, tmp_path):
    _, progress = run(cfg, source, tmp_path)
    total = lambda k: sum(p[k] for p in progress)  # noqa: E731
    assert total("input") == sum(source.end_offsets().values())
    assert total("input") == total("valid") + total("dead_letter")
    metrics, sessions = read_table(tmp_path / "lake", "metrics_minute"), read_table(tmp_path / "lake", "sessions")
    # every on-time, unique event lands in exactly one window and exactly one session
    assert metrics["events"].sum() == total("aggregated") == sessions["events"].sum()
    assert total("late_dropped") > 0 and total("duplicates") > 0 and total("dead_letter") > 0


def test_batches_respect_the_record_budget(cfg, source, tmp_path):
    _, progress = run(cfg, source, tmp_path)
    assert len(progress) > 3
    assert all(p["input"] <= cfg.max_records_per_trigger + cfg.partitions for p in progress)


def test_rerun_with_no_new_data_does_nothing(cfg, source, tmp_path):
    run(cfg, source, tmp_path)
    before = snapshot(tmp_path / "lake")
    _, progress = run(cfg, source, tmp_path)
    assert progress == []
    assert_same(before, snapshot(tmp_path / "lake"))


def test_stop_and_resume_matches_a_single_run(cfg, source, tmp_path):
    run(cfg, source, tmp_path / "single")
    q = StreamingQuery(cfg, source, tmp_path / "resumed" / "lake", tmp_path / "resumed" / "ckpt", fmt="csv")
    first = q.run_available(max_batches=3)
    rest = StreamingQuery(cfg, source, tmp_path / "resumed" / "lake", tmp_path / "resumed" / "ckpt",
                          fmt="csv").run_available()
    assert [p["batch_id"] for p in first + rest] == list(range(len(first) + len(rest)))
    assert_same(snapshot(tmp_path / "single" / "lake"), snapshot(tmp_path / "resumed" / "lake"))


def test_crash_after_sink_write_is_recovered_exactly_once(cfg, source, tmp_path):
    run(cfg, source, tmp_path / "clean")

    def boom(batch_id):
        if batch_id == 4:
            raise SimulatedCrash("killed after writing sinks, before commit")

    crashed = StreamingQuery(cfg, source, tmp_path / "crash" / "lake", tmp_path / "crash" / "ckpt", fmt="csv",
                             after_sinks=boom)
    with pytest.raises(SimulatedCrash):
        crashed.run_available()
    assert not (tmp_path / "crash" / "ckpt" / "commits" / "4.json").exists()
    StreamingQuery(cfg, source, tmp_path / "crash" / "lake", tmp_path / "crash" / "ckpt", fmt="csv").run_available()
    assert_same(snapshot(tmp_path / "clean" / "lake"), snapshot(tmp_path / "crash" / "lake"))
    assert len(load_progress(tmp_path / "crash" / "ckpt")) == len(load_progress(tmp_path / "clean" / "ckpt"))


def test_new_data_is_picked_up_incrementally(cfg, tmp_path):
    from tests.conftest import closed_stream

    records = closed_stream(cfg)
    log = LocalLog(tmp_path / "src", cfg.topic, cfg.partitions)
    half = len(records) // 2
    log.append(records[:half])
    _, p1 = run(cfg, log, tmp_path)
    log.append(records[half:])
    _, p2 = run(cfg, log, tmp_path)
    assert sum(p["input"] for p in p1 + p2) == len(records)
    assert p2[0]["batch_id"] == p1[-1]["batch_id"] + 1


def test_results_do_not_depend_on_batch_size_without_very_late_data(cfg, tmp_path):
    """With no event later than the watermark, any batching gives identical aggregates."""
    from tests.conftest import closed_stream

    gen = replace(cfg.generator, late_beyond_watermark=0.0, clock_skew_rate=0.0)
    calm = replace(cfg, generator=gen)
    results = []
    for budget in (90, 700):
        c = replace(calm, max_records_per_trigger=budget)
        log = LocalLog(tmp_path / f"src{budget}", c.topic, c.partitions)
        log.append(closed_stream(c))
        root = tmp_path / f"run{budget}"
        _, progress = run(c, log, root)
        assert sum(p["late_dropped"] for p in progress) == 0
        snap = snapshot(root / "lake")
        results.append({k: snap[k] for k in ("metrics_minute", "sessions")})
    assert_same(*results)


def test_parquet_sink(cfg, source, tmp_path):
    pytest.importorskip("pyarrow")
    q = StreamingQuery(cfg, source, tmp_path / "lake", tmp_path / "ckpt", fmt="parquet")
    q.run_available()
    assert list((tmp_path / "lake" / "metrics_minute").glob("dt=*/*.parquet"))
    assert len(read_table(tmp_path / "lake", "sessions")) > 0
