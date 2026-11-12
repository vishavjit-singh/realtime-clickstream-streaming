from clickstream.engine import StreamingQuery, load_progress
from clickstream.report import render_all, summarise


def test_report_renders_every_chart(cfg, source):
    StreamingQuery(cfg, source, cfg.path("lake"), cfg.path("checkpoint"), fmt="csv").run_available()
    paths = render_all(cfg, cfg.path("images"))
    assert len(paths) == 6
    assert all(p.exists() and p.stat().st_size > 10_000 for p in paths)
    s = summarise(cfg.path("lake"), load_progress(cfg.path("checkpoint")))
    assert s["check_windows_equal_aggregated"] and s["check_sessions_equal_aggregated"]
    assert s["records_read"] == s["valid_records"] + s["dead_letters"]
    assert 0 < s["conversion_rate"] < 1


def test_report_without_progress_log_skips_the_batch_chart(cfg, source, tmp_path):
    """A lake written by Spark has no local progress log; the other charts still render."""
    StreamingQuery(cfg, source, cfg.path("lake"), cfg.path("checkpoint"), fmt="csv").run_available()
    paths = render_all(cfg, tmp_path / "img", cfg.path("lake"), tmp_path / "no-checkpoint")
    assert len(paths) == 5 and not any(p.name == "microbatch_progress.png" for p in paths)
