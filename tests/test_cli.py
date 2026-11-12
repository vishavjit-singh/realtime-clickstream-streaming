import os

from click.testing import CliRunner

from clickstream.cli import cli, export_files
from tests.conftest import closed_stream


def test_export_files_orders_batches_by_mtime_with_heartbeats_last(cfg, tmp_path):
    n = export_files(closed_stream(cfg), cfg, tmp_path / "in", 250)
    files = sorted((tmp_path / "in").iterdir(), key=lambda f: os.stat(f).st_mtime)
    assert len(files) == n
    assert [f.name for f in files] == sorted(f.name for f in files)
    assert "heartbeat" in files[-1].read_text() and "heartbeat" in files[-2].read_text()


def test_generate_run_summary(tmp_path):
    cfg_file = tmp_path / "streaming.yaml"
    cfg_file.write_text(f"""
max_records_per_trigger: 400
paths: {{source: {tmp_path}/src, lake: {tmp_path}/lake, checkpoint: {tmp_path}/ckpt, images: {tmp_path}/img}}
generator: {{users: 80, minutes: 40, campaign_start_minute: 20, campaign_minutes: 5}}
""")
    runner = CliRunner()
    for args in (["generate"], ["run", "--format", "csv"], ["summary"]):
        result = runner.invoke(cli, ["--config", str(cfg_file), *args])
        assert result.exit_code == 0, result.output
    assert '"check_windows_equal_aggregated": true' in result.output
    again = runner.invoke(cli, ["--config", str(cfg_file), "run"])
    assert "nothing to do" in again.output
