"""Command line: ``python -m clickstream <command>``."""
from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path

import click

from .config import load_settings
from .engine import StreamingQuery, load_progress
from .generator import generate, heartbeats
from .log import LocalLog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@click.group()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False), default=None,
              help="Path to streaming.yaml (defaults to config/streaming.yaml).")
@click.pass_context
def cli(ctx, config_path):
    """Real-time clickstream pipeline: generate, stream, aggregate, report."""
    ctx.obj = load_settings(config_path)


def _write_source(cfg, close: bool):
    records = generate(cfg.generator)
    if close:
        tail = cfg.watermark + cfg.session_gap + timedelta(minutes=5)
        records += heartbeats(records[-1].arrival, cfg.partitions, tail)
        records += heartbeats(records[-1].arrival, cfg.partitions, tail)
    return records


@cli.command("generate")
@click.option("--close/--no-close", default=True, help="Append heartbeats so every window is emitted.")
@click.pass_obj
def generate_cmd(cfg, close):
    """Write the synthetic stream into the local partitioned log (replacing any previous one)."""
    src = cfg.path("source")
    shutil.rmtree(src, ignore_errors=True)
    records = _write_source(cfg, close)
    counts = LocalLog(src, cfg.topic, cfg.partitions).append(records)
    click.echo(f"wrote {len(records):,} records to {src} · per partition {counts}")


@cli.command("run")
@click.option("--max-batches", type=int, default=None, help="Stop after N micro-batches (resume later).")
@click.option("--format", "fmt", type=click.Choice(["parquet", "csv"]), default=None)
@click.pass_obj
def run_cmd(cfg, max_batches, fmt):
    """Run the local streaming engine over everything available (Trigger.AvailableNow)."""
    query = StreamingQuery(cfg, LocalLog(cfg.path("source"), cfg.topic, cfg.partitions),
                           cfg.path("lake"), cfg.path("checkpoint"), fmt)
    progress = query.run_available(max_batches)
    if not progress:
        click.echo("no new data: nothing to do")
        return
    for p in progress:
        click.echo(f"batch {p['batch_id']:>3}  in={p['input']:>4}  dlq={p['dead_letter']:>2}  "
                   f"late={p['late_dropped']:>2}  dup={p['duplicates']:>2}  windows={p['windows_emitted']:>3}  "
                   f"sessions={p['sessions_emitted']:>4}  watermark={p['watermark']}")


@cli.command("summary")
@click.pass_obj
def summary_cmd(cfg):
    """Print headline numbers from the lake and the checkpoint."""
    from .report import summarise

    click.echo(json.dumps(summarise(cfg.path("lake"), load_progress(cfg.path("checkpoint"))), indent=2))


@cli.command("report")
@click.option("--out", type=click.Path(file_okay=False), default=None)
@click.option("--lake", type=click.Path(file_okay=False), default=None, help="Defaults to the local engine's lake.")
@click.option("--checkpoint", type=click.Path(file_okay=False), default=None)
@click.pass_obj
def report_cmd(cfg, out, lake, checkpoint):
    """Render the README charts from the lake tables and streaming progress."""
    from .report import render_all

    paths = render_all(cfg, Path(out) if out else cfg.path("images"), lake, checkpoint)
    for p in paths:
        click.echo(f"wrote {p}")


@cli.command("export-files")
@click.option("--out", type=click.Path(file_okay=False), default="data/spark_input")
@click.option("--chunk", type=int, default=None, help="Records per file (default: max_records_per_trigger).")
@click.pass_obj
def export_files_cmd(cfg, out, chunk):
    """Write the stream as numbered text files for Spark's file source (one file = one micro-batch)."""
    n = export_files(_write_source(cfg, close=False), cfg, Path(out), chunk or cfg.max_records_per_trigger)
    click.echo(f"wrote {n} files to {out}")


def export_files(records, cfg, out: Path, chunk: int) -> int:
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    chunks = [records[i:i + chunk] for i in range(0, len(records), chunk)]
    last = records[-1].arrival
    # Each heartbeat round gets its own file, hence its own micro-batch.
    tail = cfg.watermark + cfg.session_gap + timedelta(minutes=5)
    chunks.append(heartbeats(last, cfg.partitions, tail))
    chunks.append(heartbeats(last, cfg.partitions, 2 * tail))
    base = 1_700_000_000
    for i, part in enumerate(chunks):
        f = out / f"batch-{i:05d}.json"
        f.write_text("".join(r.value + "\n" for r in part))
        os.utime(f, (base + i * 10, base + i * 10))  # Spark orders files by modification time
    return len(chunks)


@cli.command("produce")
@click.option("--bootstrap", default=lambda: os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
@click.option("--speedup", type=float, default=60.0, help="Event-time seconds replayed per wall-clock second.")
@click.pass_obj
def produce_cmd(cfg, bootstrap, speedup):
    """Replay the synthetic stream into Kafka, then close it with heartbeats."""
    from .kafka_io import close_stream, make_producer, replay

    records = generate(cfg.generator)
    producer = make_producer(bootstrap)
    sent = replay(records, producer, cfg.topic, speedup)
    close_stream(producer, cfg.topic, records[-1].arrival, cfg.partitions)
    click.echo(f"produced {sent:,} records to {cfg.topic}@{bootstrap}")


@cli.command("spark")
@click.option("--source", type=click.Choice(["kafka", "files"]), default="kafka")
@click.option("--bootstrap", default=lambda: os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
@click.option("--files", "files_dir", default="data/spark_input")
@click.option("--lake", default="data/spark_lake")
@click.option("--checkpoint", default="data/spark_checkpoints")
@click.option("--trigger", default="available_now", help="'available_now' or an interval like '10 seconds'.")
@click.pass_obj
def spark_cmd(cfg, source, bootstrap, files_dir, lake, checkpoint, trigger):
    """Run the Spark Structured Streaming job (needs pyspark and a JVM)."""
    from .spark_job import run

    run(cfg, source, lake, checkpoint, bootstrap=bootstrap, files=files_dir, trigger=trigger)


def main() -> None:  # pragma: no cover
    cli(prog_name="clickstream")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
