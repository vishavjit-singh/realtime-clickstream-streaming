"""Partitioned lake tables written per micro-batch.

Every batch writes ``<table>/dt=YYYY-MM-DD/part-<batch>.<ext>``. Re-running a batch overwrites
the same files, which is what makes recovery idempotent. Parquet is used when ``pyarrow`` is
installed (it is in requirements.txt); otherwise the engine falls back to CSV so the demo still
runs in a bare environment.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

TIME_COLUMNS = {
    "events": ("event_time", "sent_at"),
    "dead_letter": (),
    "metrics_minute": ("window_start", "window_end"),
    "sessions": ("session_start", "session_end"),
}
PARTITION_SOURCE = {"events": "event_time", "metrics_minute": "window_start", "sessions": "session_start"}


def default_format() -> str:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return "csv"
    return "parquet"


def write_batch(lake: Path, table: str, rows: list[dict], batch_id: int, fmt: str, dt_fallback: str) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    for col in TIME_COLUMNS[table]:
        df[col] = pd.to_datetime(df[col], utc=True)
    src = PARTITION_SOURCE.get(table)
    df["dt"] = df[src].dt.strftime("%Y-%m-%d") if src else dt_fallback
    for dt, part in df.groupby("dt", sort=True):
        out = lake / table / f"dt={dt}"
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"part-{batch_id:05d}.{fmt}"
        part = part.drop(columns="dt")
        if fmt == "parquet":
            part.to_parquet(path, index=False)
        else:
            part.to_csv(path, index=False)
    return len(df)


def read_table(lake: str | Path, table: str) -> pd.DataFrame:
    """Read every partition of a table (written by the local engine or by Spark)."""
    root = Path(lake) / table
    frames = []
    for f in sorted(root.glob("dt=*/*")):
        if f.suffix == ".csv":
            df = pd.read_csv(f)
        elif f.suffix == ".parquet":
            df = pd.read_parquet(f)
        else:
            continue
        df["dt"] = f.parent.name.split("=", 1)[1]
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    for col in TIME_COLUMNS.get(table, ()):
        if col in df:
            df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")
    return df
