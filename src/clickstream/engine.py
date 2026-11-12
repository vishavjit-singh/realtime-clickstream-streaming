"""Micro-batch runner with a write-ahead offset log and checkpointed state.

The checkpoint mirrors Spark's layout and protocol:

``offsets/<n>.json``  planned offset range for batch *n*, written **before** the batch runs
``state/<n>.json``    operator state after batch *n*
``commits/<n>.json``  written **after** every sink has been written
``progress/<n>.json`` per-batch metrics (like ``StreamingQueryProgress``)

On restart, a batch that has an offsets entry but no commit is re-run over exactly the same
offsets, starting from the state of the last committed batch. Sinks overwrite their per-batch
files, so a crash anywhere in a batch yields neither lost nor duplicated output.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .config import Settings
from .log import LocalLog
from .operators import new_state, process_batch
from .sinks import default_format, write_batch


class SimulatedCrash(RuntimeError):
    """Raised by the fault-injection hook in tests."""


def _write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=1, default=str))
    tmp.replace(path)  # atomic rename, so a reader never sees half a file


def _latest(folder: Path) -> int:
    ids = [int(p.stem) for p in folder.glob("*.json")] if folder.exists() else []
    return max(ids, default=-1)


class StreamingQuery:
    def __init__(self, cfg: Settings, log: LocalLog, lake: Path, checkpoint: Path, fmt: str | None = None,
                 after_sinks: Callable[[int], None] | None = None):
        self.cfg, self.log = cfg, log
        self.lake, self.ckpt = Path(lake), Path(checkpoint)
        self.fmt = fmt or default_format()
        self.after_sinks = after_sinks  # fault-injection hook for tests

    # -- checkpoint helpers -------------------------------------------------------------
    def _load_state(self, batch_id: int) -> dict:
        f = self.ckpt / "state" / f"{batch_id}.json"
        return json.loads(f.read_text()) if f.exists() else new_state()

    def _plan(self, start: dict[int, int]) -> dict[int, int] | None:
        """Pick end offsets for the next batch, spreading the record budget over partitions."""
        latest = self.log.end_offsets()
        backlog = {p: latest[p] - start.get(p, 0) for p in latest}
        total = sum(backlog.values())
        if total <= 0:
            return None
        budget = self.cfg.max_records_per_trigger
        end = {}
        for p, lag in backlog.items():
            take = lag if total <= budget else max(1, round(budget * lag / total)) if lag else 0
            end[p] = start.get(p, 0) + min(lag, take)
        return end

    # -- main loop ----------------------------------------------------------------------
    def run_available(self, max_batches: int | None = None) -> list[dict]:
        """Process everything currently in the log (Spark's ``Trigger.AvailableNow``)."""
        committed = _latest(self.ckpt / "commits")
        planned = _latest(self.ckpt / "offsets")
        progress = []
        batch_id = committed + 1
        while max_batches is None or len(progress) < max_batches:
            if batch_id <= planned:  # recovery: replay the logged plan exactly
                plan = json.loads((self.ckpt / "offsets" / f"{batch_id}.json").read_text())
                start = {int(k): v for k, v in plan["start"].items()}
                end = {int(k): v for k, v in plan["end"].items()}
            else:
                prev = self.ckpt / "offsets" / f"{batch_id - 1}.json"
                start = ({int(k): v for k, v in json.loads(prev.read_text())["end"].items()}
                         if prev.exists() else {p: 0 for p in range(self.log.partitions)})
                end = self._plan(start)
                if end is None:
                    break
                _write_json(self.ckpt / "offsets" / f"{batch_id}.json", {"start": start, "end": end})
            progress.append(self._run_batch(batch_id, start, end))
            batch_id += 1
        return progress

    def _run_batch(self, batch_id: int, start: dict[int, int], end: dict[int, int]) -> dict:
        t0 = time.perf_counter()
        state = self._load_state(batch_id - 1)
        records = []
        for p in sorted(end):
            for offset, rec in self.log.read(p, start.get(p, 0), end[p]):
                records.append({"partition": p, "offset": offset, **rec})
        records.sort(key=lambda r: (r["arrival"], r["partition"], r["offset"]))

        res = process_batch(state, records, self.cfg, batch_id)
        ingest_dt = records[-1]["arrival"][:10] if records else "1970-01-01"
        write_batch(self.lake, "events", res.events, batch_id, self.fmt, ingest_dt)
        write_batch(self.lake, "dead_letter", res.dead_letters, batch_id, self.fmt, ingest_dt)
        write_batch(self.lake, "metrics_minute", res.metrics, batch_id, self.fmt, ingest_dt)
        write_batch(self.lake, "sessions", res.sessions, batch_id, self.fmt, ingest_dt)
        if self.after_sinks:
            self.after_sinks(batch_id)

        _write_json(self.ckpt / "state" / f"{batch_id}.json", state)
        progress = {
            "batch_id": batch_id,
            "offsets": {"start": start, "end": end},
            "last_arrival": records[-1]["arrival"] if records else None,
            "duration_ms": round((time.perf_counter() - t0) * 1000, 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **res.counters,
        }
        _write_json(self.ckpt / "progress" / f"{batch_id}.json", progress)
        _write_json(self.ckpt / "commits" / f"{batch_id}.json", {"batch_id": batch_id})
        # Older state snapshots are no longer needed once a newer batch has committed.
        old = self.ckpt / "state" / f"{batch_id - 2}.json"
        if old.exists():
            old.unlink()
        return progress


def load_progress(checkpoint: Path) -> list[dict]:
    folder = Path(checkpoint) / "progress"
    return [json.loads(f.read_text()) for f in sorted(folder.glob("*.json"), key=lambda p: int(p.stem))]
