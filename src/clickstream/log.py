"""A partitioned, append-only local log that behaves like a Kafka topic.

Each partition is a JSON-lines file; a record's line number is its offset. Readers track their
own offsets (the engine checkpoints them), so the log itself is never mutated by consumers.
This keeps the demo runnable without a broker while preserving the semantics that matter:
per-key ordering, independent partitions, replay from any offset.
"""
from __future__ import annotations

import json
import zlib
from pathlib import Path
from typing import Iterable

from .generator import Record


def partition_for(key: str, partitions: int) -> int:
    return zlib.crc32(key.encode()) % partitions


class LocalLog:
    def __init__(self, root: str | Path, topic: str, partitions: int = 3):
        self.dir = Path(root) / f"topic={topic}"
        self.partitions = partitions

    def _file(self, p: int) -> Path:
        return self.dir / f"partition={p}.jsonl"

    def append(self, records: Iterable[Record]) -> dict[int, int]:
        """Append records and return how many landed in each partition."""
        self.dir.mkdir(parents=True, exist_ok=True)
        handles = {p: self._file(p).open("a") for p in range(self.partitions)}
        counts = dict.fromkeys(range(self.partitions), 0)
        try:
            for rec in records:
                p = rec.partition if rec.partition is not None else partition_for(rec.key, self.partitions)
                handles[p].write(json.dumps({"key": rec.key, "arrival": rec.arrival.isoformat(),
                                             "value": rec.value}) + "\n")
                counts[p] += 1
        finally:
            for h in handles.values():
                h.close()
        return counts

    def end_offsets(self) -> dict[int, int]:
        out = {}
        for p in range(self.partitions):
            f = self._file(p)
            out[p] = sum(1 for _ in f.open()) if f.exists() else 0
        return out

    def read(self, partition: int, start: int, end: int) -> list[tuple[int, dict]]:
        """Records with offsets in [start, end)."""
        f = self._file(partition)
        if not f.exists() or end <= start:
            return []
        out = []
        with f.open() as fh:
            for offset, line in enumerate(fh):
                if offset >= end:
                    break
                if offset >= start:
                    out.append((offset, json.loads(line)))
        return out
