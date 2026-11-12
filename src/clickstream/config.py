"""Typed settings loaded from config/streaming.yaml."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "config" / "streaming.yaml"


@dataclass(frozen=True)
class GeneratorSettings:
    seed: int = 7
    users: int = 1500
    minutes: int = 180
    start: str = "2026-01-05T09:00:00+00:00"
    campaign_start_minute: int = 95
    campaign_minutes: int = 25
    late_within_watermark: float = 0.07
    late_beyond_watermark: float = 0.01
    duplicate_rate: float = 0.015
    malformed_rate: float = 0.008
    clock_skew_rate: float = 0.002

    @property
    def start_dt(self) -> datetime:
        return datetime.fromisoformat(self.start)


@dataclass(frozen=True)
class Settings:
    topic: str = "clickstream"
    partitions: int = 3
    watermark_minutes: float = 10
    window_minutes: float = 1
    session_gap_minutes: float = 30
    max_future_skew_minutes: float = 5
    max_records_per_trigger: int = 600
    paths: dict = field(default_factory=lambda: {
        "source": "data/source", "lake": "data/lake",
        "checkpoint": "data/checkpoints", "images": "docs/images",
    })
    generator: GeneratorSettings = field(default_factory=GeneratorSettings)

    @property
    def watermark(self) -> timedelta:
        return timedelta(minutes=self.watermark_minutes)

    @property
    def window(self) -> timedelta:
        return timedelta(minutes=self.window_minutes)

    @property
    def session_gap(self) -> timedelta:
        return timedelta(minutes=self.session_gap_minutes)

    @property
    def max_future_skew(self) -> timedelta:
        return timedelta(minutes=self.max_future_skew_minutes)

    def path(self, key: str) -> Path:
        p = Path(self.paths[key])
        return p if p.is_absolute() else ROOT / p


def load_settings(path: str | Path | None = None) -> Settings:
    raw = yaml.safe_load(Path(path or DEFAULT_CONFIG).read_text()) or {}
    gen = GeneratorSettings(**raw.pop("generator", {}))
    return Settings(generator=gen, **raw)
