from __future__ import annotations

from dataclasses import replace

import pytest

from clickstream.config import GeneratorSettings, load_settings
from clickstream.generator import generate, heartbeats
from clickstream.log import LocalLog


@pytest.fixture
def cfg(tmp_path):
    """Small, fast settings that write everything under tmp_path."""
    base = load_settings()
    gen = GeneratorSettings(seed=11, users=150, minutes=60, campaign_start_minute=30, campaign_minutes=10)
    return replace(base, generator=gen, max_records_per_trigger=200, paths={
        "source": str(tmp_path / "source"), "lake": str(tmp_path / "lake"),
        "checkpoint": str(tmp_path / "ckpt"), "images": str(tmp_path / "images"),
    })


def closed_stream(cfg):
    records = generate(cfg.generator)
    tail = cfg.watermark + cfg.session_gap
    records += heartbeats(records[-1].arrival, cfg.partitions, tail)
    return records


@pytest.fixture
def source(cfg):
    log = LocalLog(cfg.path("source"), cfg.topic, cfg.partitions)
    log.append(closed_stream(cfg))
    return log
