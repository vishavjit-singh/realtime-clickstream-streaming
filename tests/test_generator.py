import json
from collections import Counter
from dataclasses import replace

from clickstream.generator import generate, heartbeats
from clickstream.schema import validate


def test_deterministic(cfg):
    a, b = generate(cfg.generator), generate(cfg.generator)
    assert [r.value for r in a] == [r.value for r in b]
    c = generate(replace(cfg.generator, seed=99))
    assert [r.value for r in a] != [r.value for r in c]


def test_records_are_in_arrival_order(cfg):
    arrivals = [r.arrival for r in generate(cfg.generator)]
    assert arrivals == sorted(arrivals)


def test_stream_contains_the_faults_it_promises(cfg):
    gen = replace(cfg.generator, users=600, malformed_rate=0.03, duplicate_rate=0.03, clock_skew_rate=0.01)
    records = generate(gen)
    reasons = Counter(validate(r.value)[1] for r in records)
    assert reasons[None] > 0.9 * len(records)
    assert len([k for k in reasons if k]) >= 5  # most rejection rules are exercised
    ids = Counter(json.loads(r.value)["event_id"] for r in records if validate(r.value)[0])
    assert any(n > 1 for n in ids.values())  # duplicates share an event_id


def test_late_events_exist_beyond_the_watermark(cfg):
    late = 0
    for r in generate(cfg.generator):
        ev, _ = validate(r.value)
        if ev and (ev.sent_at - ev.event_time) > cfg.watermark:
            late += 1
    assert late > 0


def test_heartbeats_cover_every_partition(cfg):
    hb = heartbeats(generate(cfg.generator)[-1].arrival, 3, cfg.watermark)
    assert sorted(r.partition for r in hb) == [0, 1, 2]
    assert all(validate(r.value)[0].is_control for r in hb)
