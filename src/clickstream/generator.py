"""Deterministic synthetic clickstream with the faults real event streams have.

Users browse in sessions that may progress through the funnel
page_view → add_to_cart → checkout → purchase. Events are then *delivered* the way a mobile and
web fleet delivers them: mostly within seconds, some buffered for minutes, a few far beyond the
watermark, some twice (producer retries), some malformed and some with a broken device clock.

All of this is synthetic. A fixed seed makes every run reproducible.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np

from .config import GeneratorSettings
from .schema import format_ts

PAGES = ("/", "/search", "/category/shoes", "/category/electronics", "/category/home", "/deals", "/product")
DEVICES = ("web", "android", "ios")
CATEGORIES = {"shoes": (1499, 6999), "electronics": (2999, 49999), "home": (499, 8999), "books": (199, 1299)}


@dataclass(frozen=True)
class Record:
    """One message as a broker would hold it: key, payload and when it arrived."""
    key: str
    value: str
    arrival: datetime
    partition: int | None = None  # None: chosen from the key, like Kafka's default partitioner


def _catalogue(rng: np.random.Generator) -> list[tuple[str, str, float]]:
    items = []
    for cat, (lo, hi) in CATEGORIES.items():
        for i in range(10):
            items.append((f"{cat[:2].upper()}-{i:03d}", cat, float(round(rng.uniform(lo, hi), -1) - 1)))
    return items


def _session_events(rng, user_id, start, device, catalogue):
    """Yield (event_type, event_time, page, product, price) for one browsing session."""
    t = start
    n_views = int(rng.integers(1, 9))
    product = catalogue[int(rng.integers(len(catalogue)))]
    out = []
    for _ in range(n_views):
        page = PAGES[int(rng.integers(len(PAGES)))]
        out.append(("page_view", t, page, product[0] if page == "/product" else None, None))
        t += timedelta(seconds=float(rng.uniform(5, 120)))
    if rng.random() < 0.32:
        out.append(("add_to_cart", t, "/product", product[0], product[2]))
        t += timedelta(seconds=float(rng.uniform(10, 240)))
        if rng.random() < 0.55:
            out.append(("checkout", t, "/checkout", product[0], product[2]))
            t += timedelta(seconds=float(rng.uniform(20, 180)))
            if rng.random() < 0.72:
                out.append(("purchase", t, "/checkout/success", product[0], product[2]))
    return out


def _malform(rng, payload: dict) -> str:
    kind = int(rng.integers(5))
    if kind == 0:  # truncated message; sent_at is the last key, so it is always lost
        text = json.dumps(payload)
        return text[: len(text) // 2]
    bad = dict(payload)
    if kind == 1:
        bad["user_id"] = None
    elif kind == 2:
        bad["event_type"] = "pageview"  # a client on an old SDK
    elif kind == 3:
        bad["event_time"] = "05/01/2026 09:12"
    else:
        bad["event_type"], bad["price"] = "purchase", -abs(bad.get("price") or 499.0)
    return json.dumps(bad)


def generate(cfg: GeneratorSettings) -> list[Record]:
    """Return every message in arrival order."""
    rng = np.random.default_rng(cfg.seed)
    uid = uuid.UUID(int=cfg.seed)
    catalogue = _catalogue(rng)
    start, horizon = cfg.start_dt, timedelta(minutes=cfg.minutes)

    events = []
    for u in range(cfg.users):
        user_id = f"u{u:05d}"
        device = DEVICES[int(rng.choice(3, p=[0.45, 0.35, 0.20]))]
        for _ in range(int(rng.poisson(1.3)) + 1):
            if rng.random() < 0.22:  # campaign traffic
                offset = cfg.campaign_start_minute + rng.uniform(0, cfg.campaign_minutes)
            else:
                offset = rng.uniform(0, cfg.minutes - 15)
            s_start = start + timedelta(minutes=float(offset))
            for ev in _session_events(rng, user_id, s_start, device, catalogue):
                if ev[1] < start + horizon:
                    events.append((user_id, device, *ev))

    records: list[Record] = []
    counter = 0
    for user_id, device, etype, etime, page, product, price in events:
        counter += 1
        r = rng.random()
        if r < cfg.late_beyond_watermark:
            delay = timedelta(minutes=float(rng.uniform(12, 40)))
        elif r < cfg.late_beyond_watermark + cfg.late_within_watermark:
            delay = timedelta(seconds=float(rng.uniform(30, 480)))
        else:
            delay = timedelta(seconds=float(rng.exponential(2.0)))
        arrival = etime + delay
        claimed_time = etime
        if rng.random() < cfg.clock_skew_rate:
            claimed_time = etime + timedelta(hours=float(rng.uniform(2, 6)))
        payload = {
            "event_id": str(uuid.uuid5(uid, str(counter))),
            "user_id": user_id,
            "event_type": etype,
            "event_time": format_ts(claimed_time),
            "page": page,
            "product_id": product,
            "price": price,
            "device": device,
            "sent_at": format_ts(arrival),
        }
        value = _malform(rng, payload) if rng.random() < cfg.malformed_rate else json.dumps(payload)
        records.append(Record(user_id, value, arrival))
        if rng.random() < cfg.duplicate_rate:  # retry after a lost ack: same event_id, later arrival
            records.append(Record(user_id, value, arrival + timedelta(seconds=float(rng.uniform(1, 30)))))

    records.sort(key=lambda rec: rec.arrival)
    return records


def heartbeats(after: datetime, partitions: int, offset: timedelta) -> list[Record]:
    """Control events that push the watermark past the end of the stream.

    A real stream never ends, so open windows close when later traffic arrives. A finite demo
    stream ends instead; these heartbeats play the part of that later traffic.
    """
    out = []
    for p in range(partitions):
        t = after + offset
        out.append(Record(f"__heartbeat_{p}", json.dumps({
            "event_id": f"heartbeat-{p}-{int(t.timestamp())}", "user_id": "__system__",
            "event_type": "heartbeat", "event_time": format_ts(t), "sent_at": format_ts(t),
        }), t, partition=p))
    return out
