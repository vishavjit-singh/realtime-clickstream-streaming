"""Stateful streaming operators with Spark Structured Streaming semantics, in plain Python.

One call to :func:`process_batch` is one micro-batch:

1. parse + validate every record; failures go to the dead-letter output with a reason
2. drop events older than the current watermark (late) and repeated ``event_id`` s (duplicates)
3. update 1-minute tumbling-window state and 30-minute session-window state
4. advance the watermark to ``max(event_time) - delay``
5. emit (append mode) every window and session that the watermark has closed, then forget it

The watermark used to drop late rows is the one computed at the end of the *previous* batch,
exactly as Spark does. State is a plain dict so it can be checkpointed as JSON.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from .config import Settings
from .schema import validate

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def window_start(t: datetime, size: timedelta) -> datetime:
    return EPOCH + ((t - EPOCH) // size) * size


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def new_state() -> dict:
    return {"watermark": None, "seen": {}, "windows": {}, "sessions": {}}


@dataclass
class BatchResult:
    events: list[dict] = field(default_factory=list)        # bronze rows
    dead_letters: list[dict] = field(default_factory=list)
    metrics: list[dict] = field(default_factory=list)       # closed tumbling windows
    sessions: list[dict] = field(default_factory=list)      # closed sessions
    counters: dict = field(default_factory=dict)


def _merge_session(user_sessions: list[dict], t: datetime, ev, gap: timedelta) -> None:
    """Add one event to a user's open sessions, merging any sessions it bridges."""
    lo, hi = t, t + gap
    overlapping = [s for s in user_sessions if _dt(s["start"]) <= hi and t <= _dt(s["end"])]
    merged = {"start": _iso(lo), "end": _iso(hi), "events": 0, "page_view": 0, "add_to_cart": 0,
              "checkout": 0, "purchase": 0, "revenue": 0.0}
    for s in overlapping:
        user_sessions.remove(s)
        merged["start"] = _iso(min(_dt(merged["start"]), _dt(s["start"])))
        merged["end"] = _iso(max(_dt(merged["end"]), _dt(s["end"])))
        for k in ("events", "page_view", "add_to_cart", "checkout", "purchase", "revenue"):
            merged[k] += s[k]
    merged["events"] += 1
    merged[ev.event_type] += 1
    merged["revenue"] = round(merged["revenue"] + ev.revenue, 2)
    user_sessions.append(merged)


def process_batch(state: dict, records: list[dict], cfg: Settings, batch_id: int = 0) -> BatchResult:
    """Run one micro-batch. ``records`` are log entries: ``{partition, offset, value, ...}``.

    ``state`` is updated in place; pass a copy if the previous state must be kept.
    """
    res = BatchResult()
    c = dict.fromkeys(("input", "valid", "dead_letter", "late_dropped", "duplicates", "aggregated"), 0)
    watermark = _dt(state["watermark"]) if state["watermark"] else None
    max_event_time = None
    on_time = []

    for rec in records:
        c["input"] += 1
        ev, reason = validate(rec["value"], cfg.max_future_skew)
        meta = {"kafka_partition": rec["partition"], "kafka_offset": rec["offset"], "ingest_batch": batch_id}
        if ev is None:
            c["dead_letter"] += 1
            res.dead_letters.append({"raw_value": rec["value"], "reason": reason, **meta})
            continue
        c["valid"] += 1
        max_event_time = ev.event_time if max_event_time is None else max(max_event_time, ev.event_time)
        if not ev.is_control:
            res.events.append({
                "event_id": ev.event_id, "user_id": ev.user_id, "event_type": ev.event_type,
                "event_time": ev.event_time, "sent_at": ev.sent_at, "page": ev.page,
                "product_id": ev.product_id, "price": ev.price, "device": ev.device, **meta,
            })
        if watermark is not None and ev.event_time < watermark:
            c["late_dropped"] += 1
            continue
        if ev.event_id in state["seen"]:
            c["duplicates"] += 1
            continue
        state["seen"][ev.event_id] = _iso(ev.event_time)
        if not ev.is_control:
            on_time.append(ev)

    for ev in on_time:
        c["aggregated"] += 1
        ws = window_start(ev.event_time, cfg.window)
        key = f"{_iso(ws)}|{ev.event_type}"
        w = state["windows"].setdefault(key, {"events": 0, "users": [], "revenue": 0.0})
        w["events"] += 1
        if ev.user_id not in w["users"]:
            w["users"].append(ev.user_id)
        w["revenue"] = round(w["revenue"] + ev.revenue, 2)
        _merge_session(state["sessions"].setdefault(ev.user_id, []), ev.event_time, ev, cfg.session_gap)

    # Advance the watermark, then emit everything it has closed (Spark's append output mode).
    if max_event_time is not None:
        candidate = max_event_time - cfg.watermark
        if watermark is None or candidate > watermark:
            watermark = candidate
            state["watermark"] = _iso(watermark)

    if watermark is not None:
        for key in sorted(state["windows"]):
            start_s, etype = key.split("|")
            start = _dt(start_s)
            if start + cfg.window <= watermark:
                w = state["windows"].pop(key)
                res.metrics.append({"window_start": start, "window_end": start + cfg.window, "event_type": etype,
                                    "events": w["events"], "users": len(w["users"]), "revenue": w["revenue"]})
        for user_id in sorted(state["sessions"]):
            keep = []
            for s in state["sessions"][user_id]:
                if _dt(s["end"]) <= watermark:
                    start, end = _dt(s["start"]), _dt(s["end"])
                    res.sessions.append({
                        "user_id": user_id, "session_start": start, "session_end": end,
                        "duration_s": (end - start - cfg.session_gap).total_seconds(),
                        "events": s["events"], "page_views": s["page_view"], "add_to_cart": s["add_to_cart"],
                        "checkouts": s["checkout"], "purchases": s["purchase"], "revenue": s["revenue"],
                        "converted": s["purchase"] > 0,
                    })
                else:
                    keep.append(s)
            if keep:
                state["sessions"][user_id] = keep
            else:
                del state["sessions"][user_id]
        # Deduplication state only needs to outlive the watermark.
        state["seen"] = {k: v for k, v in state["seen"].items() if _dt(v) + cfg.watermark >= watermark}

    c["windows_emitted"] = len(res.metrics)
    c["sessions_emitted"] = len(res.sessions)
    c["state_windows"] = len(state["windows"])
    c["state_sessions"] = sum(len(v) for v in state["sessions"].values())
    c["watermark"] = state["watermark"]
    res.counters = c
    return res
