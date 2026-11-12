"""Render the README images from the lake tables and the streaming progress log.

Every number in these charts is computed from what the pipeline actually wrote.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

from .config import Settings  # noqa: E402
from .engine import load_progress  # noqa: E402
from .schema import EVENT_TYPES  # noqa: E402
from .sinks import read_table  # noqa: E402

INK = "#0e1a2b"
MUTED = "#5b6676"
GRID = "#e3e7ee"
ACCENT = "#2a78d6"
SIGNAL = "#eda100"
BAD = "#e34948"
GOOD = "#1baf7a"
SERIES = {"page_view": "#2a78d6", "add_to_cart": "#eb6834", "checkout": "#1baf7a", "purchase": "#6250d6"}

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "axes.edgecolor": GRID,
    "axes.labelcolor": MUTED,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "axes.titleweight": "bold",
    "axes.titlesize": 13,
    "axes.titlecolor": INK,
    "axes.titlelocation": "left",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.dpi": 150,
})


def _inr(x: float, _pos=None) -> str:
    if abs(x) >= 1e5:
        return f"₹{x / 1e5:.1f} L"
    if abs(x) >= 1e3:
        return f"₹{x / 1e3:.0f}k"
    return f"₹{x:,.0f}"


# ---------------------------------------------------------------------------------------
def summarise(lake: Path, progress: list[dict]) -> dict:
    events = read_table(lake, "events")
    dead = read_table(lake, "dead_letter")
    metrics = read_table(lake, "metrics_minute")
    sessions = read_table(lake, "sessions")
    total = lambda k: int(sum(p.get(k, 0) for p in progress))  # noqa: E731
    out = {
        "micro_batches": len(progress),
        "records_read": total("input"),
        "valid_records": total("valid"),
        "dead_letters": total("dead_letter"),
        "dead_letters_by_reason": dead["reason"].value_counts().to_dict() if len(dead) else {},
        "duplicates_dropped": total("duplicates"),
        "late_dropped": total("late_dropped"),
        "events_aggregated": total("aggregated"),
        "bronze_events": int(len(events)),
        "minute_windows": int(len(metrics)),
        "sessions": int(len(sessions)),
        "converted_sessions": int(sessions["converted"].astype(bool).sum()) if len(sessions) else 0,
        "revenue_inr": round(float(sessions["revenue"].sum()), 2) if len(sessions) else 0.0,
        "batch_ms_p50": round(float(pd.Series([p["duration_ms"] for p in progress]).median()), 1) if progress else None,
    }
    if out["sessions"]:
        out["conversion_rate"] = round(out["converted_sessions"] / out["sessions"], 4)
    # Every on-time, de-duplicated event ends up in exactly one window and one session.
    if len(metrics) and len(sessions):
        out["check_windows_equal_aggregated"] = int(metrics["events"].sum()) == out["events_aggregated"]
        out["check_sessions_equal_aggregated"] = int(sessions["events"].sum()) == out["events_aggregated"]
    return out


# ---------------------------------------------------------------------------------------
def architecture(out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(13, 5.6))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 6.0)
    ax.axis("off")

    def box(x, y, w, h, title, sub, color=INK):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                    fc="white", ec=color, lw=2))
        ax.text(x + w / 2, y + h * 0.66, title, ha="center", va="center", fontsize=11.5, weight="bold", color=color)
        ax.text(x + w / 2, y + h * 0.3, sub, ha="center", va="center", fontsize=8.5, color=MUTED, linespacing=1.4)

    def arrow(x1, y1, x2, y2, color=MUTED):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, lw=1.6, color=color))

    ax.text(0.15, 5.75, "realtime-clickstream-streaming: event time, watermarks, exactly-once sinks",
            fontsize=14, weight="bold", color=INK)
    box(0.2, 2.2, 1.9, 1.3, "Web & app", "synthetic users\nlate · dupes · bad JSON")
    box(2.6, 2.2, 1.9, 1.3, "Kafka topic", "clickstream\n3 partitions, key=user", ACCENT)
    box(5.0, 2.2, 2.0, 1.3, "Parse + validate", "from_json · 6 rules")
    box(7.6, 3.55, 2.3, 1.25, "Watermark 10 min", "drop late · dedupe\nevent_id", SIGNAL)
    box(5.0, 0.35, 2.0, 1.2, "dead_letter", "raw value + reason", BAD)
    box(7.6, 1.95, 2.3, 1.2, "events (bronze)", "every valid event\npartitioned by dt")
    box(10.5, 3.9, 2.3, 1.15, "metrics_minute", "1-min tumbling window\nevents · users · revenue", ACCENT)
    box(10.5, 2.35, 2.3, 1.15, "sessions", "30-min session window\nfunnel + revenue", ACCENT)
    box(10.5, 0.35, 2.3, 1.2, "Reports", "charts in docs/images\n(this README)")
    box(7.6, 0.35, 2.3, 1.2, "Checkpoint", "offsets WAL · state\ncommits")

    arrow(2.1, 2.85, 2.6, 2.85)
    arrow(4.5, 2.85, 5.0, 2.85)
    arrow(6.0, 2.2, 6.0, 1.55, BAD)
    arrow(7.0, 2.7, 7.6, 2.55)
    arrow(7.0, 3.1, 7.6, 3.95)
    arrow(9.9, 4.3, 10.5, 4.45)
    arrow(9.9, 3.95, 10.5, 3.0)
    arrow(11.65, 2.35, 11.65, 1.55)
    arrow(9.9, 2.55, 10.5, 1.2)
    ax.text(11.65, 5.38, "append mode: each window is written once,\nafter the watermark closes it", ha="center",
            fontsize=8, color="#9a6a00", va="center")
    ax.text(0.2, 1.2, "Engines\n• Spark Structured Streaming\n  (Kafka source, docker compose)\n"
            "• Local reference engine\n  (same semantics, no JVM; make demo)", fontsize=8.5, color=MUTED, va="top",
            linespacing=1.45)
    fig.tight_layout()
    path = out / "architecture.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def traffic(metrics: pd.DataFrame, cfg: Settings, out: Path) -> Path:
    piv = (metrics.pivot_table(index="window_start", columns="event_type", values="events", aggfunc="sum")
           .reindex(columns=list(EVENT_TYPES)).fillna(0).sort_index())
    rev = metrics.groupby("window_start")["revenue"].sum().reindex(piv.index).fillna(0)
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 6), sharex=True, height_ratios=[2.2, 1])
    a1.stackplot(piv.index, *[piv[c] for c in piv.columns], labels=piv.columns,
                 colors=[SERIES[c] for c in piv.columns], edgecolor="white", linewidth=0.6, zorder=2)
    g = cfg.generator
    camp0 = g.start_dt + pd.Timedelta(minutes=g.campaign_start_minute)
    camp1 = camp0 + pd.Timedelta(minutes=g.campaign_minutes)
    for a in (a1, a2):
        a.axvspan(camp0, camp1, color=SIGNAL, alpha=0.12, lw=0, zorder=0)
        a.grid(axis="y", color=GRID)
    a1.text(camp1, a1.get_ylim()[1] * 0.97, " synthetic campaign spike", color="#9a6a00", fontsize=9, va="top")
    a1.set_title("Events per 1-minute tumbling window, by type (metrics_minute)")
    a1.set_ylabel("events / minute")
    a1.legend(frameon=False, ncol=4, loc="upper left", fontsize=9)
    a2.bar(rev.index, rev.values, width=pd.Timedelta(seconds=50), color=ACCENT)
    a2.yaxis.set_major_formatter(_inr)
    a2.set_ylabel("revenue / minute")
    a2.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
    a2.set_xlabel("event time (UTC)")
    fig.tight_layout()
    path = out / "traffic_per_minute.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def funnel(sessions: pd.DataFrame, out: Path) -> Path:
    steps = [("Viewed a page", (sessions["page_views"] > 0).sum()),
             ("Added to cart", (sessions["add_to_cart"] > 0).sum()),
             ("Started checkout", (sessions["checkouts"] > 0).sum()),
             ("Purchased", (sessions["purchases"] > 0).sum())]
    labels, values = zip(*steps)
    fig, ax = plt.subplots(figsize=(9, 3.8))
    y = range(len(values))[::-1]
    bars = ax.barh(list(y), values, color=[SERIES[t] for t in EVENT_TYPES], height=0.62)
    top = len(sessions)
    for i, (b, v) in enumerate(zip(bars, values)):
        prev = values[i - 1] if i else v
        note = f"{v:,}  ·  {v / top:.1%} of sessions" + (f"  ·  {v / prev:.0%} of previous step" if i else "")
        ax.text(b.get_width() + top * 0.01, b.get_y() + b.get_height() / 2, note, va="center", fontsize=9,
                color=INK)
    ax.set_yticks(list(y), labels)
    ax.set_xlim(0, max(values) * 1.75)
    ax.set_xticks([])
    ax.spines["bottom"].set_visible(False)
    ax.set_title(f"Session funnel from 30-minute session windows ({top:,} sessions)")
    fig.tight_layout()
    path = out / "session_funnel.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def microbatches(progress: list[dict], out: Path) -> Path:
    df = pd.DataFrame(progress)
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 6.2), sharex=True, height_ratios=[1.2, 1])
    parts = [("duplicates", "duplicate dropped", SIGNAL), ("late_dropped", "late, beyond watermark", "#e87ba4"),
             ("dead_letter", "dead letter", BAD)]
    bottom = pd.Series(0, index=df.index, dtype=float)
    for col, label, color in parts:
        a1.bar(df["batch_id"], df[col], bottom=bottom, color=color, label=label, width=0.75, edgecolor="white",
               linewidth=0.8)
        bottom += df[col]
    reads = int(df["input"].median())
    a1.set_title(f"Records kept out of the aggregates, per micro-batch (~{reads} records read per batch)")
    a1.set_ylabel("records")
    a1.legend(frameon=False, ncol=3, loc="upper left", fontsize=9)
    a1.set_ylim(0, bottom.max() * 1.35)
    a1.grid(axis="y", color=GRID)
    a2.plot(df["batch_id"], df["state_sessions"], color="#6250d6", lw=2, label="open sessions")
    a2.plot(df["batch_id"], df["state_windows"], color=GOOD, lw=2, label="open 1-min windows")
    a2.set_ylabel("rows in state")
    a2.set_xlabel("micro-batch id")
    a2.grid(axis="y", color=GRID)
    a2.legend(frameon=False, loc="upper left", fontsize=9)
    a2.set_title("Operator state: closed windows and sessions are evicted as the watermark advances", fontsize=11)
    fig.tight_layout()
    path = out / "microbatch_progress.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def lateness(events: pd.DataFrame, cfg: Settings, out: Path) -> Path:
    delay = (events["sent_at"] - events["event_time"]).dt.total_seconds().clip(lower=0.01)
    wm = cfg.watermark.total_seconds()
    fig, ax = plt.subplots(figsize=(10, 3.8))
    bins = pd.Series([0.01 * (1.25 ** i) for i in range(70)])
    bins = bins[bins < delay.max() * 1.3]
    ax.hist(delay, bins=bins, color=ACCENT, edgecolor="white", linewidth=0.5)
    ax.set_xscale("log")
    ax.axvline(wm, color=BAD, lw=2)
    beyond = int((delay > wm).sum())
    ax.text(wm * 1.08, ax.get_ylim()[1] * 0.92,
            f"watermark: {cfg.watermark_minutes:g} min\n{beyond:,} events ({beyond / len(delay):.1%}) "
            f"arrive\nmore than {cfg.watermark_minutes:g} min after they happen",
            color=BAD, fontsize=9, va="top")
    ticks = [0.1, 1, 10, 60, 600, 3600]
    ax.set_xticks(ticks, ["0.1 s", "1 s", "10 s", "1 min", "10 min", "1 h"])
    ax.set_xlabel("arrival delay = sent_at − event_time (log scale)")
    ax.set_ylabel("valid events")
    ax.grid(axis="y", color=GRID)
    ax.set_title("How late events arrive (bronze events table)")
    fig.tight_layout()
    path = out / "event_lateness.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def data_quality(dead: pd.DataFrame, summary: dict, out: Path) -> Path:
    counts = dead["reason"].value_counts().sort_values()
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.barh(counts.index, counts.values, color=BAD, height=0.6)
    for i, v in enumerate(counts.values):
        ax.text(v + counts.max() * 0.01, i, f" {v}", va="center", fontsize=9, color=INK)
    ax.set_xlim(0, counts.max() * 1.2)
    ax.grid(axis="x", color=GRID)
    ax.set_title(f"Dead-letter records by rule ({summary['dead_letters']:,} of {summary['records_read']:,} read)")
    ax.set_xlabel("records")
    fig.tight_layout()
    path = out / "dead_letters.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def render_all(cfg: Settings, out: Path, lake: Path | None = None, checkpoint: Path | None = None) -> list[Path]:
    """Render every chart. Progress-based charts need the local engine's checkpoint and are
    skipped for a lake written by Spark (whose progress log has a different shape)."""
    out.mkdir(parents=True, exist_ok=True)
    lake = Path(lake or cfg.path("lake"))
    progress = load_progress(Path(checkpoint or cfg.path("checkpoint")))
    paths = [architecture(out)]
    paths.append(traffic(read_table(lake, "metrics_minute"), cfg, out))
    paths.append(funnel(read_table(lake, "sessions"), out))
    if progress:
        paths.append(microbatches(progress, out))
    paths.append(lateness(read_table(lake, "events"), cfg, out))
    dead = read_table(lake, "dead_letter")
    paths.append(data_quality(dead, {"dead_letters": len(dead), "records_read": len(dead) + len(read_table(
        lake, "events"))} if not progress else summarise(lake, progress), out))
    return paths
