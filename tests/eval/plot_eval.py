#!/usr/bin/env python3
"""Draw the HERD and Motor comparison figures from measurement CSVs.

The figures are a function of the CSV files only. Nothing is sampled here.

    python3 tests/eval/plot_eval.py
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import os
import sys
from pathlib import Path


def _reexec_for_stable_env() -> None:
    env = os.environ.copy()
    changed = False
    for key, value in (("PYTHONHASHSEED", "0"), ("LC_ALL", "C"), ("LANG", "C")):
        if env.get(key) != value:
            env[key] = value
            changed = True
    if changed:
        os.execve(sys.executable, [sys.executable] + sys.argv, env)


_reexec_for_stable_env()

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
HERD = ROOT / "herd_rpc" / "perf"
MOTOR = ROOT / "motor" / "perf"
OUT = Path(__file__).resolve().parent / "figures"

NATIVE = "#1f4e79"
VARUNA = "#d35400"
BLIND = "#922b21"
LOST = "#98a2b3"
DUP = "#922b21"
OK = "#1f7a4d"
MISS = "#b54708"


def read_csv(path: Path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def nice_num(value: float, do_round: bool) -> float:
    if value <= 0:
        return 1.0
    exp = math.floor(math.log10(value))
    frac = value / (10 ** exp)
    if do_round:
        nice = 1.0 if frac < 1.5 else 2.0 if frac < 3.0 else 5.0 if frac < 7.0 else 10.0
    else:
        nice = 1.0 if frac <= 1.0 else 2.0 if frac <= 2.0 else 5.0 if frac <= 5.0 else 10.0
    return nice * (10 ** exp)


def axis_upper(values) -> float:
    peak = max(values)
    if peak <= 0:
        return 1.0
    step = nice_num(nice_num(peak, False) / 4.0, True)
    upper = math.ceil((peak * 1.08) / step) * step
    if upper <= peak:
        upper += step
    return upper


def tick_values(upper: float):
    step = nice_num(nice_num(upper, False) / 4.0, True)
    ticks = []
    value = 0.0
    while value < upper - step * 0.2:
        ticks.append(value)
        value += step
    ticks.append(upper)
    return ticks


def format_tick(value: float) -> str:
    if abs(value - round(value)) < 1e-6:
        return str(int(round(value)))
    return f"{value:.1f}"


def style_ax(ax, ylabel, upper):
    ticks = tick_values(upper)
    ax.set_ylim(0, ticks[-1])
    ax.set_yticks(ticks)
    ax.set_yticklabels([format_tick(tick) for tick in ticks])
    ax.set_ylabel(ylabel)
    ax.grid(True, axis="y", color="#d0d5dd", linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color("#667085")
    ax.spines["bottom"].set_color("#667085")
    ax.tick_params(colors="#344054", labelsize=9)


def apply_style(salt: str):
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.unicode_minus": False,
            "svg.fonttype": "path",
            "svg.hashsalt": salt,
            "pdf.fonttype": 42,
            "path.simplify": False,
            "savefig.facecolor": "white",
            "figure.facecolor": "white",
        }
    )


def save(fig, name: str):
    OUT.mkdir(parents=True, exist_ok=True)
    png = OUT / f"{name}.png"
    svg = OUT / f"{name}.svg"
    fig.savefig(png, dpi=140)
    fig.savefig(svg)
    plt.close(fig)
    text = svg.read_text(encoding="utf-8")
    lines = []
    for line in text.splitlines(keepends=True):
        if "<dc:date>" in line:
            indent = line[: len(line) - len(line.lstrip(" "))]
            newline = "\n" if line.endswith("\n") else ""
            lines.append(f"{indent}<dc:date>1970-01-01T00:00:00Z</dc:date>{newline}")
        else:
            lines.append(line)
    svg.write_text("".join(lines), encoding="utf-8")
    return png, svg


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def herd_perf():
    rows = {row["system"]: row for row in read_csv(HERD / "herd_perf.csv")}
    apply_style("herd-perf-v1")
    fig, axes = plt.subplots(1, 2, figsize=(8.4, 4.2), dpi=140)
    labels = ["HERD native", "HERD + Varuna"]
    keys = ["herd_native", "herd_varuna"]
    colors = [NATIVE, VARUNA]
    panels = (
        (axes[0], "kops", "Throughput", "Request throughput (K op/s)"),
        (axes[1], "p50_us", "P50 latency", "P50 latency (us)"),
    )
    for ax, field, title, ylabel in panels:
        values = [float(rows[key][field]) for key in keys]
        bars = ax.bar([0, 1], values, color=colors, width=0.62, zorder=3)
        for bar, value in zip(bars, values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{value:.1f}",
                ha="center",
                va="bottom",
                fontsize=9,
                color="#101828",
            )
        ax.set_xticks([0, 1])
        ax.set_xticklabels(labels)
        ax.set_xlim(-0.55, 1.55)
        style_ax(ax, ylabel, axis_upper(values) * 1.08)
        ax.set_title(title, loc="left", color="#101828", pad=8)
    fig.suptitle(
        "HERD request write, one outstanding RPC, 1 second",
        fontsize=11,
        color="#101828",
        x=0.07,
        ha="left",
    )
    fig.subplots_adjust(left=0.09, right=0.98, top=0.82, bottom=0.14, wspace=0.32)
    return save(fig, "herd_perf")


def stacked_outcomes(ax, names, parts, ylabel):
    x = list(range(len(names)))
    bottoms = [0.0] * len(names)
    for label, color, series in parts:
        ax.bar(x, series, bottom=bottoms, color=color, width=0.62, label=label, zorder=3)
        bottoms = [b + s for b, s in zip(bottoms, series)]
    for i, total in enumerate(bottoms):
        correct = parts[0][2][i]
        ax.text(i, total + 1.5, f"{correct:.0f}%", ha="center", va="bottom", fontsize=9, color="#101828")
    ax.set_xticks(x)
    ax.set_xticklabels(names)
    ax.set_xlim(-0.55, len(names) - 0.45)
    ax.set_ylim(0, 118)
    ax.set_ylabel(ylabel)
    ax.grid(True, axis="y", color="#d0d5dd", linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color("#667085")
    ax.spines["bottom"].set_color("#667085")
    ax.tick_params(colors="#344054", labelsize=9)
    ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=3)


def herd_correct():
    order = ["herd_native", "herd_blind", "herd_varuna"]
    label = {"herd_native": "HERD native", "herd_blind": "Blind retransmit", "herd_varuna": "HERD + Varuna"}
    rows = {row["system"]: row for row in read_csv(HERD / "herd_correct.csv")}
    apply_style("herd-correct-v1")
    fig, ax = plt.subplots(figsize=(7.6, 4.4), dpi=140)

    def pct(key, field):
        row = rows[key]
        return 100.0 * int(row[field]) / int(row["trials"])

    parts = [
        ("Exactly once", OK, [pct(k, "correct") for k in order]),
        ("Executed twice", DUP, [pct(k, "duplicate") for k in order]),
        ("Lost", LOST, [pct(k, "lost") for k in order]),
        ("Wrong value", MISS, [pct(k, "mismatch") for k in order]),
    ]
    # Drop series that are zero everywhere so the legend stays about this run.
    parts = [part for part in parts if any(value > 0 for value in part[2])]
    stacked_outcomes(ax, [label[k] for k in order], parts, "Requests (%)")
    trials = int(rows[order[0]]["trials"])
    ax.set_title(
        f"Recovery outcome after the QP is moved to ERR\n{trials} posted requests, 16 in flight at each failure",
        loc="left",
        color="#101828",
        pad=10,
    )
    fig.subplots_adjust(left=0.12, right=0.98, top=0.82, bottom=0.22)
    return save(fig, "herd_correct")


def herd_timeline():
    rows = read_csv(HERD / "herd_timeline.csv")
    meta = read_csv(HERD / "herd_timeline_meta.csv")[0]
    fail_us = float(meta["fail_us"])
    recovery_us = float(meta["recovery_us"])
    xs = [(float(row["t_us"]) - fail_us) / 1000.0 for row in rows]
    ys = [float(row["kops"]) for row in rows]
    apply_style("herd-timeline-v1")
    fig, ax = plt.subplots(figsize=(8.2, 4.3), dpi=140)
    ax.plot(xs, ys, color=VARUNA, linewidth=1.8, zorder=3)
    ax.axvspan(0, recovery_us / 1000.0, color="#f5d0b5", zorder=1)
    ax.axvline(0, color="#667085", linewidth=0.8, linestyle="--", zorder=2)
    ax.set_xlim(-4, 6)
    style_ax(ax, "Request throughput (K op/s)", axis_upper(ys))
    ax.set_xlabel("Time relative to QP failure (ms)")
    ax.set_title(
        f"HERD + Varuna failover, recovery {recovery_us:.0f} us",
        loc="left",
        color="#101828",
        pad=8,
    )
    fig.subplots_adjust(left=0.11, right=0.98, top=0.86, bottom=0.16)
    return save(fig, "herd_timeline")


def motor_perf():
    rows = read_csv(MOTOR / "motor_varuna_tpcc.csv")
    threads = sorted({int(row["threads"]) for row in rows})
    series = {}
    for system in ("motor_native", "motor_varuna"):
        series[system] = []
        for thread in threads:
            match = [
                row
                for row in rows
                if row["system"] == system and int(row["threads"]) == thread
            ]
            if len(match) != 1:
                raise SystemExit(f"expected one {system} row at {thread} threads")
            series[system].append(match[0])
    apply_style("motor-perf-v1")
    fig, axes = plt.subplots(1, 3, figsize=(12.6, 4.3), dpi=140)
    panels = (
        (axes[0], "committed_ktps", "Throughput", "Committed throughput (K txn/s)"),
        (axes[1], "p50_us", "P50 latency", "P50 latency (us)"),
        (axes[2], "p99_us", "P99 latency", "P99 latency (us)"),
    )
    x = list(range(len(threads)))
    for system, color, marker in (
        ("motor_native", NATIVE, "o"),
        ("motor_varuna", VARUNA, "s"),
    ):
        label = "Motor native" if system == "motor_native" else "Motor + Varuna"
        for ax, field, _title, _ylabel in panels:
            ax.plot(
                x,
                [float(row[field]) for row in series[system]],
                color=color,
                marker=marker,
                markersize=6.5,
                linewidth=2.0,
                label=label,
                zorder=3,
            )
    for ax, field, title, ylabel in panels:
        values = [float(row[field]) for system in series for row in series[system]]
        ax.set_xticks(x)
        ax.set_xticklabels([str(thread) for thread in threads])
        ax.set_xlabel("Threads")
        ax.set_xlim(-0.35, len(threads) - 0.65)
        style_ax(ax, ylabel, axis_upper(values))
        ax.set_title(title, loc="left", color="#101828", pad=8)
        ax.legend(frameon=False, loc="best")
    sample = rows[0]
    fig.suptitle(
        f"TPC-C {sample['isolation']}, {sample['coroutines']} coroutines, "
        f"{sample['attempted_num']} attempts/thread",
        fontsize=11,
        color="#101828",
        x=0.06,
        ha="left",
    )
    fig.subplots_adjust(left=0.055, right=0.985, top=0.82, bottom=0.16, wspace=0.32)
    return save(fig, "motor_perf")


def motor_correct():
    order = ["motor_native", "motor_blind", "motor_varuna"]
    label = {
        "motor_native": "Motor native",
        "motor_blind": "Blind retransmit",
        "motor_varuna": "Motor + Varuna",
    }
    rows = {row["system"]: row for row in read_csv(MOTOR / "motor_failover_correct.csv")}
    apply_style("motor-correct-v1")
    fig, ax = plt.subplots(figsize=(7.6, 4.4), dpi=140)

    def pct(key, field):
        row = rows[key]
        total = int(row["correct"]) + int(row["incorrect"]) + int(row["aborted"])
        if total == 0:
            return 0.0
        return 100.0 * int(row[field]) / total

    parts = [
        ("Recovered correctly", OK, [pct(k, "correct") for k in order]),
        ("Replayed a completed op", DUP, [pct(k, "incorrect") for k in order]),
        ("No failover", LOST, [pct(k, "aborted") for k in order]),
    ]
    parts = [part for part in parts if any(value > 0 for value in part[2])]
    stacked_outcomes(ax, [label[k] for k in order], parts, "In-flight operations (%)")
    ax.set_title(
        "TPC-C recovery after ens1f0 is brought down\nWRITE and CAS decisions; reads are excluded",
        loc="left",
        color="#101828",
        pad=10,
    )
    fig.subplots_adjust(left=0.12, right=0.98, top=0.80, bottom=0.22)
    return save(fig, "motor_correct")


def _timeline_points(rows):
    points = []
    for row in rows:
        interval_us = float(row["interval_us"]) if row.get("interval_us") else 0.0
        points.append(
            (
                int(row["t_ns"]),
                float(row["committed_ktps"]),
                interval_us,
            )
        )
    points.sort(key=lambda item: item[0])
    return points


def _fill_idle_zero(xs, ys, step):
    points = sorted(zip(xs, ys))
    filled_x = []
    filled_y = []
    for index, (x, y) in enumerate(points):
        if index and x - points[index - 1][0] > step * 1.5:
            cursor = points[index - 1][0] + step
            while cursor < x - step * 0.5:
                filled_x.append(cursor)
                filled_y.append(0.0)
                cursor += step
        filled_x.append(x)
        filled_y.append(y)
    return filled_x, filled_y


def _bucket_means(xs, ys, width):
    bins = {}
    for x, y in zip(xs, ys):
        key = math.floor(x / width) * width
        bins.setdefault(key, []).append(y)
    out_x = []
    out_y = []
    previous = None
    for key in sorted(bins):
        if previous is not None and key - previous > width * 1.5:
            out_x.append(float("nan"))
            out_y.append(float("nan"))
        out_x.append(key + width / 2.0)
        out_y.append(sum(bins[key]) / len(bins[key]))
        previous = key
    return out_x, out_y


def _failover_series(name):
    meta = {row["series"]: row for row in read_csv(MOTOR / "motor_timeline_compare.csv")}[name]
    fail_ns = float(meta["fail_ns"])
    # A tick whose interval covers the blocked QP reports the stall average,
    # not the rate after failover.
    drawn = [
        point
        for point in _timeline_points(read_csv(MOTOR / f"motor_timeline_{name}.csv"))
        if point[2] <= 200000.0
    ]
    seconds = [(point[0] - fail_ns) / 1e9 for point in drawn]
    millis = [(point[0] - fail_ns) / 1e6 for point in drawn]
    rates = [point[1] for point in drawn]
    return meta, seconds, millis, rates


def motor_timeline():
    # Two measured paths, each aligned to its own switch_start.
    # dcqp_background keeps the backup RC down and serves the failover on DCQP.
    # rc_blocking waits for that RC to connect before recovery.
    specs = (
        ("dcqp_background", "DCQP switch, RC rebuilds in background", VARUNA),
        ("rc_blocking", "Wait for RC rebuild", NATIVE),
    )
    loaded = []
    for name, label, color in specs:
        meta, seconds, millis, rates = _failover_series(name)
        loaded.append((label, color, meta, seconds, millis, rates))
    apply_style("motor-timeline-v14")
    fig, axes = plt.subplots(
        2,
        1,
        figsize=(8.2, 6.6),
        dpi=140,
        gridspec_kw={"height_ratios": [1.15, 1.0]},
    )
    full, zoom = axes
    mean_ys = []
    mean_xs = []
    for label, color, meta, seconds, _millis, rates in loaded:
        # Align at the last commit before the stall, not at switch_start.
        # The two runs notice the failed NIC after different delays, and that
        # delay is the outage the curve actually shows.
        stall = max(x for x in seconds if x < 0.0)
        shifted = [x - stall for x in seconds]
        filled_x, filled_y = _fill_idle_zero(shifted, rates, 0.05)
        mean_x, mean_y = _bucket_means(filled_x, filled_y, 0.05)
        mean_ys.extend(value for value in mean_y if value == value)
        mean_xs.extend(x for x in mean_x if x == x)
        full.plot(mean_x, mean_y, color=color, linewidth=1.8, zorder=3, label=label)
    full.axvline(0, color="#667085", linewidth=0.8, linestyle="--", zorder=2)
    full.set_xlim(min(mean_xs) - 0.05, max(mean_xs) + 0.08)
    style_ax(full, "Committed throughput (K txn/s)", axis_upper(mean_ys))
    full.set_xlabel("Time after the last commit before the stall (s)")
    full.set_title(
        "Committed throughput, aligned where both runs stop, 50 ms mean",
        loc="left",
        color="#101828",
        pad=8,
    )
    full.legend(frameon=False, fontsize=8, loc="upper right")

    recoveries_ms = [float(item[2]["recovery_us"]) / 1000.0 for item in loaded]
    zoom_hi = max(80.0, max(recoveries_ms) * 1.25)
    wide = zoom_hi > 200.0
    zoom_ys = []
    for label, color, meta, _seconds, millis, rates in loaded:
        filled_s, filled_y = _fill_idle_zero(
            [item / 1000.0 for item in millis], rates, 0.005
        )
        window = [
            (x * 1000.0, y)
            for x, y in zip(filled_s, filled_y)
            if -1.0 <= x * 1000.0 <= zoom_hi
        ]
        zoom_ys.extend(item[1] for item in window)
        zoom.plot(
            [item[0] for item in window],
            [item[1] for item in window],
            color=color,
            linewidth=1.6,
            marker=None if wide else "o",
            markersize=3.5,
            zorder=3,
            label=label,
        )
        recovery_ms = float(meta["recovery_us"]) / 1000.0
        zoom.axvline(recovery_ms, color=color, linewidth=1.0, linestyle=":", zorder=2)
    zoom.axvline(0, color="#667085", linewidth=0.8, linestyle="--", zorder=2)
    zoom.set_xlim(-1, zoom_hi)
    style_ax(zoom, "Committed throughput (K txn/s)", axis_upper(zoom_ys))
    top = zoom.get_ylim()[1]
    dcqp_ms = recoveries_ms[0]
    rc_ms = recoveries_ms[1]

    def _span(ms):
        return f"{ms / 1000.0:.2f} s" if ms >= 100.0 else f"{ms:.1f} ms"

    zoom.text(dcqp_ms + zoom_hi * 0.012, top * 0.90, f"DCQP {_span(dcqp_ms)}", color=VARUNA, fontsize=8, va="top")
    zoom.text(rc_ms + zoom_hi * 0.012, top * 0.72, f"RC {_span(rc_ms)}", color=NATIVE, fontsize=8, va="top")
    zoom.set_xlabel("Time after the error is reported (ms)")
    zoom.set_title(
        "Throughput after the NIC error. Dotted lines classify the in-flight writes",
        loc="left",
        color="#101828",
        pad=8,
    )
    fig.subplots_adjust(left=0.12, right=0.98, top=0.94, bottom=0.09, hspace=0.48)
    return save(fig, "motor_timeline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only",
        choices=("all", "herd", "motor-perf", "motor"),
        default="all",
    )
    args = parser.parse_args()
    made = []
    if args.only in ("all", "herd"):
        made.extend(herd_perf())
        made.extend(herd_correct())
        made.extend(herd_timeline())
    if args.only in ("all", "motor-perf", "motor"):
        made.extend(motor_perf())
    if args.only in ("all", "motor"):
        made.extend(motor_correct())
        made.extend(motor_timeline())
    for path in made:
        print(f"{path} sha256 {sha256(path)}")


if __name__ == "__main__":
    main()
