#!/usr/bin/env python3
"""Plot Motor native vs Motor+Varuna TPC-C measurements.

The figure is a pure function of the CSV next to this script. Running it
again on the same file, with the same Python and matplotlib, rewrites
byte-identical PNG and SVG outputs. Nothing is sampled at plot time.

    python3 plot_motor_varuna.py
    python3 plot_motor_varuna.py --csv motor_varuna_tpcc.csv --out-dir figures

CSV columns:
    system,benchmark,isolation,threads,coroutines,attempted_num,
    attempted_ktps,committed_ktps,p50_us,p99_us

system is motor_native or motor_varuna.
attempted_ktps and committed_ktps are aggregate K txn/s, as printed by Motor.
p50_us and p99_us are the benchmark's mean of per-thread percentiles, in
microseconds. Repeated rows for the same (system, threads, coroutines,
isolation, benchmark) are reduced with a deterministic median.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import os
import sys
from collections import defaultdict
from pathlib import Path


def _reexec_for_stable_env() -> None:
    """Force a stable locale and hash seed before any library import."""
    env = os.environ.copy()
    changed = False
    if env.get("PYTHONHASHSEED") != "0":
        env["PYTHONHASHSEED"] = "0"
        changed = True
    if env.get("LC_ALL") != "C":
        env["LC_ALL"] = "C"
        changed = True
    if env.get("LANG") != "C":
        env["LANG"] = "C"
        changed = True
    if changed:
        os.execve(sys.executable, [sys.executable] + sys.argv, env)


_reexec_for_stable_env()

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
SYSTEMS = ("motor_native", "motor_varuna")
SYSTEM_LABEL = {
    "motor_native": "Motor native",
    "motor_varuna": "Motor + Varuna",
}
SYSTEM_COLOR = {
    "motor_native": "#1f4e79",
    "motor_varuna": "#d35400",
}
COLUMNS = (
    "system",
    "benchmark",
    "isolation",
    "threads",
    "coroutines",
    "attempted_num",
    "attempted_ktps",
    "committed_ktps",
    "p50_us",
    "p99_us",
)


def nice_num(value: float, do_round: bool) -> float:
    exp = math.floor(math.log10(value))
    frac = value / (10 ** exp)
    if do_round:
        if frac < 1.5:
            nice = 1.0
        elif frac < 3.0:
            nice = 2.0
        elif frac < 7.0:
            nice = 5.0
        else:
            nice = 10.0
    else:
        if frac <= 1.0:
            nice = 1.0
        elif frac <= 2.0:
            nice = 2.0
        elif frac <= 5.0:
            nice = 5.0
        else:
            nice = 10.0
    return nice * (10 ** exp)


def axis_upper(values) -> float:
    peak = max(values)
    if peak <= 0:
        return 1.0
    span = nice_num(peak, False)
    step = nice_num(span / 4.0, True)
    upper = math.ceil((peak * 1.08) / step) * step
    # Keep one grid step of headroom when a point would sit on the frame.
    if upper <= peak:
        upper += step
    return upper


def tick_values(upper: float):
    span = nice_num(upper, False)
    step = nice_num(span / 4.0, True)
    count = int(round(upper / step))
    ticks = [i * step for i in range(count + 1)]
    if ticks[-1] < upper - step * 1e-9:
        ticks.append(upper)
    return ticks


def format_tick(value: float) -> str:
    rounded = round(value, 6)
    if abs(rounded - round(rounded)) < 1e-6:
        return str(int(round(rounded)))
    text = f"{rounded:.4f}".rstrip("0").rstrip(".")
    return text


def median(values):
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def load_rows(path: Path):
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise SystemExit(f"{path} is empty")
        missing = [name for name in COLUMNS if name not in reader.fieldnames]
        if missing:
            raise SystemExit(f"{path} is missing columns: {', '.join(missing)}")
        rows = []
        for line_no, row in enumerate(reader, start=2):
            system = row["system"].strip()
            if system not in SYSTEMS:
                raise SystemExit(f"{path}:{line_no}: unknown system {system!r}")
            item = {
                "system": system,
                "benchmark": row["benchmark"].strip(),
                "isolation": row["isolation"].strip(),
                "threads": int(row["threads"]),
                "coroutines": int(row["coroutines"]),
                "attempted_num": int(row["attempted_num"]),
                "attempted_ktps": float(row["attempted_ktps"]),
                "committed_ktps": float(row["committed_ktps"]),
                "p50_us": float(row["p50_us"]),
                "p99_us": float(row["p99_us"]),
            }
            rows.append(item)
    if not rows:
        raise SystemExit(f"{path} has no data rows")
    return rows


def reduce_rows(rows):
    grouped = defaultdict(lambda: defaultdict(list))
    meta = {}
    for row in rows:
        key = (row["system"], row["threads"])
        for field in ("committed_ktps", "p50_us", "p99_us", "attempted_ktps"):
            grouped[key][field].append(row[field])
        meta_key = (
            row["benchmark"],
            row["isolation"],
            row["coroutines"],
            row["attempted_num"],
        )
        meta.setdefault("keys", set()).add(meta_key)
    if len(meta["keys"]) != 1:
        raise SystemExit(
            "CSV mixes benchmark settings. Plot one benchmark, isolation, "
            f"coroutine count, and attempted_num at a time: {sorted(meta['keys'])}"
        )
    benchmark, isolation, coroutines, attempted_num = next(iter(meta["keys"]))
    threads = sorted({row["threads"] for row in rows})
    series = {}
    for system in SYSTEMS:
        series[system] = []
        for thread in threads:
            bucket = grouped.get((system, thread))
            if bucket is None:
                raise SystemExit(f"missing {system} row for {thread} threads")
            series[system].append(
                {
                    "threads": thread,
                    "committed_ktps": median(bucket["committed_ktps"]),
                    "p50_us": median(bucket["p50_us"]),
                    "p99_us": median(bucket["p99_us"]),
                    "attempted_ktps": median(bucket["attempted_ktps"]),
                    "samples": len(bucket["committed_ktps"]),
                }
            )
    return {
        "benchmark": benchmark,
        "isolation": isolation,
        "coroutines": coroutines,
        "attempted_num": attempted_num,
        "threads": threads,
        "series": series,
        "samples": series[SYSTEMS[0]][0]["samples"],
    }


def style_axes(ax, ylabel, upper):
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


def draw(summary, out_dir: Path):
    style = {
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "axes.unicode_minus": False,
        "svg.hashsalt": "motor-varuna-tpcc-v1",
        "svg.fonttype": "path",
        "pdf.fonttype": 42,
        "path.simplify": False,
        "agg.path.chunksize": 0,
        "savefig.facecolor": "white",
        "figure.facecolor": "white",
    }
    plt.rcParams.update({key: value for key, value in style.items() if key in plt.rcParams})
    fig, axes = plt.subplots(1, 3, figsize=(12.6, 4.3), dpi=140)
    threads = summary["threads"]
    x = list(range(len(threads)))
    panels = (
        (axes[0], "committed_ktps", "Throughput", "Committed throughput (K txn/s)", "upper left"),
        (axes[1], "p50_us", "P50 latency", "P50 latency (us)", "lower right"),
        (axes[2], "p99_us", "P99 latency", "P99 latency (us)", "lower left"),
    )
    for system in SYSTEMS:
        points = summary["series"][system]
        marker = "o" if system == "motor_native" else "s"
        for ax, field, _title, _ylabel, _legend in panels:
            ax.plot(
                x,
                [point[field] for point in points],
                color=SYSTEM_COLOR[system],
                marker=marker,
                markersize=6.5,
                linewidth=2.0,
                label=SYSTEM_LABEL[system],
                zorder=3,
            )
    for ax, field, title, ylabel, legend_loc in panels:
        values = [point[field] for system in SYSTEMS for point in summary["series"][system]]
        ax.set_xticks(x)
        ax.set_xticklabels([str(thread) for thread in threads])
        ax.set_xlabel("Threads")
        ax.set_xlim(-0.35, len(threads) - 0.65)
        style_axes(ax, ylabel, axis_upper(values))
        ax.set_title(title, loc="left", color="#101828", pad=8)
        ax.legend(frameon=False, loc=legend_loc)

    sample_note = "1 run" if summary["samples"] == 1 else f"median of {summary['samples']} runs"
    fig.suptitle(
        f"TPC-C {summary['isolation']}, {summary['coroutines']} coroutines, "
        f"{summary['attempted_num']} attempts/thread ({sample_note})",
        fontsize=11,
        color="#101828",
        x=0.06,
        ha="left",
    )
    fig.subplots_adjust(left=0.055, right=0.985, top=0.82, bottom=0.16, wspace=0.32)

    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / "motor_varuna_tpcc.png"
    svg_path = out_dir / "motor_varuna_tpcc.svg"
    fig.savefig(png_path, dpi=140)
    fig.savefig(svg_path)
    plt.close(fig)
    normalize_svg(svg_path)
    return png_path, svg_path


def normalize_svg(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    lines = []
    for line in text.splitlines(keepends=True):
        if "<dc:date>" in line:
            indent = line[: len(line) - len(line.lstrip(" "))]
            newline = "\n" if line.endswith("\n") else ""
            lines.append(f"{indent}<dc:date>1970-01-01T00:00:00Z</dc:date>{newline}")
        else:
            lines.append(line)
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("".join(lines))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        type=Path,
        default=HERE / "motor_varuna_tpcc.csv",
        help="measurement CSV (default: beside this script)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=HERE / "figures",
        help="directory for png and svg (default: figures/ beside this script)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = args.csv if args.csv.is_absolute() else Path.cwd() / args.csv
    out_dir = args.out_dir if args.out_dir.is_absolute() else Path.cwd() / args.out_dir
    if not csv_path.is_file():
        raise SystemExit(f"CSV not found: {csv_path}")
    summary = reduce_rows(load_rows(csv_path))
    png_path, svg_path = draw(summary, out_dir)
    print(f"csv {csv_path}")
    print(f"png {png_path} sha256 {sha256(png_path)}")
    print(f"svg {svg_path} sha256 {sha256(svg_path)}")


if __name__ == "__main__":
    main()
