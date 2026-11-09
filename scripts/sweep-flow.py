#!/usr/bin/env python3
"""Run measure-flow once per producer load and compare the latency across loads.

  sweep-flow.py --out DIR --rates 500 1000 2000 -- <producer arguments without -rate>
  sweep-flow.py --out DIR --speeds 100 400 -- <producer arguments without -rate or -speed>

A load is either a fixed rate (-rate, events per second) or a replay speed (-speed,
the gaps between trade times sped up by that factor). Each load runs measure-flow
into DIR/rate-<n>/ or DIR/speed-<n>/ (the flow is reset first, so this permanently
clears the project topics and database results before every run). After each
completed run DIR/sweep-summary.csv is rewritten with one row per load and stage:
the latency percentiles from that run's latency-summary.csv plus its throughput
(and, for speeds, the producer's replay lag) from pipeline.json. The sweep stops at
the first measure-flow failure; the loads that completed stay in the summary.
"""

import argparse
import csv
import json
import math
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
MEASURED_COLUMNS = ("stage", "count", "p50_ms", "p95_ms", "p99_ms", "max_ms",
                    "producer_events_per_s", "end_to_end_events_per_s")


def summary_columns(mode):
    """The first column names the swept setting: rate or speed."""
    extra = ("replay_lag_max_ms",) if mode == "speed" else ()
    return (mode, *MEASURED_COLUMNS, *extra)


def speed_value(text):
    """A positive, finite replay speed, kept as written so it names its directory."""
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid speed {text!r}") from None
    if not (value > 0 and math.isfinite(value)):
        raise argparse.ArgumentTypeError(f"speed must be a finite number above zero, not {text!r}")
    return text


def measure_command(mode, value, out_dir, producer_args):
    return [sys.executable, "scripts/measure-flow.py", "--out", f"{out_dir}/{mode}-{value}",
            "--label", f"{mode}={value}", "--", *producer_args, f"-{mode}={value}"]


def build_sweep_rows(mode, runs):
    """One row per load and stage from (value, latency_summary_rows, pipeline) triples."""
    rows = []
    for value, summary, pipeline in runs:
        for item in summary:
            row = {mode: value, "stage": item["stage"], "count": item["count"],
                   "p50_ms": item["p50_ms"], "p95_ms": item["p95_ms"], "p99_ms": item["p99_ms"],
                   "max_ms": item["max_ms"],
                   "producer_events_per_s": pipeline.get("producer_events_per_s"),
                   "end_to_end_events_per_s": pipeline.get("end_to_end_events_per_s")}
            if mode == "speed":
                row["replay_lag_max_ms"] = (pipeline.get("producer") or {}).get("replay_lag_max_ms")
            rows.append(row)
    return rows


def write_summary(out_dir, mode, runs):
    with open(out_dir / "sweep-summary.csv", "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=summary_columns(mode))
        writer.writeheader()
        writer.writerows({key: "" if value is None else value for key, value in row.items()}
                         for row in build_sweep_rows(mode, runs))


def main(argv=None, runner=subprocess.run):
    argv = list(sys.argv[1:] if argv is None else argv)
    producer_args = argv[argv.index("--") + 1:] if "--" in argv else []
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True, help="parent directory of the per-load results")
    loads = parser.add_mutually_exclusive_group(required=True)
    loads.add_argument("--rates", type=int, nargs="+", help="producer events per second, one run each")
    loads.add_argument("--speeds", type=speed_value, nargs="+",
                       help="replay speeds (trade-time gaps divided by this factor), one run each")
    args = parser.parse_args(argv[:argv.index("--")] if "--" in argv else argv)
    mode, values = ("rate", args.rates) if args.rates else ("speed", args.speeds)
    if any(arg.startswith(("-rate=", "-speed=")) for arg in producer_args):
        parser.error("leave -rate and -speed out of the producer arguments; the sweep sets them")
    out = args.out.resolve()
    unit = "events/s" if mode == "rate" else "x replay speed"

    runs = []
    for value in values:
        print(f"== {value} {unit} ==", flush=True)
        status = runner(measure_command(mode, value, out, producer_args), cwd=ROOT).returncode
        if status != 0:
            print(f"Error: measure-flow failed at {value} {unit} (status {status}); stopping. "
                  f"Results of the earlier loads are in {out}", file=sys.stderr)
            return 1
        run_dir = out / f"{mode}-{value}"
        with open(run_dir / "latency-summary.csv", newline="") as handle:
            summary = list(csv.DictReader(handle))
        runs.append((value, summary, json.loads((run_dir / "pipeline.json").read_text())))
        write_summary(out, mode, runs)
    print(f"Sweep summary in {out / 'sweep-summary.csv'}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Sweep interrupted; use make reset-flow before the next run.", file=sys.stderr)
        sys.exit(130)
