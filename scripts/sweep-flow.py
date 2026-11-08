#!/usr/bin/env python3
"""Run measure-flow once per producer rate and compare the latency across rates.

  sweep-flow.py --out DIR --rates 500 1000 2000 -- <producer arguments without -rate>

Each rate runs measure-flow into DIR/rate-<rate>/ (the flow is reset first, so this
permanently clears the project topics and database results before every run). After
each completed run DIR/sweep-summary.csv is rewritten with one row per rate and
stage: the latency percentiles from that run's latency-summary.csv plus its
throughput from pipeline.json. The sweep stops at the first measure-flow failure;
the rates that completed stay in the summary.
"""

import argparse
import csv
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
SUMMARY_COLUMNS = ("rate", "stage", "count", "p50_ms", "p95_ms", "p99_ms", "max_ms",
                   "producer_events_per_s", "end_to_end_events_per_s")


def measure_command(rate, out_dir, producer_args):
    return [sys.executable, "scripts/measure-flow.py", "--out", f"{out_dir}/rate-{rate}",
            "--label", f"rate={rate}", "--", *producer_args, f"-rate={rate}"]


def build_sweep_rows(runs):
    """One row per rate and stage from (rate, latency_summary_rows, pipeline) triples."""
    return [{"rate": rate, "stage": item["stage"], "count": item["count"],
             "p50_ms": item["p50_ms"], "p95_ms": item["p95_ms"], "p99_ms": item["p99_ms"],
             "max_ms": item["max_ms"],
             "producer_events_per_s": pipeline.get("producer_events_per_s"),
             "end_to_end_events_per_s": pipeline.get("end_to_end_events_per_s")}
            for rate, summary, pipeline in runs for item in summary]


def write_summary(out_dir, runs):
    with open(out_dir / "sweep-summary.csv", "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows({key: "" if value is None else value for key, value in row.items()}
                         for row in build_sweep_rows(runs))


def main(argv=None, runner=subprocess.run):
    argv = list(sys.argv[1:] if argv is None else argv)
    producer_args = argv[argv.index("--") + 1:] if "--" in argv else []
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True, help="parent directory of the per-rate results")
    parser.add_argument("--rates", type=int, nargs="+", required=True, help="producer events per second, one run each")
    args = parser.parse_args(argv[:argv.index("--")] if "--" in argv else argv)
    out = args.out.resolve()

    runs = []
    for rate in args.rates:
        print(f"== {rate} events/s ==", flush=True)
        status = runner(measure_command(rate, out, producer_args), cwd=ROOT).returncode
        if status != 0:
            print(f"Error: measure-flow failed at {rate} events/s (status {status}); stopping. "
                  f"Results of the earlier rates are in {out}", file=sys.stderr)
            return 1
        run_dir = out / f"rate-{rate}"
        with open(run_dir / "latency-summary.csv", newline="") as handle:
            summary = list(csv.DictReader(handle))
        runs.append((rate, summary, json.loads((run_dir / "pipeline.json").read_text())))
        write_summary(out, runs)
    print(f"Sweep summary in {out / 'sweep-summary.csv'}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Sweep interrupted; use make reset-flow before the next run.", file=sys.stderr)
        sys.exit(130)
