#!/usr/bin/env python3
"""Export CPU, memory, network I/O and block I/O for a time window from Prometheus.

Every component is a container, read from the docker-stats-exporter (see
compose.monitoring.yaml). Writes

  timeseries.csv  every sample: offset_s,component,kind,metric,value
  summary.csv     per component and metric: avg, max, and the total moved
  run.json        the window and settings, so a run can be reproduced
"""

import argparse
import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request

# (metric, kind, label identifying the component, total query or None, PromQL with {w} = rate window)
DOCKER = "docker_container"
METRICS = [
    ("cpu_cores", "docker", "service", "sum by (service) (rate(%s_cpu_usage_seconds_total[{w}]))" % DOCKER,
     "sum by (service) (increase(%s_cpu_usage_seconds_total[{d}]))" % DOCKER),
    ("memory_bytes", "docker", "service", "sum by (service) (%s_memory_working_set_bytes)" % DOCKER, None),
    ("network_receive_bytes_per_s", "docker", "service", "sum by (service) (rate(%s_network_receive_bytes_total[{w}]))" % DOCKER,
     "sum by (service) (increase(%s_network_receive_bytes_total[{d}]))" % DOCKER),
    ("network_transmit_bytes_per_s", "docker", "service", "sum by (service) (rate(%s_network_transmit_bytes_total[{w}]))" % DOCKER,
     "sum by (service) (increase(%s_network_transmit_bytes_total[{d}]))" % DOCKER),
    ("block_read_bytes_per_s", "docker", "service", "sum by (service) (rate(%s_block_read_bytes_total[{w}]))" % DOCKER,
     "sum by (service) (increase(%s_block_read_bytes_total[{d}]))" % DOCKER),
    ("block_write_bytes_per_s", "docker", "service", "sum by (service) (rate(%s_block_write_bytes_total[{w}]))" % DOCKER,
     "sum by (service) (increase(%s_block_write_bytes_total[{d}]))" % DOCKER),
]
# The monitoring stack's own containers would otherwise clutter the report tables.
DEFAULT_EXCLUDE = ("prometheus", "grafana", "docker-stats-exporter")
# Prometheus rejects range queries with more than 11000 points.
MAX_POINTS = 10000


def parse_time(text):
    """Epoch seconds, or an ISO 8601 timestamp (taken as UTC when it has no offset)."""
    try:
        return float(text)
    except ValueError:
        moment = datetime.fromisoformat(text)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return moment.timestamp()


def effective_step(start, end, step):
    return max(step, math.ceil((end - start) / MAX_POINTS))


def prometheus_get(base_url, endpoint, params):
    url = f"{base_url.rstrip('/')}/api/v1/{endpoint}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            body = json.load(response)
    except urllib.error.URLError as error:
        raise RuntimeError(f"Cannot reach Prometheus at {base_url} ({error.reason}); is the monitoring stack up (make monitoring-up)?")
    if body.get("status") != "success":
        raise RuntimeError(f"Prometheus rejected {params.get('query')}: {body.get('error')}")
    return body["data"]["result"]


def collect(fetch, start, end, step, rate_window, exclude=()):
    """fetch(endpoint, params) -> result list. Returns (series, totals).

    series: {(component, kind, metric): [(timestamp, value)]}
    totals: {(component, kind, metric): value moved over the whole window}
    """
    duration = max(1, int(round(end - start)))
    series, totals = {}, {}
    for metric, kind, label, query, total_query in METRICS:
        result = fetch("query_range", {"query": query.replace("{w}", rate_window), "start": start, "end": end, "step": step})
        for item in result:
            if item["metric"][label] in exclude:
                continue
            key = (item["metric"][label], kind, metric)
            series[key] = [(float(t), float(v)) for t, v in item["values"] if v not in ("NaN", "+Inf", "-Inf")]
        if total_query:
            result = fetch("query", {"query": total_query.replace("{d}", f"{duration}s"), "time": end})
            for item in result:
                if item["metric"][label] in exclude:
                    continue
                totals[(item["metric"][label], kind, metric)] = float(item["value"][1])
    return series, totals


def summarize(series, totals):
    rows = []
    for key in sorted(series):
        values = [v for _, v in series[key]]
        if not values:
            continue
        rows.append({
            "component": key[0], "kind": key[1], "metric": key[2],
            "avg": sum(values) / len(values), "max": max(values), "total": totals.get(key, ""),
        })
    return rows


def write_outputs(out_dir, series, totals, start, end, settings):
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "timeseries.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["offset_s", "component", "kind", "metric", "value"])
        for (component, kind, metric), points in sorted(series.items()):
            for timestamp, value in points:
                writer.writerow([f"{timestamp - start:.1f}", component, kind, metric, f"{value:.6g}"])
    rows = summarize(series, totals)
    with (out_dir / "summary.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["component", "kind", "metric", "avg", "max", "total"])
        writer.writeheader()
        for row in rows:
            writer.writerow({k: (f"{v:.6g}" if isinstance(v, float) else v) for k, v in row.items()})
    (out_dir / "run.json").write_text(json.dumps({
        "start": datetime.fromtimestamp(start, timezone.utc).isoformat(),
        "end": datetime.fromtimestamp(end, timezone.utc).isoformat(),
        "duration_s": round(end - start, 3),
        **settings,
    }, indent=2) + "\n")
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", required=True, type=parse_time, help="window start: epoch seconds or ISO 8601 (UTC if no offset)")
    parser.add_argument("--end", type=parse_time, default=None, help="window end; defaults to now")
    parser.add_argument("--out", required=True, type=Path, help="output directory")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--step", type=int, default=2, help="seconds between samples (the scrape interval); raised for very long windows")
    parser.add_argument("--rate-window", default="10s", help="PromQL window for rates; keep it above two scrape intervals")
    parser.add_argument("--exclude", action="append", default=None, metavar="COMPONENT",
                        help="component to leave out (repeatable); default: the monitoring containers. Pass --exclude '' to keep everything")
    parser.add_argument("--label", action="append", default=[], metavar="KEY=VALUE", help="extra run metadata, e.g. --label rate=1000")
    args = parser.parse_args(argv)

    end = args.end if args.end is not None else datetime.now(timezone.utc).timestamp()
    if end <= args.start:
        parser.error("--end must be after --start")
    step = effective_step(args.start, end, args.step)
    labels = dict(item.split("=", 1) for item in args.label if "=" in item)

    def fetch(endpoint, params):
        return prometheus_get(args.prometheus_url, endpoint, params)

    exclude = DEFAULT_EXCLUDE if args.exclude is None else tuple(item for item in args.exclude if item)
    series, totals = collect(fetch, args.start, end, step, args.rate_window, exclude)
    if not series:
        raise RuntimeError("Prometheus returned no samples for this window; check the times and that the stack was running")
    rows = write_outputs(args.out, series, totals, args.start, end,
                         {"step_s": step, "rate_window": args.rate_window, "prometheus_url": args.prometheus_url, "labels": labels})
    print(f"Wrote {len(series)} series ({len(rows)} summary rows) to {args.out}")
    for row in rows:
        if row["metric"] in ("cpu_cores", "memory_bytes"):
            print(f"  {row['kind']:6} {row['component']:24} {row['metric']:12} avg={row['avg']:.4g} max={row['max']:.4g}")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
