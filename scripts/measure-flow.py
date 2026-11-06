#!/usr/bin/env python3
"""Run the complete pipeline once and export its resource usage.

Starts the infrastructure and the monitoring stack, resets the flow, submits the
Flink job, then runs the materializer and the producer while timing the run
itself:

  start  just before the producer launches
  end    the last moment anything still moved: Flink's source consumed a record,
         ema-windows or trade-advisories grew, or the materializer committed

The run is finished once the producer has exited and nothing has moved for
--idle-seconds with the materializer caught up on both result topics. Progress
comes from Prometheus (Flink and Redpanda metrics), so the monitoring stack must
be scraping. Everything after `--` goes to bin/data-producer.

Writes timeseries.csv, summary.csv and run.json (see collect-resource-metrics.py)
plus summary.txt and pipeline.json (event counts, durations, throughput).
"""

import argparse
from datetime import datetime
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time

from flow import flow_lock

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", "compose.yaml", "-f", "compose.monitoring.yaml"]
INFRA_SERVICES = ["postgres", "redpanda-0", "console", "jobmanager", "taskmanager"]
MONITORING_SERVICES = ["docker-stats-exporter", "process-exporter", "prometheus", "grafana"]
RESULT_TOPICS = ("ema-windows", "trade-advisories")
REQUIRED_JOBS = ("docker", "process", "flink-taskmanager", "redpanda")
SOURCE_RECORDS = 'sum(flink_taskmanager_job_task_operator_numRecordsIn{operator_name=~"Source.*"})'
PRODUCER_SUMMARY = re.compile(r"published=(\d+)\s+skipped=(\d+)\s+rejected=(\d+)")


def load_collector():
    spec = importlib.util.spec_from_file_location("collect_resource_metrics", ROOT / "scripts/collect-resource-metrics.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collector = load_collector()


class Settle:
    """Decides when the pipeline has finished, from periodic samples of its state.

    A sample is (signature, caught_up). The signature changes whenever anything
    moves; `last_change` is the time of the latest change and becomes the end of
    the measured window.
    """

    def __init__(self, started):
        self.signature = None
        self.last_change = started
        self.moved = False
        self.caught_up = False

    def update(self, now, signature, caught_up):
        if self.signature is not None and signature != self.signature:
            self.last_change = now
            self.moved = True
        self.signature = signature
        self.caught_up = caught_up

    def done(self, now, producer_finished, idle_seconds):
        return (producer_finished and self.moved and self.caught_up
                and now - self.last_change >= idle_seconds)


def pipeline_sample(source_records, end_offsets, committed):
    """(signature, caught_up) from the Flink source count and the result topics' offsets.

    end_offsets and committed map topic -> offset. A topic with data that the
    materializer has not committed to at all counts as not caught up.
    """
    caught_up = all(
        end_offsets.get(topic, 0) == 0 or committed.get(topic, -1) >= end_offsets[topic]
        for topic in RESULT_TOPICS
    )
    signature = (source_records, tuple(end_offsets.get(t, 0) for t in RESULT_TOPICS),
                 tuple(committed.get(t, -1) for t in RESULT_TOPICS))
    return signature, caught_up


def throughput(events, producer_s, total_s):
    return {
        "producer_events_per_s": round(events / producer_s, 1) if producer_s > 0 else None,
        "end_to_end_events_per_s": round(events / total_s, 1) if total_s > 0 else None,
    }


def format_table(rows):
    """Per component: CPU cores, memory and the bytes moved over the window, in MiB."""
    mib = 1024 * 1024
    by = {}
    for row in rows:
        by.setdefault((row["component"], row["kind"]), {})[row["metric"]] = row

    def value(metrics, metric, column):
        row = metrics.get(metric)
        return float(row[column]) if row and row[column] != "" else None

    def cell(number, scale=1.0, digits=1):
        return f"{number / scale:.{digits}f}" if number is not None else "-"

    lines = [f"{'component':<18}{'cpu avg':>9}{'cpu max':>9}{'mem avg':>9}{'mem max':>9}{'net rx':>9}{'net tx':>9}{'blk rd':>9}{'blk wr':>9}",
             f"{'(cores, MiB)':<18}{'':>9}{'':>9}{'':>9}{'':>9}{'(total)':>9}{'(total)':>9}{'(total)':>9}{'(total)':>9}"]
    for (component, kind), metrics in sorted(by.items()):
        cells = [
            cell(value(metrics, "cpu_cores", "avg"), digits=3), cell(value(metrics, "cpu_cores", "max"), digits=3),
            cell(value(metrics, "memory_bytes", "avg"), mib, 0), cell(value(metrics, "memory_bytes", "max"), mib, 0),
            cell(value(metrics, "network_receive_bytes_per_s", "total"), mib), cell(value(metrics, "network_transmit_bytes_per_s", "total"), mib),
            cell(value(metrics, "block_read_bytes_per_s", "total"), mib), cell(value(metrics, "block_write_bytes_per_s", "total"), mib),
        ]
        lines.append(f"{component + (' *' if kind == 'host' else ''):<18}" + "".join(f"{c:>9}" for c in cells))
    lines.append("* runs on the host; per-process network I/O is not available")
    return "\n".join(lines)


class Prometheus:
    def __init__(self, url):
        self.url = url

    def query(self, expression):
        return collector.prometheus_get(self.url, "query", {"query": expression})

    def scalar(self, expression):
        result = self.query(expression)
        return float(result[0]["value"][1]) if result else 0.0

    def by_topic(self, expression):
        return {item["metric"]["redpanda_topic"]: float(item["value"][1]) for item in self.query(expression)}

    def healthy_jobs(self):
        return {item["metric"]["job"] for item in self.query("up == 1")}

    def sample(self, group):
        topics = "|".join(RESULT_TOPICS)
        ends = self.by_topic(f'redpanda_kafka_max_offset{{redpanda_namespace="kafka",redpanda_topic=~"{topics}"}}')
        committed = self.by_topic(
            f'redpanda_kafka_consumer_group_committed_offset{{redpanda_group="{group}",redpanda_topic=~"{topics}"}}')
        return pipeline_sample(self.scalar(SOURCE_RECORDS), ends, committed), ends


def csv_inputs(producer_args):
    """The -csvPath values among the producer arguments, in order."""
    values = []
    for index, arg in enumerate(producer_args):
        name, separator, value = arg.partition("=")
        if name in ("-csvPath", "--csvPath"):
            if separator:
                values.append(value)
            elif index + 1 < len(producer_args):
                values.append(producer_args[index + 1])
    return values


def check_inputs(producer_args):
    """Resolve the CSV inputs like the producer does, before anything is reset.

    Rejects a missing or dangling path, and two inputs that are the same file
    (for example an alias symlink next to the file it points to), which the
    producer would replay twice.
    """
    inputs = csv_inputs(producer_args)
    if not inputs:
        return []
    sys.path.insert(0, str(ROOT / "validation"))
    from csv_inputs import resolve_csv_paths
    try:
        files = resolve_csv_paths(inputs)
    except (ValueError, OSError) as error:
        raise RuntimeError(f"Cannot use the CSV input: {error}. Nothing was reset.")
    seen = {}
    for path in files:
        real = path.resolve()
        if real in seen:
            raise RuntimeError(f"{seen[real]} and {path} are the same file ({real}) and would be replayed twice; "
                               "list each file once, e.g. CSV=\"dataset/day08.csv dataset/day09.csv\". Nothing was reset.")
        seen[real] = path
    return files


def run(command, **kwargs):
    subprocess.run(command, cwd=ROOT, check=True, **kwargs)


def wait_for_monitoring(prometheus, timeout=120):
    deadline = time.monotonic() + timeout
    missing = set(REQUIRED_JOBS)
    while time.monotonic() < deadline:
        try:
            missing = set(REQUIRED_JOBS) - prometheus.healthy_jobs()
        except RuntimeError:
            pass
        if not missing:
            return
        time.sleep(2)
    raise RuntimeError(f"Prometheus is not scraping {', '.join(sorted(missing))}; check make monitoring-up and the targets page")


def stop_children(children):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    active = [child for child in children if child.poll() is None]
    for child in active:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 10
    for child in active:
        try:
            child.wait(timeout=max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()


def measure(args, producer_args):
    prometheus = Prometheus(args.prometheus_url)
    group = os.environ.get("MATERIALIZER_GROUP_ID", "materializer")
    children = []
    producer_output = []

    def echo(stream):
        for line in stream:
            sys.stdout.write(line)
            sys.stdout.flush()
            producer_output.append(line)

    print("Starting infrastructure and monitoring...", flush=True)
    run([*COMPOSE, "up", "-d", "--wait", "--wait-timeout", "120", *INFRA_SERVICES, *MONITORING_SERVICES])
    wait_for_monitoring(prometheus)
    print("Resetting the flow for a clean run...", flush=True)
    run([sys.executable, "scripts/reset-flow.py"])

    with flow_lock():
        try:
            run([sys.executable, "scripts/submit-flink-job.py", "--timeout", str(args.flink_start_timeout), "--", "--starting-offsets", "earliest"])
            started = time.time()
            materializer = subprocess.Popen([str(ROOT / "bin/materializer")], cwd=ROOT, start_new_session=True)
            children.append(materializer)
            producer = subprocess.Popen([str(ROOT / "bin/data-producer"), *producer_args], cwd=ROOT, start_new_session=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            children.append(producer)
            threading.Thread(target=echo, args=(producer.stdout,), daemon=True).start()
            print(f"Measuring from {datetime.fromtimestamp(started).isoformat(timespec='seconds')} (epoch {int(started)}).", flush=True)

            settle = Settle(started)
            producer_done = None
            deadline = None
            next_report = 0
            while True:
                if materializer.poll() is not None:
                    raise RuntimeError(f"Materializer exited with status {materializer.returncode}")
                now = time.time()
                if producer_done is None and producer.poll() is not None:
                    if producer.returncode:
                        raise RuntimeError(f"Producer exited with status {producer.returncode}")
                    producer_done = now
                    deadline = time.monotonic() + args.timeout
                    print("Producer finished; waiting for Flink and the materializer to settle...", flush=True)
                (signature, caught_up), _ = prometheus.sample(group)
                settle.update(now, signature, caught_up)
                if settle.done(now, producer_done is not None, args.idle_seconds):
                    break
                if deadline is not None and time.monotonic() > deadline:
                    raise RuntimeError(f"The pipeline did not settle within {args.timeout}s of the producer finishing")
                if now >= next_report and producer_done is not None:
                    print(f"  source={signature[0]:.0f} ema={signature[1][0]} advisories={signature[1][1]} committed={signature[2]}", flush=True)
                    next_report = now + 15
                time.sleep(args.poll)
        finally:
            stop_children(children)

    end = settle.last_change
    time.sleep(args.scrape_grace)
    summary = next((PRODUCER_SUMMARY.search(line) for line in reversed(producer_output) if PRODUCER_SUMMARY.search(line)), None)
    _, ends = prometheus.sample(group)
    events = prometheus.scalar(SOURCE_RECORDS)
    return {
        "start": started, "end": end, "producer_done": producer_done,
        "events": int(events), "ema_windows": int(ends.get("ema-windows", 0)), "trade_advisories": int(ends.get("trade-advisories", 0)),
        "producer": {"published": int(summary[1]), "skipped": int(summary[2]), "rejected": int(summary[3])} if summary else None,
    }


def export(args, result, producer_args):
    started, end = result["start"], result["end"]
    step = collector.effective_step(started, end, args.step)

    def fetch(endpoint, params):
        return collector.prometheus_get(args.prometheus_url, endpoint, params)

    series, totals = collector.collect(fetch, started, end, step, args.rate_window, collector.DEFAULT_EXCLUDE)
    if not series:
        raise RuntimeError("Prometheus returned no samples for the measured window")
    labels = dict(item.split("=", 1) for item in args.label if "=" in item)
    rows = collector.write_outputs(args.out, series, totals, started, end, {
        "step_s": step, "rate_window": args.rate_window, "prometheus_url": args.prometheus_url,
        "labels": labels, "producer_args": producer_args})
    total_s = end - started
    producer_s = (result["producer_done"] or end) - started
    pipeline = {
        "total_s": round(total_s, 1), "producer_s": round(producer_s, 1),
        "flink_source_records": result["events"], "ema_windows": result["ema_windows"],
        "trade_advisories": result["trade_advisories"], "producer": result["producer"],
        **throughput(result["events"], producer_s, total_s),
    }
    (args.out / "pipeline.json").write_text(json.dumps(pipeline, indent=2) + "\n")
    table = format_table(rows)
    (args.out / "summary.txt").write_text(table + "\n")
    return pipeline, table


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    producer_args = argv[argv.index("--") + 1:] if "--" in argv else []
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=ROOT / "bin/metrics" / datetime.now().strftime("%Y%m%d-%H%M%S"), help="output directory")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--idle-seconds", type=float, default=20, help="how long nothing may move before the run counts as finished")
    parser.add_argument("--timeout", type=float, default=3600, help="seconds to wait for the pipeline to settle after the producer exits")
    parser.add_argument("--poll", type=float, default=2, help="seconds between progress checks")
    parser.add_argument("--step", type=int, default=2, help="seconds between exported samples")
    parser.add_argument("--rate-window", default="10s")
    parser.add_argument("--flink-start-timeout", type=int, default=120)
    parser.add_argument("--scrape-grace", type=float, default=6, help="seconds to let Prometheus scrape past the end before exporting")
    parser.add_argument("--label", action="append", default=[], metavar="KEY=VALUE", help="extra run metadata")
    args = parser.parse_args(argv[:argv.index("--")] if "--" in argv else argv)
    args.out = args.out.resolve()

    for binary in ("bin/materializer", "bin/data-producer"):
        if not (ROOT / binary).is_file():
            raise RuntimeError(f"{binary} is missing; run make build-materializer build-data-producer")
    files = check_inputs(producer_args)
    if files:
        print(f"Input: {len(files)} file(s): {', '.join(str(path) for path in files)}", flush=True)
    result = measure(args, producer_args)
    pipeline, table = export(args, result, producer_args)
    print(f"\n{table}\n")
    print(f"Events through Flink: {pipeline['flink_source_records']}  EMA windows: {pipeline['ema_windows']}  advisories: {pipeline['trade_advisories']}")
    print(f"Producer {pipeline['producer_s']}s ({pipeline['producer_events_per_s']} events/s), end to end {pipeline['total_s']}s ({pipeline['end_to_end_events_per_s']} events/s)")
    print(f"Results in {args.out}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Measurement interrupted; nothing was exported. Use make reset-flow before the next run.", file=sys.stderr)
        sys.exit(130)
    except (RuntimeError, subprocess.CalledProcessError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
