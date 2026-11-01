#!/usr/bin/env python3
"""Live producer SIGKILL/resume correctness tests; requires a running local cluster."""
import argparse
from collections import Counter
from datetime import datetime, timezone, timedelta
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from fractions import Fraction
import json
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
REST = "http://localhost:8081"
BASE = 1636693200000
SYMBOLS = ("REPLAY.FR", "REPLAY.ETR")
TERMINAL = {"FINISHED", "FAILED", "CANCELED", "SUSPENDED"}


def fixture(extended=False):
    rows = []
    for w in range(90):
        ticks = 200 if extended and w == 0 else 2
        for tick in range(ticks):
            for index, symbol in enumerate(SYMBOLS):
                close = (100 if w < 20 else 1 if w < 60 else 1000) * (index + 1)
                price = close * 1000000 if tick == ticks - 1 else close * 500000
                offset = 1000 + tick * 1000 if ticks > 2 else (1000 if tick == 0 else 299000)
                rows.append(dict(symbol=symbol, row=len(rows) + 14, time=BASE + w * 300000 + offset, price=str(price)))
    rows.append(dict(symbol="WATERMARK.GUARD", row=len(rows) + 14, time=BASE + 120 * 300000 + 1000, price="1000000"))
    return rows


def write_fixture(path, rows):
    text = "# metadata\n" * 11 + "ID,SecType,Last,Trading time,Trading date\n# descriptions\n"
    for row in rows:
        when = datetime.fromtimestamp(row["time"] / 1000, timezone(timedelta(hours=2)))
        price = int(row["price"])
        text += f"{row['symbol']},E,{price // 1000000}.{price % 1000000:06d},{when.strftime('%H:%M:%S.%f')[:-3]},{when:%d-%m-%Y}\n"
    path.write_text(text)


def reference(rows):
    closes = {}
    for row in rows:
        if row["symbol"] == "WATERMARK.GUARD":
            continue
        key = (row["symbol"], row["time"] // 300000 * 300000)
        old = closes.get(key)
        if old is None or (row["time"], row["row"]) > (old["time"], old["row"]):
            closes[key] = row
    state = {}
    windows, advisories = [], []
    for (symbol, start), row in sorted(closes.items()):
        previous = state.get(symbol, (Decimal(0), Decimal(0)))
        current = []
        for period, old in zip((38, 100), previous):
            exact = (Fraction(int(row["price"]), 1000000) * 2 + Fraction(old) * (period - 1)) / (period + 1)
            with localcontext() as ctx:
                ctx.prec = 34
                ctx.rounding = ROUND_HALF_EVEN
                current.append(Decimal(exact.numerator) / Decimal(exact.denominator))
        value = dict(symbol=symbol, start=start, end=start + 300000, price=row["price"], ema38=str(current[0]), ema100=str(current[1]))
        windows.append(value)
        action = None
        if previous[0] <= previous[1] and current[0] > current[1]:
            action = "ADVISORY_ACTION_BUY"
        elif previous[0] >= previous[1] and current[0] < current[1]:
            action = "ADVISORY_ACTION_SELL"
        if action:
            advisories.append(dict(value, action=action, previousEma38=str(previous[0]), previousEma100=str(previous[1])))
        state[symbol] = tuple(current)
    return windows, advisories


def compare(expected, actual):
    normalize = lambda v: {k: Decimal(x) if "ema" in k.lower() else x for k, x in v.items()}
    key = lambda v: (v["symbol"], v["start"])
    wanted = {key(v): normalize(v) for v in expected}
    got, conflicting = {}, 0
    for value in actual:
        k = key(value)
        v = normalize(value)
        if k in got and got[k] != v:
            conflicting += 1
        got[k] = v
    return dict(expected=len(wanted), records=len(actual), unique=len(got), duplicates=len(actual)-len(got),
                missing=len(wanted.keys()-got.keys()), extra=len(got.keys()-wanted.keys()),
                mismatches=sum(got[k] != wanted[k] for k in got.keys() & wanted.keys()), conflictingDuplicates=conflicting)


class Verification:
    def __init__(self, output):
        self.output = output
        self.output.mkdir(parents=True, exist_ok=True)
        self.reader = self.output / "read-records"
        self.token = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    def run(self, args):
        result = subprocess.run(args, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        with (self.output / "commands.log").open("a") as f:
            f.write(repr(args) + "\n" + result.stdout + "\n")
        if result.returncode:
            raise RuntimeError(result.stdout)
        return result.stdout

    def get(self, path):
        with urllib.request.urlopen(REST + path, timeout=5) as response:
            return json.load(response)

    def wait(self, description, predicate, timeout=120):
        deadline = time.monotonic() + timeout
        last = 0
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            if time.monotonic() - last > 10:
                print("Waiting:", description, flush=True)
                last = time.monotonic()
            time.sleep(0.1)
        raise RuntimeError("Timed out: " + description)

    def size(self, topic):
        data = json.loads(self.run(["docker", "compose", "exec", "-T", "redpanda-0", "rpk", "topic", "describe", topic, "-p", "--format", "json", "-X", "brokers=redpanda-0:9092"]))
        return data[0]["partitions"][0]["high_watermark"]

    def submit(self, names, restore=None):
        args = ["docker", "compose", "exec", "-T", "jobmanager", "flink", "run", "-d"]
        if restore:
            args += ["--fromSavepoint", restore]
        args += ["/tmp/producer-recovery-test.jar", "--topic", names[0], "--output-topic", names[1], "--advisory-topic", names[2],
                 "--group-id", names[0], "--sample-limit", "0", "--checkpoint-interval-ms", "1000", "--starting-offsets", "earliest"]
        out = self.run(args)
        match = re.search(r"JobID ([0-9a-f]{32})", out)
        if not match:
            raise RuntimeError(out)
        job = match[1]
        self.wait("test job running", lambda: self.get("/jobs/"+job)["state"] == "RUNNING" and all(
            v["tasks"].get("RUNNING", 0) == v["parallelism"] for v in self.get("/jobs/"+job)["vertices"]))
        return job

    def stop(self, job):
        self.run(["docker", "compose", "exec", "-T", "jobmanager", "flink", "cancel", job])
        self.wait("test job cancellation", lambda: self.get("/jobs/"+job)["state"] == "CANCELED")

    def checkpoint(self, job):
        after = time.time() * 1000
        def completed():
            cp = self.get(f"/jobs/{job}/checkpoints").get("latest", {}).get("completed")
            return cp if cp and cp["trigger_timestamp"] >= after else None
        return self.wait("Flink checkpoint", completed)

    def read(self, topic, kind, label):
        path = self.output / (label + ".json")
        self.run([str(self.reader), "-topic="+topic, "-kind="+kind, "-count="+str(self.size(topic)), "-output="+str(path)])
        return json.loads(path.read_text())

    def case(self, label):
        print("CASE:", label, flush=True)
        rows = fixture(extended=label == "open-window")
        csv = self.output / (label + ".csv")
        write_fixture(csv, rows)
        checkpoint = self.output / (label + "-checkpoint.json")
        names = [f"verify-producer-{self.token}-{label}-{suffix}" for suffix in ("input", "ema", "advice")]
        self.run(["docker", "compose", "exec", "-T", "redpanda-0", "rpk", "topic", "create", *names, "--partitions", "1", "--replicas", "1", "-X", "brokers=redpanda-0:9092"])
        job, producer = None, None
        log = (self.output / (label + "-producer.log")).open("w")
        args = ["./bin/data-producer", "-csvPath="+str(csv), "-topic="+names[0]]
        try:
            job = self.submit(names)
            if label != "baseline":
                rate, target = (200, 260) if label == "open-window" else (100, 140)
                producer = subprocess.Popen(args + ["-rate="+str(rate), "-checkpointPath="+str(checkpoint)], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.wait("producer past durable checkpoint", lambda: checkpoint.exists() and json.loads(checkpoint.read_text())["last_completed_row"] > 14 and self.size(names[0]) >= target)
                producer.send_signal(signal.SIGSTOP)
                if label == "open-window":
                    assert self.size(names[1]) == 0, "Window finalized before open-window crash"
                else:
                    self.wait("windows finalized before producer crash", lambda: self.size(names[1]) > 0)
                producer.kill()  # SIGKILL prevents the producer's final checkpoint save.
                producer.wait(timeout=10)
                saved = json.loads(checkpoint.read_text())["last_completed_row"]
                prefix = self.size(names[0])
                assert prefix + 13 > saved, "No uncheckpointed acknowledged input; test did not inject replay"
                print(f"Killed producer: saved row {saved}, broker prefix {prefix} records", flush=True)
                if label == "combined-restore":
                    self.checkpoint(job)
                    self.stop(job)
                    restore = self.get(f"/jobs/{job}/checkpoints")["latest"]["completed"]["external_path"]
                self.run(args + ["-checkpointPath="+str(checkpoint), "-resume"])
                if label == "combined-restore":
                    job = self.submit(names, restore)
            else:
                self.run(args)
            expected = reference(rows)
            self.wait("target EMA windows", lambda: self.size(names[1]) >= len(expected[0]))
            self.checkpoint(job)
            inputs = self.read(names[0], "input", label+"-inputs")
            unique = {}
            for value in inputs:
                assert value["row"] not in unique or unique[value["row"]] == value, "Conflicting duplicate input"
                unique[value["row"]] = value
            assert unique == {r["row"]:r for r in rows}, "Missing or changed fixture input"
            duplicates = len(inputs)-len(unique)
            assert label == "baseline" or duplicates > 0, "No duplicate input observed"
            result = dict(case=label, jobId=job, inputRecords=len(inputs), uniqueInputRecords=len(unique), duplicatedInputRecords=duplicates)
            for kind, topic, wanted in zip(("ema", "advisory"), names[1:], expected):
                result[kind] = compare(wanted, self.read(topic, kind, label+"-"+kind))
            result["passed"] = all(not any(result[k][f] for f in ("missing", "extra", "mismatches", "conflictingDuplicates")) for k in ("ema", "advisory"))
            print(json.dumps(result), flush=True)
            return result
        finally:
            if producer is not None and producer.poll() is None:
                producer.kill()
                producer.wait()
            log.close()
            if job and self.get("/jobs/"+job)["state"] not in TERMINAL:
                self.stop(job)

    def main(self):
        if self.get("/overview")["slots-available"] < 1:
            raise RuntimeError("At least one free Flink slot is required; existing jobs will not be stopped")
        self.run(["go", "build", "-o", str(self.reader), "scripts/verification/producer-recovery-read.go"])
        self.run(["docker", "compose", "cp", "bin/streaming-processor.jar", "jobmanager:/tmp/producer-recovery-test.jar"])
        report = {"cases": [], "outputGuarantee": "at-least-once; duplicate identical outputs are counted"}
        for label in ("baseline", "open-window", "closed-window", "combined-restore"):
            report["cases"].append(self.case(label))
            (self.output/"report.json").write_text(json.dumps(report, indent=2))
        report["passed"] = all(c["passed"] for c in report["cases"])
        (self.output/"report.json").write_text(json.dumps(report, indent=2))
        if not report["passed"]:
            raise RuntimeError("Recovery results differ from reference; inspect report.json")
        print("PASS:", self.output/"report.json", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT/"bin"/("producer-recovery-"+datetime.now().strftime("%Y%m%d-%H%M%S")))
    options = parser.parse_args()
    Verification(options.output_dir.resolve()).main()
