#!/usr/bin/env python3
"""Tests for verify_flow.py.

End-to-end wiring of the flow-validation CLI: a fake `consume` stands
in for kafka_topics.consume_all and returns serialized real protobuf
bytes (PriceEvent records for the events topic; EmaWindow/TradeAdvisory
records rebuilt from the shadow's keep-all table for the other two), so
run() exercises the whole consume -> shadow replay -> decode -> compare
-> report path.  Cases: the happy path passes, writes the report with
the expected counts, and prints a summary; an unexplained close-price
mismatch fails with the report still written and the example window
printed; the console caps unexplained examples at 10; main() returns
0/1, parses the default tolerance, and creates a missing report parent
directory.
"""

import csv
import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from pathlib import Path
from unittest import mock

from google.protobuf.timestamp_pb2 import Timestamp

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bindings  # noqa: E402
import ema_replay as er  # noqa: E402
import generate_validation_data as gvd  # noqa: E402
import shadow as sh  # noqa: E402
import verify_flow as vf  # noqa: E402

SYM = "AAA.NL"
OTHER = "BB.DE"
BROKERS = "test:19092"
TOPICS = {"events": "trade-events", "ema": "ema-windows",
          "advisories": "trade-advisories"}
TOLERANCE = Decimal("1e-9")
ACTION = {"BUY": bindings.ADVISORY_ACTION_BUY,
          "SELL": bindings.ADVISORY_ACTION_SELL}


def ts_ms(ms):
    ts = Timestamp()
    ts.seconds, ts.nanos = ms // 1000, (ms % 1000) * 1_000_000
    return ts


def make_stream(specs):
    """(symbol, tt_ms, micros, source_row) specs -> (records, shadow).

    Records are serialized PriceEvent bytes at offsets 0.. in arrival
    order; the shadow is the exact replay run() must recompute, with
    order = the Kafka offset.
    """
    records, events = [], []
    for offset, (symbol, tt_ms, micros, source_row) in enumerate(specs):
        event = bindings.PriceEvent(
            symbol=symbol, security_type=bindings.SECURITY_TYPE_EQUITY,
            trade_time=ts_ms(tt_ms), last_price_micros=micros,
            source_row=source_row)
        records.append((offset, event.SerializeToString()))
        events.append(sh.ShadowEvent(symbol, tt_ms, micros, offset))
    return records, sh.shadow_replay(events)


def price_str(ema_micros):
    return str(ema_micros / Decimal(10**6))


def actual_topic_records(result, table="keep_all"):
    """Serialized ema/advisory records rebuilt from one shadow table."""
    ema_records, advisory_records = [], []
    for (symbol, widx), row in sorted(getattr(result, table).items()):
        window = bindings.EmaWindow(
            symbol=symbol, window_start=ts_ms(widx * er.WindowMs),
            window_end=ts_ms((widx + 1) * er.WindowMs),
            closing_price_micros=row.close_micros,
            ema38=price_str(row.ema38_micros),
            ema100=price_str(row.ema100_micros))
        ema_records.append((len(ema_records), window.SerializeToString()))
        if row.advisory:
            advisory = bindings.TradeAdvisory(
                ema_window=window, action=ACTION[row.advisory],
                previous_ema38=price_str(row.prev_ema38_micros),
                previous_ema100=price_str(row.prev_ema100_micros))
            advisory_records.append(
                (len(advisory_records), advisory.SerializeToString()))
    return ema_records, advisory_records


def corrupt_closes(ema_records, indices, delta=1):
    """Copies of the records with some windows' closing prices shifted."""
    records = list(ema_records)
    for index in indices:
        window = bindings.EmaWindow()
        window.ParseFromString(records[index][1])
        window.closing_price_micros += delta
        records[index] = (records[index][0], window.SerializeToString())
    return records


def fake_consume(event_records, ema_records, advisory_records):
    """consume_all stand-in recording every (brokers, topic) call."""
    by_topic = {TOPICS["events"]: event_records, TOPICS["ema"]: ema_records,
                TOPICS["advisories"]: advisory_records}
    calls = []

    def consume(brokers, topic):
        calls.append((brokers, topic))
        return list(by_topic[topic])

    consume.calls = calls
    return consume


class RunHappyPathTest(unittest.TestCase):
    SPECS = [
        (SYM, 10_000, 15_000_000, 1),    # w0 -> BUY, at-risk window
        (SYM, 700_000, 25_000_000, 2),   # w2, no crossover
        (OTHER, 50_000, 11_000_000, 3),  # w0 -> BUY
    ]

    def test_passes_writes_report_prints_summary(self):
        event_records, shadow_out = make_stream(self.SPECS)
        ema_records, advisory_records = actual_topic_records(shadow_out)
        consume = fake_consume(event_records, ema_records, advisory_records)
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "flow-report.json"
            out = io.StringIO()
            with redirect_stdout(out):
                result = vf.run(BROKERS, TOPICS, report_path, TOLERANCE,
                                consume=consume)
            self.assertTrue(result.passed)
            self.assertIsNone(result.reference)
            self.assertEqual(result.compare.total_windows, 3)
            self.assertEqual(result.compare.matched, 3)
            self.assertEqual(result.compare.unexplained, [])
            report = json.loads(report_path.read_text())
            self.assertNotIn("reference", report)
            self.assertEqual(report["topics"], TOPICS)
            self.assertEqual(report["tolerance"], str(TOLERANCE))
            self.assertEqual(report["total_events"], 3)
            self.assertEqual(report["total_windows"], 3)
            self.assertEqual(report["matched"], 3)
            self.assertEqual(report["at_risk_windows"], 1)
            self.assertEqual(report["at_risk_events"], 1)
            self.assertTrue(report["passed"])
            self.assertEqual(report["unexplained"],
                             {"count": 0, "truncated": False, "windows": []})
        self.assertEqual(consume.calls, [
            (BROKERS, "trade-events"), (BROKERS, "ema-windows"),
            (BROKERS, "trade-advisories")])
        self.assertIn("unexplained 0", out.getvalue())
        self.assertIn("passed: True", out.getvalue())


class RunMismatchTest(unittest.TestCase):
    def test_unexplained_mismatch_fails_but_report_written(self):
        event_records, shadow_out = make_stream([(SYM, 10_000, 15_000_000, 1)])
        ema_records, advisory_records = actual_topic_records(shadow_out)
        consume = fake_consume(event_records,
                               corrupt_closes(ema_records, [0]),
                               advisory_records)
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "flow-report.json"
            out = io.StringIO()
            with redirect_stdout(out):
                result = vf.run(BROKERS, TOPICS, report_path, TOLERANCE,
                                consume=consume)
            self.assertFalse(result.passed)
            self.assertEqual(result.compare.matched, 0)
            self.assertEqual(len(result.compare.unexplained), 1)
            report = json.loads(report_path.read_text())
            self.assertFalse(report["passed"])
            self.assertEqual(report["unexplained"]["count"], 1)
            self.assertEqual(report["unexplained"]["windows"][0], {
                "symbol": SYM, "window_start_ms": 0,
                "parts": ["closing_price_micros 15000001 != 15000000"]})
        text = out.getvalue()
        self.assertIn("passed: False", text)
        self.assertIn(SYM, text)  # the example window is printed

    def test_summary_caps_unexplained_examples_at_ten(self):
        specs = [(f"S{i:02d}.X", 10_000, 15_000_000, i + 1) for i in range(11)]
        event_records, shadow_out = make_stream(specs)
        ema_records, advisory_records = actual_topic_records(shadow_out)
        consume = fake_consume(event_records,
                               corrupt_closes(ema_records, range(11)),
                               advisory_records)
        with tempfile.TemporaryDirectory() as tmp:
            out = io.StringIO()
            with redirect_stdout(out):
                result = vf.run(BROKERS, TOPICS, Path(tmp) / "r.json",
                                TOLERANCE, consume=consume)
            self.assertFalse(result.passed)
            self.assertEqual(len(result.compare.unexplained), 11)
            report = json.loads((Path(tmp) / "r.json").read_text())
            self.assertEqual(report["unexplained"]["count"], 11)
        self.assertEqual(
            out.getvalue().count(
                "closing_price_micros 15000001 != 15000000"), 10)


def write_reference(directory, shadow_out, table="drop_model",
                    out_of_orderness_ms=er.OutOfOrdernessMs):
    """A generator-format reference (ema_windows.csv + summary.json) of one shadow table.

    reference_test.py pins the real generator's output against the loader;
    here only the columns the loader reads are written.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    cest = timezone(timedelta(hours=2))
    with open(directory / "ema_windows.csv", "w", newline="") as ema_file:
        writer = csv.writer(ema_file, lineterminator="\n")
        writer.writerow(["symbol", "window_start", "event_count", "close_micros",
                         "ema38_micros", "ema100_micros", "prev_ema38_micros",
                         "prev_ema100_micros", "advisory"])
        with localcontext() as ctx:
            ctx.prec = 50
            for (symbol, widx), row in sorted(getattr(shadow_out, table).items()):
                start = datetime.fromtimestamp(widx * er.WindowMs / 1000, tz=cest)
                writer.writerow([symbol, start.strftime("%Y-%m-%dT%H:%M"),
                                 row.event_count, row.close_micros,
                                 gvd.fmt_ema(row.ema38_micros), gvd.fmt_ema(row.ema100_micros),
                                 gvd.fmt_ema(row.prev_ema38_micros),
                                 gvd.fmt_ema(row.prev_ema100_micros), row.advisory])
    (directory / "summary.json").write_text(json.dumps(
        {"spec": {"ema_init": "prev-zero", "out_of_orderness_ms": out_of_orderness_ms}}))


class RunWithReferenceTest(unittest.TestCase):
    SPECS = RunHappyPathTest.SPECS

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.event_records, self.shadow_out = make_stream(self.SPECS)
        ema_records, advisory_records = actual_topic_records(self.shadow_out)
        self.consume = fake_consume(self.event_records, ema_records, advisory_records)

    def run_flow(self, reference_dir):
        out = io.StringIO()
        with redirect_stdout(out):
            result = vf.run(BROKERS, TOPICS, self.dir / "report.json", TOLERANCE,
                            consume=self.consume, reference_dir=reference_dir)
        return result, out.getvalue(), json.loads((self.dir / "report.json").read_text())

    def test_faithful_reference_passes_and_is_reported(self):
        write_reference(self.dir / "ref", self.shadow_out)
        result, text, report = self.run_flow(self.dir / "ref")
        self.assertTrue(result.passed)
        self.assertTrue(result.reference.passed)
        self.assertTrue(report["passed"])
        self.assertEqual(report["reference"]["side"], "drop_model")
        # The third event arrives after the 700 s event moved the watermark past its
        # window, so the drop model (the generator's rule) holds 2 of the 3 windows.
        self.assertEqual(report["reference"]["matched"], 2)
        self.assertEqual(report["reference"]["dir"], str(self.dir / "ref"))
        self.assertIn("reference (drop_model): 2 of 2 windows match", text)
        self.assertIn("passed: True", text)

    def test_reference_mismatch_fails_the_flow_though_the_processor_matches(self):
        # The CSV said the first close was a micro higher than what was published.
        _, csv_side = make_stream([(SYM, 10_000, 15_000_001, 1)] + list(self.SPECS[1:]))
        write_reference(self.dir / "ref", csv_side)
        result, text, report = self.run_flow(self.dir / "ref")
        self.assertTrue(result.compare.passed)
        self.assertFalse(result.reference.passed)
        self.assertFalse(result.passed)
        self.assertFalse(report["passed"])
        self.assertGreaterEqual(report["reference"]["mismatches"]["count"], 1)
        self.assertIn("close_micros", text)
        self.assertIn("passed: False", text)

    def test_unusable_reference_fails_before_any_topic_is_read(self):
        with self.assertRaisesRegex(FileNotFoundError, "generate-reference"):
            self.run_flow(self.dir / "nowhere")
        self.assertEqual(self.consume.calls, [])


class MainTest(unittest.TestCase):
    ARGS = ["--brokers", BROKERS, "--events-topic", "trade-events",
            "--ema-topic", "ema-windows", "--advisory-topic",
            "trade-advisories"]

    def test_reference_dir_flag_decides_the_exit_code(self):
        event_records, shadow_out = make_stream([(SYM, 10_000, 15_000_000, 1)])
        ema_records, advisory_records = actual_topic_records(shadow_out)
        consume = fake_consume(event_records, ema_records, advisory_records)
        _, other = make_stream([(SYM, 10_000, 16_000_000, 1)])
        with tempfile.TemporaryDirectory() as tmp:
            codes = {}
            for name, table_source in (("good", shadow_out), ("bad", other)):
                write_reference(Path(tmp) / name, table_source)
                with mock.patch("kafka_topics.consume_all", consume):
                    with redirect_stdout(io.StringIO()):
                        codes[name] = vf.main(self.ARGS + [
                            "--report", str(Path(tmp) / f"{name}.json"),
                            "--reference-dir", str(Path(tmp) / name)])
            self.assertEqual(codes, {"good": 0, "bad": 1})

    def test_returns_0_and_creates_missing_report_parent(self):
        event_records, shadow_out = make_stream([(SYM, 10_000, 15_000_000, 1)])
        ema_records, advisory_records = actual_topic_records(shadow_out)
        consume = fake_consume(event_records, ema_records, advisory_records)
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "nested" / "dir" / "report.json"
            with mock.patch("kafka_topics.consume_all", consume):
                with redirect_stdout(io.StringIO()):
                    code = vf.main(self.ARGS + ["--report", str(report_path)])
            self.assertEqual(code, 0)
            self.assertTrue(report_path.is_file())
            report = json.loads(report_path.read_text())
            self.assertEqual(report["topics"], TOPICS)
            self.assertEqual(report["tolerance"], str(Decimal("1e-9")))
            self.assertTrue(report["passed"])

    def test_returns_1_on_unexplained_mismatch(self):
        event_records, shadow_out = make_stream([(SYM, 10_000, 15_000_000, 1)])
        ema_records, advisory_records = actual_topic_records(shadow_out)
        consume = fake_consume(event_records,
                               corrupt_closes(ema_records, [0]),
                               advisory_records)
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "report.json"
            with mock.patch("kafka_topics.consume_all", consume):
                with redirect_stdout(io.StringIO()):
                    code = vf.main(self.ARGS + ["--report", str(report_path)])
            self.assertEqual(code, 1)
            report = json.loads(report_path.read_text())
            self.assertFalse(report["passed"])

    def test_garbage_tolerance_is_a_clean_usage_error(self):
        # --tolerance soon must produce argparse's usage error, not a
        # raw decimal.InvalidOperation traceback.
        with redirect_stderr(io.StringIO()) as err:
            with self.assertRaises(SystemExit) as raised:
                vf.main(["--tolerance", "soon"])
        self.assertEqual(raised.exception.code, 2)
        self.assertIn("usage:", err.getvalue().lower())
        self.assertIn("--tolerance", err.getvalue())


def module_follows_override(module_name, overrides, expression):
    """`expression` evaluated against `module_name` in a fresh interpreter
    (copied from config_test.py; see there for the rationale)."""
    lines = ["import sys", "import inspect", "from decimal import Decimal",
             f"sys.path.insert(0, {str(Path(__file__).resolve().parent)!r})",
             "import config"]
    lines += [f"config.{name} = {value!r}" for name, value in overrides.items()]
    lines += [f"import {module_name}", f"print({expression})"]
    result = subprocess.run([sys.executable, "-c", "\n".join(lines)],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise AssertionError(f"override subprocess failed:\n{result.stderr}")
    return result.stdout.strip()


class VerifyFlowFollowsConfigTest(unittest.TestCase):
    def test_cli_defaults_follow_config(self):
        self.assertEqual(module_follows_override(
            "verify_flow", {"BROKERS": "broker:9092"},
            "verify_flow.build_parser().get_default('brokers')"), "broker:9092")
        self.assertEqual(module_follows_override(
            "verify_flow", {"TOPIC_EVENTS": "evt-x"},
            "verify_flow.build_parser().get_default('events_topic')"), "evt-x")
        self.assertEqual(module_follows_override(
            "verify_flow", {"EMA_TOLERANCE": Decimal("1e-3")},
            "str(verify_flow.build_parser().get_default('tolerance'))"), "0.001")
        self.assertEqual(module_follows_override(
            "verify_flow", {"TOPIC_EMA_WINDOWS": "ema-x"},
            "verify_flow.build_parser().get_default('ema_topic')"), "ema-x")
        self.assertEqual(module_follows_override(
            "verify_flow", {"TOPIC_TRADE_ADVISORIES": "adv-x"},
            "verify_flow.build_parser().get_default('advisory_topic')"),
            "adv-x")
        self.assertEqual(module_follows_override(
            "verify_flow", {"DEFAULT_REPORT_PATH": "r.json"},
            "verify_flow.build_parser().get_default('report')"), "r.json")
        self.assertEqual(module_follows_override(
            "verify_flow", {"SUMMARY_EXAMPLES": 7},
            "verify_flow.SUMMARY_EXAMPLES"), "7")


if __name__ == "__main__":
    unittest.main(verbosity=2)
