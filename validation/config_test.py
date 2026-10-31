#!/usr/bin/env python3
"""Tests for config.py: the values of every tunable constant, and the
wiring that consumers follow config.

The value test pins each constant to its spec value in the single
editable place; the wiring tests (added per consumer module) override a
config value in a fresh interpreter before importing the module and
assert the module follows - CPython interns small ints, strings, and
floats, so `is`-identity checks would pass even for re-hardcoded
copies.  config.py itself must import nothing from the package: the
flow scripts load it standalone, and a package import would cycle.
"""

import subprocess
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402

PACKAGE_MODULES = ("ema_replay", "shadow", "compare", "verify_flow",
                  "kafka_topics", "generate_validation_data", "bindings")


def module_follows_override(module_name, overrides, expression):
    """`expression` evaluated against `module_name` in a fresh interpreter.

    Runs sys.executable -c: validation/ goes on sys.path, `config` is
    imported, `overrides` (attr -> literal value, Decimal pre-imported)
    are applied BEFORE importing the target module, then `expression`
    (any Python expression naming the module, e.g. "ema_replay.WindowMs")
    is printed.  Returns stdout stripped."""
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


def script_follows_override(script, overrides, expression):
    """Like module_follows_override for the hyphenated flow scripts.

    The script (a Path) is loaded via importlib.util.spec_from_file_location
    as `module` - the pattern scripts/*_test.py use - so `expression`
    references it as `module` (e.g. "module.REST_URL").  The script's own
    directory goes on sys.path last, as when Python runs the script, so
    its sibling imports (reset-flow.py's `from flow import flow_lock`)
    resolve without shadowing validation/'s `config`."""
    lines = ["import sys", "import importlib.util",
             "from decimal import Decimal",
             f"sys.path.insert(0, {str(Path(__file__).resolve().parent)!r})",
             f"sys.path.append({str(Path(script).resolve().parent)!r})",
             "import config"]
    lines += [f"config.{name} = {value!r}" for name, value in overrides.items()]
    lines += [f"spec = importlib.util.spec_from_file_location"
              f"('flowscript', {str(script)!r})",
              "module = importlib.util.module_from_spec(spec)",
              "spec.loader.exec_module(module)",
              f"print({expression})"]
    result = subprocess.run([sys.executable, "-c", "\n".join(lines)],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise AssertionError(f"override subprocess failed:\n{result.stderr}")
    return result.stdout.strip()


class ConfigValuesTest(unittest.TestCase):
    def test_constants(self):
        self.assertEqual(config.OUT_OF_ORDERNESS_MS, 70_000)
        self.assertEqual(config.WINDOW_MS, 300_000)
        self.assertEqual(config.EMA_J_SHORT, 38)
        self.assertEqual(config.EMA_J_LONG, 100)
        self.assertEqual(config.EMA_INIT, "prev-zero")
        self.assertEqual(config.ALLOWED_LATENESS_MS, 0)
        self.assertEqual(config.EMA_TOLERANCE, Decimal("1e-9"))
        self.assertEqual(config.MAX_REPORT_MISMATCHES, 100)
        self.assertEqual(config.SUMMARY_EXAMPLES, 10)
        self.assertEqual(config.DECIMAL_PREC, 50)
        self.assertEqual(config.EMA_PLACES, 9)
        self.assertEqual(config.BROKERS, "localhost:19092")
        self.assertEqual(config.TOPIC_EVENTS, "trade-events")
        self.assertEqual(config.TOPIC_DLQ, "trade-events-dlq")
        self.assertEqual(config.TOPIC_EMA_WINDOWS, "ema-windows")
        self.assertEqual(config.TOPIC_TRADE_ADVISORIES, "trade-advisories")
        self.assertEqual(config.FLINK_REST_URL, "http://localhost:8081")
        self.assertEqual(config.FLINK_JOB_NAME, "market-price-event-consumer")
        self.assertEqual(config.KAFKA_POLL_TIMEOUT_S, 1.0)
        self.assertEqual(config.WATERMARK_ATTEMPTS, 5)
        self.assertEqual(config.WATERMARK_BACKOFF_S, 0.5)
        self.assertEqual(config.CONSUMER_GROUP_ID, "full-flow-validation")
        self.assertEqual(config.DEFAULT_REPORT_PATH, "validation/out/flow-report.json")

    def test_config_imports_nothing_from_the_package(self):
        source = (Path(__file__).resolve().parent / "config.py").read_text(
            encoding="utf-8")
        for module in PACKAGE_MODULES:
            self.assertNotIn(f"import {module}", source)


class HelperSmokeTest(unittest.TestCase):
    def test_module_follows_override_reads_overrides(self):
        self.assertEqual(
            module_follows_override("config", {"WINDOW_MS": 299_999},
                                     "config.WINDOW_MS"),
            "299999")


class EmaReplayFollowsConfigTest(unittest.TestCase):
    def test_constants_and_init_default_follow_config(self):
        self.assertEqual(module_follows_override(
            "ema_replay", {"WINDOW_MS": 299_999}, "ema_replay.WindowMs"),
            "299999")
        self.assertEqual(module_follows_override(
            "ema_replay", {"OUT_OF_ORDERNESS_MS": 1},
            "ema_replay.OutOfOrdernessMs"), "1")
        self.assertEqual(module_follows_override(
            "ema_replay", {"EMA_J_SHORT": 37}, "ema_replay.JShort"), "37")
        self.assertEqual(module_follows_override(
            "ema_replay", {"EMA_J_LONG": 101},
            "ema_replay.JLong"), "101")
        self.assertEqual(module_follows_override(
            "ema_replay", {"DECIMAL_PREC": 40}, "ema_replay.DECIMAL_PREC"),
            "40")
        self.assertEqual(module_follows_override(
            "ema_replay", {"EMA_INIT": "first-close"},
            "inspect.signature(ema_replay.replay).parameters['ema_init'].default"),
            "first-close")


class GeneratorFollowsConfigTest(unittest.TestCase):
    def test_constants_follow_config(self):
        self.assertEqual(module_follows_override(
            "generate_validation_data", {"EMA_PLACES": 7},
            "generate_validation_data.EMA_PLACES"), "7")
        self.assertEqual(module_follows_override(
            "generate_validation_data", {"DECIMAL_PREC": 40},
            "generate_validation_data.DECIMAL_PREC"), "40")
        self.assertEqual(module_follows_override(
            "generate_validation_data", {"OUT_OF_ORDERNESS_MS": 123},
            "generate_validation_data.OUT_OF_ORDERNESS_MS"), "123")
        self.assertEqual(module_follows_override(
            "generate_validation_data", {"EMA_INIT": "zero-window"},
            "inspect.signature(generate_validation_data.run)"
            ".parameters['ema_init'].default"),
            "zero-window")


class KafkaTopicsFollowsConfigTest(unittest.TestCase):
    def test_consumer_knobs_follow_config(self):
        self.assertEqual(module_follows_override(
            "kafka_topics", {"KAFKA_POLL_TIMEOUT_S": 2.5},
            "kafka_topics.POLL_TIMEOUT_S"), "2.5")
        self.assertEqual(module_follows_override(
            "kafka_topics", {"WATERMARK_ATTEMPTS": 9},
            "kafka_topics.WATERMARK_ATTEMPTS"), "9")
        self.assertEqual(module_follows_override(
            "kafka_topics", {"WATERMARK_BACKOFF_S": 0.25},
            "kafka_topics.WATERMARK_BACKOFF_S"), "0.25")
        self.assertEqual(module_follows_override(
            "kafka_topics", {"CONSUMER_GROUP_ID": "other-group"},
            "kafka_topics.CONSUMER_GROUP_ID"), "other-group")


class FlowScriptsFollowsConfigTest(unittest.TestCase):
    RESET = Path(__file__).resolve().parent.parent / "scripts" / "reset-flow.py"
    SUBMIT = (Path(__file__).resolve().parent.parent / "scripts"
              / "submit-flink-job.py")

    def test_reset_flow_follows_config(self):
        self.assertEqual(script_follows_override(
            self.RESET, {"FLINK_REST_URL": "http://flink:1"}, "module.REST_URL"),
            "http://flink:1")
        self.assertEqual(script_follows_override(
            self.RESET, {"FLINK_JOB_NAME": "other-job"}, "module.JOB_NAME"),
            "other-job")
        self.assertEqual(script_follows_override(
            self.RESET, {"TOPIC_EVENTS": "evt"}, "module.TOPICS"),
            "('evt', 'trade-events-dlq', 'ema-windows', 'trade-advisories')")

    def test_submit_flink_job_follows_config(self):
        self.assertEqual(script_follows_override(
            self.SUBMIT, {"FLINK_REST_URL": "http://flink:2"},
            "module.REST_URL"),
            "http://flink:2")
        self.assertEqual(script_follows_override(
            self.SUBMIT, {"FLINK_JOB_NAME": "other-job"}, "module.JOB_NAME"),
            "other-job")


if __name__ == "__main__":
    unittest.main(verbosity=2)