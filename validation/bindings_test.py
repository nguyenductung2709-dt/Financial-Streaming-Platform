#!/usr/bin/env python3
"""Tests for bindings.py.

Real protobuf round-trips through the single import point for the
gen/python bindings: serialize a message, parse it back, and assert
every field survives, including a uint64 beyond the signed 64-bit
range and Timestamp seconds/nanos.
"""

import sys
import unittest
from pathlib import Path

from google.protobuf.timestamp_pb2 import Timestamp

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bindings  # noqa: E402


class PriceEventRoundTripTest(unittest.TestCase):
    def test_fields_survive_serialization(self):
        trade_time = Timestamp(seconds=1636696800, nanos=123000000)
        event = bindings.PriceEvent(
            symbol="AAA.NL",
            security_type=bindings.SECURITY_TYPE_EQUITY,
            last_price_micros=2**63,  # uint64 beyond the signed 64-bit range
            trade_time=trade_time,
            source_row=42,
        )
        parsed = bindings.PriceEvent.FromString(event.SerializeToString())
        self.assertEqual(parsed.symbol, "AAA.NL")
        self.assertEqual(parsed.security_type, bindings.SECURITY_TYPE_EQUITY)
        self.assertEqual(parsed.last_price_micros, 2**63)
        self.assertEqual(parsed.trade_time.seconds, 1636696800)
        self.assertEqual(parsed.trade_time.nanos, 123000000)
        self.assertEqual(parsed.source_row, 42)


class EmaWindowRoundTripTest(unittest.TestCase):
    def test_fields_survive_serialization(self):
        window_start = Timestamp(seconds=1636696800, nanos=999999999)
        window = bindings.EmaWindow(
            symbol="AAA.NL",
            window_start=window_start,
            closing_price_micros=57_117_900,
            ema38="123.456789",
            ema100="122.987654",
        )
        parsed = bindings.EmaWindow.FromString(window.SerializeToString())
        self.assertEqual(parsed.symbol, "AAA.NL")
        self.assertEqual(parsed.window_start.seconds, 1636696800)
        self.assertEqual(parsed.window_start.nanos, 999999999)
        self.assertEqual(parsed.closing_price_micros, 57_117_900)
        self.assertEqual(parsed.ema38, "123.456789")
        self.assertEqual(parsed.ema100, "122.987654")


class TradeAdvisoryRoundTripTest(unittest.TestCase):
    def test_embedded_window_and_action_survive_serialization(self):
        window = bindings.EmaWindow(symbol="AAA.NL", ema38="10.000000000")
        advisory = bindings.TradeAdvisory(
            ema_window=window,
            action=bindings.ADVISORY_ACTION_SELL,
            previous_ema38="9.000000000",
            previous_ema100="9.500000000",
        )
        parsed = bindings.TradeAdvisory.FromString(advisory.SerializeToString())
        self.assertEqual(parsed.ema_window.symbol, "AAA.NL")
        self.assertEqual(parsed.ema_window.ema38, "10.000000000")
        self.assertEqual(parsed.action, bindings.ADVISORY_ACTION_SELL)
        self.assertEqual(parsed.previous_ema38, "9.000000000")
        self.assertEqual(parsed.previous_ema100, "9.500000000")


class AdvisoryActionConstantsTest(unittest.TestCase):
    def test_values_match_proto_schema(self):
        self.assertEqual(bindings.ADVISORY_ACTION_BUY, 1)
        self.assertEqual(bindings.ADVISORY_ACTION_SELL, 2)


class StageStampTest(unittest.TestCase):
    def test_fields_survive_serialization(self):
        stamp = bindings.StageStamp(
            symbol="IUSAC.FR",
            window_end=Timestamp(seconds=1636693500),
            stage=bindings.STAGE_CROSSOVER_EVALUATED,
            at=Timestamp(seconds=1636693501, nanos=250_000_000),
            advisory_emitted=True,
        )
        parsed = bindings.StageStamp.FromString(stamp.SerializeToString())
        self.assertEqual(parsed, stamp)
        self.assertEqual(parsed.symbol, "IUSAC.FR")
        self.assertEqual(parsed.window_end.seconds, 1636693500)
        self.assertEqual(parsed.stage, bindings.STAGE_CROSSOVER_EVALUATED)
        self.assertEqual(parsed.at.nanos, 250_000_000)
        self.assertTrue(parsed.advisory_emitted)

    def test_values_match_proto_schema(self):
        self.assertEqual(bindings.STAGE_WINDOW_FIRED, 1)
        self.assertEqual(bindings.STAGE_EMA_COMPUTED, 2)
        self.assertEqual(bindings.STAGE_CROSSOVER_EVALUATED, 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
