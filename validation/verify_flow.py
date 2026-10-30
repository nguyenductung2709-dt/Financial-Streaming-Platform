#!/usr/bin/env python3
"""End-to-end flow validation CLI: consume, shadow-replay, compare, report.

run() drains the three flow topics via kafka_topics.consume_all, replays
the price events through the shadow (keep-all/drop-model/at-risk), decodes
the processor's actual protobuf output, and classifies every window
against the shadow via compare.compare.  The JSON report
(compare.build_report's dict, nothing more) is written to the report path
and a console summary is printed: totals per class, at-risk totals, and
the first 10 unexplained windows.  main() is a thin argparse shell
returning exit code 0 iff the flow passed.  Spec:
docs/superpowers/plans/2026-10-30-full-flow-validation.md.
"""

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path

import bindings
import compare as cmp
import kafka_topics
import shadow as sh

SUMMARY_EXAMPLES = 10  # unexplained windows printed to the console


def _decode(msg_type, value: bytes):
    """One consumed record -> parsed protobuf message."""
    msg = msg_type()
    msg.ParseFromString(value)
    return msg


def _shadow_event(offset: int, value: bytes) -> sh.ShadowEvent:
    """One trade-events record -> ShadowEvent (order = its Kafka offset)."""
    msg = _decode(bindings.PriceEvent, value)
    tt = msg.trade_time
    return sh.ShadowEvent(msg.symbol,
                          tt.seconds * 1000 + tt.nanos // 1_000_000,
                          msg.last_price_micros, offset)


def _print_summary(shadow, result: cmp.CompareResult) -> None:
    """Console summary: totals per class, at-risk totals, examples."""
    print(f"events: {shadow.total_events}")
    print(f"windows: total {result.total_windows},"
          f" matched {result.matched},"
          f" lateness_explained {len(result.lateness_explained)},"
          f" lateness_related {len(result.lateness_related)},"
          f" unexplained {len(result.unexplained)}")
    print(f"at-risk: {len(shadow.at_risk_windows)} windows,"
          f" {shadow.at_risk_events} events")
    if result.unexplained:
        shown = min(len(result.unexplained), SUMMARY_EXAMPLES)
        print(f"unexplained (first {shown} of {len(result.unexplained)}):")
        for mismatch in result.unexplained[:SUMMARY_EXAMPLES]:
            print(f"  {mismatch.symbol}"
                  f" window_start_ms={mismatch.window_start_ms}:"
                  f" {'; '.join(mismatch.parts)}")
    print(f"passed: {result.passed}")


def run(brokers: str, topics: dict[str, str], report_path: Path,
        tolerance: Decimal, consume=None) -> cmp.CompareResult:
    """Validate one finished flow and write its JSON report.

    Consumes the three topics (events feed the shadow replay; ema and
    advisories are the actual), compares actual against the shadow at the
    given tolerance, writes compare.build_report's dict to report_path
    (creating parent directories), prints the console summary, and
    returns the comparison result.  `consume` is injectable for tests and
    defaults to kafka_topics.consume_all.
    """
    consume = consume or kafka_topics.consume_all
    event_records = consume(brokers, topics["events"])
    ema_records = consume(brokers, topics["ema"])
    advisory_records = consume(brokers, topics["advisories"])
    shadow = sh.shadow_replay(
        _shadow_event(offset, value) for offset, value in event_records)
    ema_actual = [cmp.decode_ema_window(_decode(bindings.EmaWindow, value))
                  for _, value in ema_records]
    advisories_actual = [cmp.decode_advisory(
        _decode(bindings.TradeAdvisory, value))
        for _, value in advisory_records]
    result = cmp.compare(ema_actual, advisories_actual, shadow, tolerance)
    report = cmp.build_report(shadow, result, tolerance, topics)
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2)
        report_file.write("\n")
    _print_summary(shadow, result)
    return result


def main(argv=None) -> int:
    """Parse the CLI args, run the validation, return 0 iff it passed."""
    parser = argparse.ArgumentParser(
        description="Validate a finished flow: shadow-replay trade-events"
                    " and compare against ema-windows/trade-advisories.")
    parser.add_argument("--brokers", default="localhost:19092",
                        help="Kafka bootstrap brokers")
    parser.add_argument("--events-topic", default="trade-events")
    parser.add_argument("--ema-topic", default="ema-windows")
    parser.add_argument("--advisory-topic", default="trade-advisories")
    parser.add_argument("--report", default="validation/out/flow-report.json",
                        help="path of the JSON report to write")
    parser.add_argument("--tolerance", type=Decimal, default=Decimal("1e-9"),
                        help="EMA comparison tolerance in price units")
    args = parser.parse_args(argv)
    topics = {"events": args.events_topic, "ema": args.ema_topic,
              "advisories": args.advisory_topic}
    result = run(args.brokers, topics, Path(args.report), args.tolerance)
    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
