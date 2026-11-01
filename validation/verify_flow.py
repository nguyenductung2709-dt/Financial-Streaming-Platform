#!/usr/bin/env python3
"""End-to-end flow validation CLI: consume, shadow-replay, compare, report.

run() drains the three flow topics via kafka_topics.consume_all, replays
the price events through the shadow (keep-all/drop-model/at-risk), decodes
the processor's actual protobuf output, and classifies every window
against the shadow via compare.compare.  With --reference-dir the shadow is
also checked against the CSV generator's offline output (reference.py),
which isolates the producer from the processor; the reference is loaded
before any topic is read so an unusable one fails fast.  The JSON report
(compare.build_report's dict, plus a "reference" block when a reference was
given) is written to the report path and a console summary is printed:
totals per class, at-risk totals, the reference result, and the first 10
unexplained windows.  main() is a thin argparse shell returning exit code
0 iff the flow passed (processor comparison and reference, if any).  Spec:
docs/superpowers/plans/2026-10-30-full-flow-validation.md.
"""

import argparse
import json
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import NamedTuple

import config
import bindings
import compare as cmp
import kafka_topics
import reference as ref
import shadow as sh

SUMMARY_EXAMPLES = config.SUMMARY_EXAMPLES  # unexplained windows printed to the console


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


class FlowResult(NamedTuple):
    """The processor comparison, the reference comparison (None without
    --reference-dir), and the verdict: both comparisons passed."""

    compare: cmp.CompareResult
    reference: ref.ReferenceResult | None
    passed: bool


def _print_examples(label: str, mismatches: list[cmp.Mismatch]) -> None:
    """Up to SUMMARY_EXAMPLES mismatched windows under a label."""
    shown = min(len(mismatches), SUMMARY_EXAMPLES)
    print(f"{label} (first {shown} of {len(mismatches)}):")
    for mismatch in mismatches[:SUMMARY_EXAMPLES]:
        print(f"  {mismatch.symbol}"
              f" window_start_ms={mismatch.window_start_ms}:"
              f" {'; '.join(mismatch.parts)}")


def _print_summary(shadow, result: cmp.CompareResult,
                   reference: ref.ReferenceResult | None, passed: bool) -> None:
    """Console summary: totals per class, at-risk totals, reference, examples."""
    print(f"events: {shadow.total_events}")
    print(f"windows: total {result.total_windows},"
          f" matched {result.matched},"
          f" lateness_explained {len(result.lateness_explained)},"
          f" lateness_related {len(result.lateness_related)},"
          f" unexplained {len(result.unexplained)}")
    print(f"at-risk: {len(shadow.at_risk_windows)} windows,"
          f" {shadow.at_risk_events} events")
    if reference is not None:
        print(f"reference ({reference.side}): {reference.matched} of"
              f" {reference.reference_windows} windows match,"
              f" {len(reference.mismatches)} mismatched")
    if result.unexplained:
        _print_examples("unexplained", result.unexplained)
    if reference is not None and reference.mismatches:
        _print_examples("reference mismatches", reference.mismatches)
    print(f"passed: {passed}")


def run(brokers: str, topics: dict[str, str], report_path: Path,
        tolerance: Decimal, consume=None, reference_dir=None) -> FlowResult:
    """Validate one finished flow and write its JSON report.

    Consumes the three topics (events feed the shadow replay; ema and
    advisories are the actual), compares actual against the shadow at the
    given tolerance and, when reference_dir is given, the shadow against
    the generator's output there; writes the report dict to report_path
    (creating parent directories), prints the console summary, and
    returns both results with the overall verdict.  `consume` is injectable
    for tests and defaults to kafka_topics.consume_all.
    """
    consume = consume or kafka_topics.consume_all
    reference = ref.load_reference(reference_dir) if reference_dir is not None else None
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
    reference_result = (ref.compare_reference(shadow, reference)
                        if reference is not None else None)
    passed = result.passed and (reference_result is None or reference_result.passed)
    report = cmp.build_report(shadow, result, tolerance, topics)
    if reference_result is not None:
        report["reference"] = ref.reference_report(reference_result, reference_dir)
    report["passed"] = passed
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2)
        report_file.write("\n")
    _print_summary(shadow, result, reference_result, passed)
    return FlowResult(result, reference_result, passed)


def _tolerance_arg(text: str) -> Decimal:
    """argparse type for --tolerance: raise ValueError (which argparse
    turns into a clean usage error) instead of InvalidOperation."""
    try:
        return Decimal(text)
    except InvalidOperation:
        raise ValueError(f"invalid tolerance: {text!r}")


def build_parser() -> argparse.ArgumentParser:
    """The flow-validation CLI, defaults sourced from config."""
    parser = argparse.ArgumentParser(
        description="Validate a finished flow: shadow-replay trade-events"
                    " and compare against ema-windows/trade-advisories.")
    parser.add_argument("--brokers", default=config.BROKERS,
                        help="Kafka bootstrap brokers")
    parser.add_argument("--events-topic", default=config.TOPIC_EVENTS)
    parser.add_argument("--ema-topic", default=config.TOPIC_EMA_WINDOWS)
    parser.add_argument("--advisory-topic", default=config.TOPIC_TRADE_ADVISORIES)
    parser.add_argument("--report", default=config.DEFAULT_REPORT_PATH,
                        help="path of the JSON report to write")
    parser.add_argument("--tolerance", type=_tolerance_arg,
                        default=config.EMA_TOLERANCE,
                        help="EMA comparison tolerance in price units")
    parser.add_argument("--reference-dir", default=None, type=Path,
                        help="generate_validation_data.py output directory to"
                             " also check the shadow against (needs the same CSV)")
    return parser


def main(argv=None) -> int:
    """Parse the CLI args, run the validation, return 0 iff it passed."""
    args = build_parser().parse_args(argv)
    topics = {"events": args.events_topic, "ema": args.ema_topic,
              "advisories": args.advisory_topic}
    result = run(args.brokers, topics, Path(args.report), args.tolerance,
                 reference_dir=args.reference_dir)
    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
