#!/usr/bin/env python3
"""End-to-end flow validation CLI: consume, shadow-replay, compare, report.

run() drains the three flow topics via kafka_topics.consume_all, replays
the price events through the shadow (keep-all/drop-model/at-risk), decodes
the processor's actual protobuf output, and classifies every window
against the shadow via compare.compare.  With --reference-dir the shadow is
also checked against the CSV generator's offline output (reference.py),
which isolates the producer from the processor; the reference is loaded
before any topic is read so an unusable one fails fast.  With --db-url
the materializer's PostgreSQL tables are also checked against the topic
records (db_check.py); the connection is opened first for the same reason.
The JSON report (compare.build_report's dict, plus "reference" and
"database" blocks when those checks ran) is written to the report path and
a console summary is printed: totals per class, at-risk totals, the
reference and database results, and the first 10 mismatched windows of
each kind.  main() is a thin argparse shell returning exit code 0 iff the
flow passed (processor comparison and every optional check).  Spec:
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
import db_check
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
    """The processor comparison, the reference and database comparisons
    (None without --reference-dir / --db-url), and the verdict: every
    comparison that ran passed."""

    compare: cmp.CompareResult
    reference: ref.ReferenceResult | None
    database: db_check.DbResult | None
    passed: bool


def _print_examples(label: str, mismatches: list) -> None:
    """Up to SUMMARY_EXAMPLES mismatched windows under a label (database
    mismatches also name their table)."""
    shown = min(len(mismatches), SUMMARY_EXAMPLES)
    print(f"{label} (first {shown} of {len(mismatches)}):")
    for mismatch in mismatches[:SUMMARY_EXAMPLES]:
        table = f"{mismatch.table} " if isinstance(mismatch, db_check.DbMismatch) else ""
        print(f"  {table}{mismatch.symbol}"
              f" window_start_ms={mismatch.window_start_ms}:"
              f" {'; '.join(mismatch.parts)}")


def _print_summary(shadow, result: cmp.CompareResult,
                   reference: ref.ReferenceResult | None,
                   database: db_check.DbResult | None, passed: bool) -> None:
    """Console summary: totals per class, at-risk totals, reference, database, examples."""
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
    if database is not None:
        print(f"database: {database.matched_windows} of {database.expected_windows} windows,"
              f" {database.matched_advisories} of {database.expected_advisories}"
              f" advisories match, {len(database.mismatches)} mismatched")
    if result.unexplained:
        _print_examples("unexplained", result.unexplained)
    if reference is not None and reference.mismatches:
        _print_examples("reference mismatches", reference.mismatches)
    if database is not None and database.mismatches:
        _print_examples("database mismatches", database.mismatches)
    print(f"passed: {passed}")


def run(brokers: str, topics: dict[str, str], report_path: Path,
        tolerance: Decimal, consume=None, reference_dir=None,
        db_url=None, db_connect=None) -> FlowResult:
    """Validate one finished flow and write its JSON report.

    Consumes the three topics (events feed the shadow replay; ema and
    advisories are the actual), compares actual against the shadow at the
    given tolerance and, when reference_dir is given, the shadow against
    the generator's output there.  When db_url is given the materializer's
    PostgreSQL tables are checked against the same topic records (the
    flow must be drained into the database first); the connection is
    opened before any topic is read and closed on every path.  Writes the
    report dict to report_path (creating parent directories), prints the
    console summary, and returns every result with the overall verdict.
    `consume` and `db_connect` are injectable for tests and default to
    kafka_topics.consume_all and db_check.connect.
    """
    consume = consume or kafka_topics.consume_all
    reference = ref.load_reference(reference_dir) if reference_dir is not None else None
    conn = (db_connect or db_check.connect)(db_url) if db_url is not None else None
    try:
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
        database_result = (db_check.compare_db(
            db_check.expected_state(ema_records, advisory_records), db_check.read_db(conn))
            if conn is not None else None)
    finally:
        if conn is not None:
            conn.close()
    passed = (result.passed
              and (reference_result is None or reference_result.passed)
              and (database_result is None or database_result.passed))
    report = cmp.build_report(shadow, result, tolerance, topics)
    if reference_result is not None:
        report["reference"] = ref.reference_report(reference_result, reference_dir)
    if database_result is not None:
        report["database"] = db_check.db_report(database_result)
    report["passed"] = passed
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2)
        report_file.write("\n")
    _print_summary(shadow, result, reference_result, database_result, passed)
    return FlowResult(result, reference_result, database_result, passed)


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
    parser.add_argument("--db-url", default=None,
                        help="PostgreSQL URL of the materializer's database to also"
                             " check against the topics (drain the materializer first)")
    return parser


def main(argv=None) -> int:
    """Parse the CLI args, run the validation, return 0 iff it passed."""
    args = build_parser().parse_args(argv)
    topics = {"events": args.events_topic, "ema": args.ema_topic,
              "advisories": args.advisory_topic}
    result = run(args.brokers, topics, Path(args.report), args.tolerance,
                 reference_dir=args.reference_dir, db_url=args.db_url)
    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
