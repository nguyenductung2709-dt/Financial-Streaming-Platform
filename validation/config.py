#!/usr/bin/env python3
"""Single editable home for the validation's tunable constants.

Every other module imports its constants from here and never
re-hardcodes a value; config_test.py pins both the values and the
wiring.  This module imports nothing from the package - the flow
scripts load it standalone.  Sections mirror the design:
docs/superpowers/specs/2026-10-31-validation-config-design.md.
"""

from decimal import Decimal

# --- Pipeline spec mirrors (change in pairs with the Scala side) ---
# StreamingProcessor.OutOfOrdernessAllowance: forBoundedOutOfOrderness(5 s); see
# "Out-of-orderness allowance" in services/streaming-processor/README.md.
OUT_OF_ORDERNESS_MS = 5_000
# StreamingProcessor: TumblingEventTimeWindows.of(Duration.ofMinutes(5)).
WINDOW_MS = 300_000
# EmaProcessFunction: next(..., period = 38) / next(..., period = 100).
EMA_J_SHORT = 38
EMA_J_LONG = 100
# Documented interpretation (PDF): the EMA of the previous window is 0;
# the generator's --ema-init lists the alternatives.
EMA_INIT = "prev-zero"
# StreamingProcessor sets no allowedLateness (0): windows die at the
# watermark.  is_late honors a nonzero value (mirrors a processor WITH
# allowedLateness: more events survive the drop rule).
ALLOWED_LATENESS_MS = 0
# data-producer event.go parseTradeTime: time.FixedZone("CEST", 2*60*60).  The
# generator's window_start strings are CEST wall clock; the topics carry UTC
# instants, so the reference join converts with this offset.
TRADE_TIME_UTC_OFFSET_HOURS = 2

# --- Validation-tool knobs ---
# EMA comparison tolerance in price units (= 1e-3 micros).
EMA_TOLERANCE = Decimal("1e-9")
# Per-mismatch-class cap of windows listed in the JSON report.
MAX_REPORT_MISMATCHES = 100
# Unexplained windows printed to the console.
SUMMARY_EXAMPLES = 10
# Decimal-context digits for the EMA replay (far tighter than double).
DECIMAL_PREC = 50
# Decimals emitted for EMA values in micros.
EMA_PLACES = 9
# Rows extracted to the generator's drops_pending_watermark_emission.csv
# (the summary count stays exact).
EMISSION_CASES_CAP = 1_000

# --- Infra/connection ---
BROKERS = "localhost:19092"        # compose.yaml redpanda external port
TOPIC_EVENTS = "trade-events"     # compose.yaml / producer / processor
TOPIC_DLQ = "trade-events-dlq"
TOPIC_EMA_WINDOWS = "ema-windows"
TOPIC_TRADE_ADVISORIES = "trade-advisories"
TOPIC_LATENCY_STAMPS = "latency-stamps"  # processor --latency-topic; measurement only
TOPIC_MATERIALIZE_STAMPS = "materialize-stamps"  # materializer stamps; measurement only
# Output topics created with the broker's append time as the record timestamp: the
# processor writes event time otherwise, and the latency collector reads the
# timestamp as the moment a result landed.
LOG_APPEND_TIME_TOPICS = (TOPIC_EMA_WINDOWS, TOPIC_TRADE_ADVISORIES)
LOG_APPEND_TIME_CONFIG = "message.timestamp.type=LogAppendTime"
FLINK_REST_URL = "http://localhost:8081"  # compose.yaml jobmanager REST
FLINK_JOB_NAME = "market-price-event-consumer"
KAFKA_POLL_TIMEOUT_S = 1.0        # kafka_topics.consume_all poll
WATERMARK_ATTEMPTS = 5            # transient watermark-query retries
WATERMARK_BACKOFF_S = 0.5
CONSUMER_GROUP_ID = "full-flow-validation"  # placeholder; assignment is manual
DEFAULT_REPORT_PATH = "validation/out/flow-report.json"
# scripts/drain-materializer.py: run the materializer until its group has
# committed every record of the result topics, then stop it.
DRAIN_TIMEOUT_S = 600.0           # longest wait for the materializer to catch up
DRAIN_POLL_S = 1.0                # committed-offset check interval
MATERIALIZER_STOP_TIMEOUT_S = 30.0  # SIGTERM until exit, before SIGKILL
OFFSET_QUERY_TIMEOUT_S = 10.0     # one watermark/committed-offset query
# scripts/reset-flow-data.py: a recreated topic cannot start until the old log of
# the same name is gone (~30 s for a full day's 12M events), and `rpk topic
# create` returns before that.
TOPIC_READY_TIMEOUT_S = 120.0     # longest wait for the recreated topics to be ready
TOPIC_READY_POLL_S = 1.0          # describe interval while waiting
