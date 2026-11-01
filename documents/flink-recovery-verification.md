# Flink recovery verification

Verified locally on 30 October 2026 with Flink 2.2.1, the existing filesystem
checkpoint storage, and at-least-once Redpanda output sinks.

## Fixture and checks

- Two distinct symbols, each with 90 five-minute windows and two trades per window.
- Prices rise from zero-initialized EMAs, fall sharply, then rise again.
- One additional timestamp-advancing event finalizes the 180 target windows.
- Independent Python rational arithmetic with DECIMAL128 rounding predicts
  four BUY and two SELL advisories, including exact current and previous EMAs.
- Checkpoints are taken after 162 inputs: 78 windows are finalized while
  windows 39 and 40 for both symbols remain open. Another 199 inputs follow.
- Every decoded Protobuf field and Kafka symbol key is checked. Duplicate
  output keys are counted and must have identical payloads.

## Results

| Scenario | Unique EMA windows | Unique advisories | Mismatches | Duplicate records |
| --- | ---: | ---: | ---: | ---: |
| baseline | 180 | 6 | 0 | 0 |
| worker-restart | 180 | 6 | 0 | 0 |
| manual-restore | 180 | 6 | 0 | 0 |

Automatic TaskManager restart recovery took approximately 12.3 seconds,
including the worker restart and the configured restart delay.

The manual test canceled the job, published the remaining inputs, then restored
the latest retained completed checkpoint. It deliberately requested
`--starting-offsets latest`; all pending inputs were still consumed from the
restored source positions. This verifies source offsets are restored together
with open windows, EMA state, and crossover state.

All verification jobs were canceled afterward. Existing application topics were
untouched; only isolated `verify-recovery-*` topics were used.

## Limits

This is a focused recovery correctness test, not a throughput benchmark or
a full-week endurance test. Inputs were split around a completed checkpoint;
in-flight failure scenarios and broker failures need separate experiments.
Zero duplicates in these runs does not change the at-least-once guarantee.
The base configuration does not provide JobManager HA. The optional HA override
is verified below. These earlier experiments did not exercise producer resume;
that is covered separately below. Exactly-once Kafka transactions and state
migration across incompatible job versions are not implemented.

## Optional ZooKeeper HA verification

Verified on 30 October 2026 using `compose.yaml` plus `compose.ha.yaml`, one
JobManager, one TaskManager, and persistent ZooKeeper 3.9.3. The same independent
fixture and output comparisons were used.

| Scenario | Recovery time | EMA windows | Advisories | Mismatches |
| --- | ---: | ---: | ---: | ---: |
| Uninterrupted HA baseline | — | 180 | 6 | 0 |
| JobManager JVM exit | 6.4 s | 180 | 6 | 0 |
| JobManager and TaskManager JVM exits | 6.4 s | 180 | 6 | 0 |

Failure injection sent SIGTERM to the JVM as PID 1 inside the selected containers.
Docker restart counters increased, and Flink resumed the same job ID from its
completed checkpoint without any resubmission command. Remaining input records
were published while recovery was underway. No duplicate outputs occurred in
these runs; the output guarantee remains at-least-once.

After intentional cancellation, the JobManager JVM was terminated again. The
canceled jobs stayed canceled. All test jobs were canceled after verification;
application topic records were left untouched. The local cluster was left in HA
mode with no active processor job.

`docker kill` was not used for the successful tests: on this Docker setup it was
treated as an intentional container stop and suppressed automatic restart. A
JVM process exit exercises the restart policy without that operator-stop flag.

This verifies process-exit recovery, not hard SIGKILL/OOM behavior, loss of the
ZooKeeper server, disk failure, or multi-machine failover. The local single-server
ZooKeeper deployment is not a redundant coordination quorum.

## Producer crash, resume, and combined restore

Verified on 31 October 2026 with `make verify-producer-recovery`, using the same
180-window/six-advisory reference described above. This command generates its
own CSV fixtures and uses the actual Go producer, Redpanda, and Flink pipeline.
The open-window case adds more trades inside the first window so a producer
checkpoint can be saved before any window closes.

Each crash case pauses its own producer, confirms acknowledged input is ahead
of its saved progress, and sends SIGKILL. Resume therefore must publish some
identical rows again. The combined case also cancels its test Flink job after
a completed checkpoint, queues the resumed producer's input, and restores that
checkpoint. It does not restart the shared cluster or existing application job.

| Scenario | Input records | Unique inputs | Replayed inputs | EMA windows | Advisories | Mismatches |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Uninterrupted baseline | 361 | 361 | 0 | 180 | 6 | 0 |
| Producer crash with open windows | 853 | 757 | 96 | 180 | 6 | 0 |
| Producer crash after windows close | 412 | 361 | 51 | 180 | 6 | 0 |
| Producer replay plus Flink restore | 430 | 361 | 69 | 180 | 6 | 0 |

There were no missing, extra, conflicting duplicate, or numerically incorrect
outputs. No output duplicates occurred in this run. The test permits identical
output duplicates and reports them, since the sinks remain at-least-once.
Replay counts depend on crash timing and are not fixed assertions.

### Bug found and fixed

Before the fix, the combined case produced 134 incorrect unique EMA windows
and incorrect advisories. Watermark progress can restart after restore, letting
replayed input recreate windows already applied to EMA state. The EMA operator
now checkpoints a per-symbol last applied window end and ignores completed
windows at or before that boundary. Unit tests verify this boundary survives
restore and keeps symbol state independent.

Legacy checkpoints containing EMA values without `ema-last-window-end-v1` fail
explicitly when that symbol is processed. Use a fresh replay or migrate those
checkpoints; silently restoring them cannot provide this protection.

The successful run's fixtures, command logs, decoded records, and report are in
the local ignored `bin/producer-recovery-20261031-133149/` directory. Test jobs
were canceled; isolated `verify-producer-*` topics remain for inspection. The
pre-existing application job stayed running.

These controlled tests cover producer SIGKILL and checkpoint restore together.
They do not prove end-to-end exactly-once delivery, broker failure recovery,
or correctness under every possible late-event pattern or failure timing.
