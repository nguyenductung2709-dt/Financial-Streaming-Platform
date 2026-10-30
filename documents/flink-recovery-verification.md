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
is verified below. Exactly-once Kafka transactions, producer resume, and state
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
