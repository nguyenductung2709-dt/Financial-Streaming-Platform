# Data producer

Reads the trading CSV and publishes raw Protobuf events to Redpanda. Run from
the repository root with `make run-data-producer ARGS="..."`.

## Optional checkpointing

```sh
# First run: checkpoint progress and stop after 100,000 acknowledged price events.
make run-data-producer ARGS="-checkpointPath=checkpoints/friday.json -maxEvents=100000"

# Continue the same CSV and destination; publish up to 100,000 additional events.
make run-data-producer ARGS="-checkpointPath=checkpoints/friday.json -resume -maxEvents=100000"
```

`-checkpointPath` enables checkpointing; without it, behavior is unchanged.
`-checkpointInterval` defaults to `1s`. Saves occur while rows are completed,
plus a final save on normal completion, event limit, cancellation, or a handled
error. `-resume` requires an existing valid checkpoint. A fresh run refuses to
overwrite an existing checkpoint; use a new path for a new experiment.

Progress advances only after an acknowledged price event, an acknowledged DLQ
record, or an intentional skip (empty/zero price). Failed sends and failed DLQ
sends do not advance progress. Summary counters and `-maxEvents` apply to the
current invocation; rows bypassed during resume are not counted as new skips.

The versioned JSON file contains the CSV SHA-256/size, broker addresses, topic,
DLQ configuration, and last completed physical CSV record-start line. Every
checkpointed run hashes the entire CSV before processing. Resume rereads and
validates the header, then scans past completed rows; it does not seek directly.
Keep the input immutable during a run. Matching contents can be moved to a new
path. Changed contents or destination settings are rejected.

Checkpoint files are written through a synced temporary file, atomic rename,
and directory sync. A Unix advisory lock prevents two cooperating producers
from writing the same checkpoint simultaneously; the OS releases the lock after
a crash. The `.lock` file may remain and does not itself mean the lock is held.
This locking implementation targets Linux and macOS. Checkpoint errors fail the
run instead of silently disabling progress tracking.

## Delivery and recovery limits

Checkpointing provides resumable at-least-once publishing. If the producer dies
after acknowledgment but before the next save, some records will be replayed.
Kafka producer idempotence does not deduplicate an application replay after a
restart; downstream code must not assume exactly-once input delivery.

Producer progress is separate from Flink checkpoints: this file tracks CSV
publishing, not EMA/crossover state. Resume is explicit; the producer process is
not automatically restarted.

Do not resume after clearing/recreating topics, resetting the broker, or running
`make reset-flow`: the saved CSV position assumes the previously acknowledged
records remain available. A matching broker address/topic name does not prove
it is the original broker or topic. Start those experiments with a new checkpoint
path. `/checkpoints/` is ignored by Git; keep these files on persistent storage
when running the producer in a container.

The current synchronous publishing loop completes rows in order. A future async
implementation must advance only through contiguous completed rows; it must not
checkpoint the highest acknowledged row while an earlier row is still pending.

## Verify producer crash and resume

With Redpanda and Flink running and at least one free task slot:

```sh
make verify-producer-recovery
```

This builds both services, generates small CSV fixtures, kills only its own
producer process with SIGKILL, and resumes from its last saved checkpoint.
It checks replay while windows are open, after they close, and together with a
Flink checkpoint restore. Every case must match an independent EMA/crossover
reference and contain the expected duplicate input records.

Existing jobs and application topics are left running. Test jobs are canceled
afterward; isolated `verify-producer-*` topics remain for inspection. Fixtures,
command logs, decoded results, and `report.json` are saved under the ignored
`bin/producer-recovery-<timestamp>/` directory. This is a correctness test, not a
throughput benchmark. See [the recovery results](../../documents/flink-recovery-verification.md).
