#!/usr/bin/env bash
set -euo pipefail

# Skip creation when the topic already exists; rpk exits non-zero with
# TOPIC_ALREADY_EXISTS, which must not make rerunning this script fail.
docker exec redpanda-0 rpk topic describe price-events.v1 >/dev/null 2>&1 \
  || docker exec redpanda-0 rpk topic create price-events.v1 -p 8
docker exec redpanda-0 rpk topic describe price-events-dlq.v1 >/dev/null 2>&1 \
  || docker exec redpanda-0 rpk topic create price-events-dlq.v1 -p 1
