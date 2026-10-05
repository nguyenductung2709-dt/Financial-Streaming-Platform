#!/usr/bin/env bash
docker exec redpanda-0 rpk topic create price-events.v1 -p 8 
docker exec redpanda-0 rpk topic create price-events-dlq.v1 -p 1