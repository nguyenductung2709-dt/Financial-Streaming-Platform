# Financial-Streaming-Platform

## Development setup

After cloning, activate the git hooks once:

```bash
make setup-hooks
```

The pre-commit hook checks (never rewrites) files staged from the services:

- `services/data-producer`: `gofmt -l` and `go vet`
- `services/streaming-processor`: `scalafmtCheckAll` and `scalafmtSbtCheck` (slow: JVM startup)

If it rejects a commit, run `gofmt -w services/data-producer` or
`make format-streaming-processor`, re-stage the fixed files, and commit again.
If a toolchain is missing, the hook skips that check with a warning —
CI still enforces the full format and lint suite.

## CI checks

Pushes and pull requests run checks based on the changed paths:

| Changes | Checks |
| --- | --- |
| `services/data-producer/**` (except Markdown) | Producer formatting, lint, tests, and build |
| `services/streaming-processor/**` (except Markdown) | Processor formatting, lint, tests, and assembly verification |
| `schemas/**/*.proto` | Producer, processor, and Protobuf generation |
| `gen/go/**` | Producer and Protobuf generation |
| `gen/java/**` | Processor and Protobuf generation |
| Other files under `gen/**` | Protobuf generation |
| `go.mod`, `go.sum` | Producer and Protobuf generation |
| `Makefile` | Producer, processor, and Protobuf generation |
| `compose.yaml` | Infrastructure Compose configuration validation |
| A file in `.github/workflows/` | The workflow defined by that file |

Documentation-only changes and changes to local scripts do not trigger these
checks. Changes spanning multiple areas run all matching workflows.
The producer and infrastructure workflows also support manual runs with
`workflow_dispatch`.

Infrastructure CI runs `docker compose -f compose.yaml config --quiet` to validate
the Compose configuration without starting containers. This checks configuration
validity; it does not verify image availability or service startup. Run the same
command locally before pushing Compose changes.

GitHub leaves checks pending when an entire workflow is skipped by path filters.
If branch protection requires these checks, use job-level change detection instead
of workflow-level path filters before making them required. See
[GitHub's path filtering documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow#using-filters-to-target-specific-paths-for-pull-request-or-push-events).
