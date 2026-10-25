# Financial-Streaming-Platform

## Development setup

After cloning, activate the git hooks once:

```bash
make setup-hooks
```

The pre-commit hook formats staged service files:

- `services/data-producer`: `gofmt -w` and `go vet`
- `services/streaming-processor`: `scalafmtAll` and `scalafmtSbt` (slow: JVM startup)

Formatting runs on a snapshot of the staged files. Formatting changes are copied
back and re-staged only for fully staged files. If a file needs formatting and
also has unstaged changes, the hook stops without changing your files or staging
area. Run `make formatall`, review, and stage the intended changes before retrying.
Already formatted staged files can still have unstaged edits.

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
