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