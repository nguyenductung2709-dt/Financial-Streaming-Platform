# CI

Each GitHub Actions workflow only runs when files it cares about change, and changes
to Markdown files alone don't trigger anything. The exact path filters are at the top
of each file in `.github/workflows/`. Roughly:

- Data producer CI and Materializer CI run on changes to their service, `gen/go/`, the
  schemas, `go.mod`, the `Makefile` or the pre-commit hook. They check formatting and
  `go vet`, run the tests, and build the Docker image. The materializer workflow also
  runs the race tests and the integration tests against a PostgreSQL 18 service.
- Streaming processor CI runs on changes to the processor, `gen/java/`, the schemas or
  the `Makefile`. It checks formatting and lint, runs the tests, and builds the JAR.
- Dashboard CI runs on changes to the backend, the frontend, the migrations, the flow
  scripts or the `Makefile`. It runs the backend tests against PostgreSQL, the frontend
  tests, and the flow lifecycle tests. It doesn't start the full stack or open a
  browser.
- Monitoring CI runs on changes to `compose.monitoring.yaml`, `monitoring/`, the
  measurement scripts or `gen/python/`. It validates the Compose files and the Grafana
  dashboards and runs the script tests.
- Infrastructure CI runs `docker compose --profile '*' -f compose.yaml config --quiet`
  when `compose.yaml` or `.dockerignore` changes. Run the same command before pushing a
  Compose change.
- Check generated Protobuf code regenerates `gen/` whenever a schema changes, and fails
  if the committed files differ.

A workflow also runs when its own file changes, and all of them except the Protobuf
check can be started by hand.

One thing to watch out for: when path filters skip a whole workflow, GitHub leaves its
checks pending. So if you want to make these checks required in branch protection,
move the path detection into the jobs first.
