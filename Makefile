.DEFAULT_GOAL := help
FLINK_ARGS ?= --starting-offsets earliest
PRODUCER_ARGS ?= -rate=1000
FLINK_START_TIMEOUT ?= 120
RATE ?= 0
BOUNDED_TIMEOUT ?= 3600
MAX_EVENTS ?=
REFERENCE_DIR ?=
DEFAULT_REFERENCE_DIR := validation/out/reference
# CSV is a whitespace-separated list of files/directories. Quote each input for
# the shell; explicit list order is significant in both native readers.
shell-quote = '$(subst ','"'"',$(1))'
REFERENCE_CSV_ARGS = $(foreach path,$(CSV),--csv $(call shell-quote,$(path)))
PRODUCER_CSV_ARGS = $(foreach path,$(CSV),$(call shell-quote,-csvPath=$(path)))
# 1 also checks the materializer's PostgreSQL tables against the topics (needs .env);
# validate-flow runs drain-materializer first so the database is complete.
DB_CHECK ?= 1
BACKEND_VENV ?= .venv
BACKEND_PYTHON := $(abspath $(BACKEND_VENV))/bin/python
BACKEND_HOST ?= 127.0.0.1
BACKEND_PORT ?= 8000
COMPOSE_MONITORING = docker compose -f compose.yaml -f compose.monitoring.yaml
MONITORING_SERVICES = docker-stats-exporter process-exporter prometheus grafana
OUT ?= bin/metrics/$(shell date +%Y%m%d-%H%M%S)

# Load the same settings for the standalone materializer and the complete flow.
define materializer-env
set -e; \
if [ ! -f .env ]; then echo 'Create .env first: cp .env.example .env' >&2; exit 1; fi; \
materializer_database_url="$${MATERIALIZER_DATABASE_URL:-}"; \
materializer_brokers="$${MATERIALIZER_BROKERS:-}"; \
materializer_group_id="$${MATERIALIZER_GROUP_ID:-}"; \
. ./.env; \
export MATERIALIZER_DATABASE_URL="$${materializer_database_url:-$${MATERIALIZER_DATABASE_URL-}}"; \
export MATERIALIZER_BROKERS="$${materializer_brokers:-$${MATERIALIZER_BROKERS-}}"; \
export MATERIALIZER_GROUP_ID="$${materializer_group_id:-$${MATERIALIZER_GROUP_ID-}}";
endef

.PHONY: build-data-producer test-data-producer run-data-producer generate-protobuf build-streaming-processor test-streaming-processor format-streaming-processor lint-streaming-processor setup-hooks run-streaming-processor run-flow reset-flow infra-up run-bounded-processor generate-reference drain-materializer compare-flow validate-flow test-validation

.PHONY: help \
	build-data-producer test-data-producer run-data-producer \
	build-materializer test-materializer test-materializer-integration run-materializer \
	build-streaming-processor test-streaming-processor \
	formatall format-data-producer format-materializer format-streaming-processor lint-streaming-processor \
	generate-protobuf generate-python-protobuf setup-hooks run-postgres postgres-shell \
	monitoring-up monitoring-down collect-metrics

.PHONY: setup-backend migrate-backend run-backend stop-flow

setup-backend: $(BACKEND_VENV)/.backend-requirements ## Prepare the backend virtual environment and install its dependencies

$(BACKEND_VENV)/.backend-requirements: services/backend/requirements.txt
	@test -x $(call shell-quote,$(BACKEND_PYTHON)) || python3 -m venv $(call shell-quote,$(BACKEND_VENV))
	$(call shell-quote,$(BACKEND_PYTHON)) -m pip install -r services/backend/requirements.txt
	@touch $(call shell-quote,$@)

migrate-backend: setup-backend ## Apply missing database migrations safely; PostgreSQL must be running
	@$(materializer-env) exec $(call shell-quote,$(BACKEND_PYTHON)) services/backend/migrate.py

run-backend: migrate-backend ## Apply missing migrations and serve the dashboard on port 8000
	@$(materializer-env) HOST=$(call shell-quote,$(BACKEND_HOST)) PORT=$(call shell-quote,$(BACKEND_PORT)) exec $(call shell-quote,$(BACKEND_PYTHON)) services/backend/main.py

help: ## Show command usage; also displayed by plain make
	@printf 'Run from the repository root: make <target>\n\nCommands:\n\n'
	@awk 'BEGIN { FS = ":.*## " } /^[a-zA-Z0-9_-]+:.*## / { printf "  make %s\n    %s\n\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf '%s\n' \
		'Producer arguments (pass flags through ARGS):' \
		'  make run-data-producer ARGS="-help"' \
		'  make run-data-producer ARGS="-dlqEnabled -maxEvents=100"' \
		'  make run-data-producer ARGS="-csvPath=/path/to/trades.csv -brokers=localhost:19092"' \
		'' \
		'Requirements:' \
		'  Producer: Go; race tests also require cgo and a C compiler.' \
		'  Processor: Java 17 and sbt.' \
		'  Dashboard: Python 3.12; first start installs backend dependencies into .venv.' \
		'  Code generation: protoc and protoc-gen-go (see schemas/market/v1/README.md).' \
		'  Publishing: a running broker, existing topics, and an input CSV.'

run-postgres: ## Start PostgreSQL and wait until it accepts connections
	docker compose up -d --wait --wait-timeout 60 postgres

postgres-shell: ## Open psql in the running PostgreSQL container; exit with \q
	docker compose exec postgres sh -c 'exec psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

monitoring-up: ## Start Prometheus (:9090), Grafana (:3000) and the container/host resource exporters
	$(COMPOSE_MONITORING) up -d --wait --wait-timeout 120 $(MONITORING_SERVICES)

monitoring-down: ## Stop the monitoring containers; recorded metrics are kept in the prometheus-data volume
	$(COMPOSE_MONITORING) stop $(MONITORING_SERVICES)

collect-metrics: ## Export CPU, memory, network and block I/O for a window to CSV; START=<epoch|ISO> [END=...] [OUT=dir] [LABELS="rate=1000"]
	$(if $(strip $(START)),,$(error START is required, e.g. make collect-metrics START=2026-11-05T10:00:00))
	python3 scripts/collect-resource-metrics.py --start $(call shell-quote,$(START)) $(if $(strip $(END)),--end $(call shell-quote,$(END))) --out $(call shell-quote,$(OUT)) $(foreach label,$(LABELS),--label $(call shell-quote,$(label)))

build-data-producer: ## Compile the Go producer to bin/data-producer
	mkdir -p bin
	go build -o bin/data-producer ./services/data-producer

test-data-producer: ## Run uncached Go tests with race detection, a 2m timeout, and coverage.out
	go test -race -count=1 -timeout=2m -coverprofile=coverage.out ./services/data-producer

run-data-producer: build-data-producer ## Build and publish CSV events; use ARGS="..." for producer flags
	./bin/data-producer $(ARGS)

build-materializer: ## Compile the Go materializer to bin/materializer
	mkdir -p bin
	go build -o bin/materializer ./services/materializer

test-materializer: ## Run materializer race tests and write bin/materializer-coverage.out
	mkdir -p bin
	go test -race -count=1 -timeout=2m -coverprofile=bin/materializer-coverage.out ./services/materializer/...

test-materializer-integration: ## Run PostgreSQL integration tests; requires MATERIALIZER_TEST_DATABASE_URL
	go test -race -count=1 -timeout=2m -tags integration ./services/materializer/internal/data

run-materializer: build-materializer ## Build and run using .env; copy .env.example once to configure
	@$(materializer-env) exec ./bin/materializer

build-streaming-processor: ## Assemble and verify the deployment JAR, then copy it to bin/streaming-processor.jar
	cd services/streaming-processor && sbt verifyAssembly
	mkdir -p bin
	cp services/streaming-processor/target/scala-2.13/streaming-processor.jar bin/streaming-processor.jar

test-streaming-processor: ## Run the Scala processor test suite with sbt
	cd services/streaming-processor && sbt test

infra-up: ## Start local infrastructure and create topics
	docker compose up -d redpanda-0 console jobmanager taskmanager
	docker compose run --rm create-topic

# Build, start local infrastructure, create topics, and submit one processor job.
run-streaming-processor: build-streaming-processor infra-up ## Build the processor, start local infrastructure, create topics, and submit a Flink job; use FLINK_ARGS="..." for job flags
	python3 scripts/submit-flink-job.py --timeout $(FLINK_START_TIMEOUT) -- $(FLINK_ARGS)

run-bounded-processor: build-streaming-processor infra-up ## Process everything currently in trade-events, then stop
	python3 scripts/submit-flink-job.py --timeout $(FLINK_START_TIMEOUT) --bounded-timeout $(BOUNDED_TIMEOUT) -- --bounded --starting-offsets earliest

generate-reference: ## Compute the offline Python reference; CSV=file, directory, or "file1 file2" required, REFERENCE_DIR optional
	$(if $(strip $(CSV)),,$(error CSV is required, e.g. make generate-reference CSV=dataset/debs2022-gc-trading-day-08-11-21.csv))
	python3 validation/generate_validation_data.py $(REFERENCE_CSV_ARGS) --out $(or $(strip $(REFERENCE_DIR)),$(DEFAULT_REFERENCE_DIR))

drain-materializer: build-materializer ## Run the materializer until it has consumed ema-windows and trade-advisories, then stop it
	@$(materializer-env) exec python3 scripts/drain-materializer.py

# DB_CHECK=1 (default) reads the database URL from .env and needs a drained materializer.
compare-flow: ## Compare ema-windows and trade-advisories against the Python shadow; REFERENCE_DIR=... also checks the shadow against a generate-reference output, DB_CHECK=1 the PostgreSQL tables
ifeq ($(DB_CHECK),1)
	@$(materializer-env) exec python3 validation/verify_flow.py $(if $(strip $(REFERENCE_DIR)),--reference-dir $(REFERENCE_DIR)) --db-url "$$MATERIALIZER_DATABASE_URL"
else
	python3 validation/verify_flow.py $(if $(strip $(REFERENCE_DIR)),--reference-dir $(REFERENCE_DIR))
endif

# The CSV reference needs the whole file, so a MAX_EVENTS prefix run skips it.
validate-flow: ## Full validation run; CSV=file, directory, or "file1 file2" required [RATE=0] [MAX_EVENTS=n] [DB_CHECK=0]
	$(if $(strip $(CSV)),,$(error CSV is required, e.g. make validate-flow CSV=dataset/debs2022-gc-trading-day-08-11-21.csv))
	$(if $(strip $(MAX_EVENTS)),,$(MAKE) generate-reference REFERENCE_DIR=$(or $(strip $(REFERENCE_DIR)),$(DEFAULT_REFERENCE_DIR)))
	$(MAKE) infra-up
	$(MAKE) build-materializer
	@$(materializer-env) exec python3 scripts/reset-flow-data.py
	$(MAKE) run-data-producer ARGS=$(call shell-quote,$(PRODUCER_CSV_ARGS) -rate=$(RATE) -dlqEnabled$(if $(strip $(MAX_EVENTS)), -maxEvents=$(MAX_EVENTS)))
	$(MAKE) run-bounded-processor
	$(if $(filter 1,$(DB_CHECK)),$(MAKE) drain-materializer)
	$(MAKE) compare-flow REFERENCE_DIR=$(if $(strip $(MAX_EVENTS)),,$(or $(strip $(REFERENCE_DIR)),$(DEFAULT_REFERENCE_DIR)))

test-validation: ## Run all Python validation and flow tests
	for t in validation/*_test.py scripts/submit_flink_job_test.py scripts/reset_flow_test.py scripts/drain_materializer_test.py scripts/csv_make_test.py scripts/test_flow.py scripts/collect_resource_metrics_test.py monitoring/docker-stats-exporter/exporter_test.py; do \
		echo "== $$t"; \
		python3 $$t || exit 1; \
	done

# Example: make run-flow PRODUCER_ARGS="-rate=1000 -maxEvents=100000"
run-flow: build-data-producer build-materializer setup-backend ## Start infrastructure, migrate PostgreSQL, and supervise producer, materializer, and dashboard; use PRODUCER_ARGS="..."
	@if [ -n "$(filter --bounded,$(FLINK_ARGS))" ]; then \
		echo "run-flow needs a continuous Flink job; remove --bounded from FLINK_ARGS." >&2; \
		exit 1; \
	fi
	@$(materializer-env) export BACKEND_PYTHON=$(call shell-quote,$(BACKEND_PYTHON)) BACKEND_HOST=$(call shell-quote,$(BACKEND_HOST)) BACKEND_PORT=$(call shell-quote,$(BACKEND_PORT)); exec python3 scripts/flow.py -- $(PRODUCER_ARGS)

# Full local cleanup: removes PostgreSQL, broker storage, and Flink checkpoints.
reset-flow: ## Stop managed processes and containers, then permanently delete all local Compose data and checkpoints
	python3 scripts/reset-flow.py

stop-flow: ## Stop managed local processes and Compose containers; preserve data and checkpoints
	python3 scripts/stop-flow.py

formatall: format-data-producer format-materializer format-streaming-processor ## Format Go sources, Scala sources, and sbt build files

format-data-producer: ## Format Go producer application and test sources
	gofmt -w services/data-producer

format-materializer: ## Format Go materializer application and test sources
	gofmt -w services/materializer

# Format Scala application/test sources and sbt build files.
format-streaming-processor: ## Format Scala application/test sources and sbt build files
	cd services/streaming-processor && sbt scalafmtAll scalafmtSbt

lint-streaming-processor: ## Check formatting and compile application/tests with lint; does not rewrite sources
	cd services/streaming-processor && sbt scalafmtCheckAll scalafmtSbtCheck compile Test/compile

generate-protobuf: ## Regenerate gen/ bindings after schema changes; commit them with the schemas
	mkdir -p gen/java
	PATH="$$(go env GOPATH)/bin:$$PATH"; export PATH; \
	find schemas -type f -name '*.proto' -print0 | \
		xargs -0 protoc -I schemas \
			--go_out=. --go_opt=module=financial-streaming-platform \
			--java_out=gen/java

generate-python-protobuf: ## Regenerate gen/python bindings after schema changes; commit them with the schemas
	python3 validation/generate_bindings.py

setup-hooks: ## Run once after cloning to set this checkout's Git hooks path to .githooks
	git config core.hooksPath .githooks
