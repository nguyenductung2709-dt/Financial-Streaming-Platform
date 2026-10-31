.DEFAULT_GOAL := help
FLINK_ARGS ?= --starting-offsets earliest
PRODUCER_ARGS ?= -rate=1000
FLINK_START_TIMEOUT ?= 120
FLINK_HA ?= 0
ifeq ($(FLINK_HA),1)
export COMPOSE_FILE := compose.yaml:compose.ha.yaml
endif
FLINK_RESTORE_PATH ?=

.PHONY: build-data-producer test-data-producer run-data-producer generate-protobuf build-streaming-processor test-streaming-processor format-streaming-processor lint-streaming-processor setup-hooks run-streaming-processor run-flow reset-flow restore-streaming-processor test-flink-scripts verify-producer-recovery

.PHONY: help \
	build-data-producer test-data-producer run-data-producer \
	build-streaming-processor test-streaming-processor \
	formatall format-data-producer format-streaming-processor lint-streaming-processor \
	generate-protobuf setup-hooks

help: ## Show command usage; also displayed by plain make
	@printf 'Run from the repository root: make <target>\n\nCommands:\n\n'
	@awk 'BEGIN { FS = ":.*## " } /^[a-zA-Z0-9_-]+:.*## / { printf "  make %s\n    %s\n\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf '%s\n' \
		'Producer arguments (pass flags through ARGS):' \
		'  make run-data-producer ARGS="-help"' \
		'  make run-data-producer ARGS="-dlqEnabled -maxEvents=100"' \
		'  make run-data-producer ARGS="-csvPath=/path/to/trades.csv -brokers=localhost:19092"' \
		'' \
		'Optional automatic Flink recovery: append FLINK_HA=1 (see services/streaming-processor/README.md).' \
		'' \
		'Requirements:' \
		'  Producer: Go; race tests also require cgo and a C compiler.' \
		'  Processor: Java 17 and sbt.' \
		'  Code generation: protoc and protoc-gen-go (see schemas/market/v1/README.md).' \
		'  Publishing: a running broker, existing topics, and an input CSV.'

build-data-producer: ## Compile the Go producer to bin/data-producer
	mkdir -p bin
	go build -o bin/data-producer ./services/data-producer

test-data-producer: ## Run uncached Go tests with race detection, a 2m timeout, and coverage.out
	go test -race -count=1 -timeout=2m -coverprofile=coverage.out ./services/data-producer

run-data-producer: build-data-producer ## Build and publish CSV events; use ARGS="..." for producer flags
	./bin/data-producer $(ARGS)

build-streaming-processor: ## Assemble and verify the deployment JAR, then copy it to bin/streaming-processor.jar
	cd services/streaming-processor && sbt verifyAssembly
	mkdir -p bin
	cp services/streaming-processor/target/scala-2.13/streaming-processor.jar bin/streaming-processor.jar

test-streaming-processor: ## Run the Scala processor test suite with sbt
	cd services/streaming-processor && sbt test

# Build, start local infrastructure, create topics, and submit one processor job.
run-streaming-processor: build-streaming-processor ## Build the processor, start local infrastructure, create topics, and submit a Flink job; use FLINK_ARGS="..." for job flags
	docker compose up -d redpanda-0 console jobmanager taskmanager
	docker compose run --rm create-topic
	python3 scripts/submit-flink-job.py --timeout $(FLINK_START_TIMEOUT) $(if $(strip $(FLINK_RESTORE_PATH)),--restore-from "$(FLINK_RESTORE_PATH)") -- $(FLINK_ARGS)

restore-streaming-processor: ## Restore the processor; requires FLINK_RESTORE_PATH=file:///opt/flink/state/checkpoints/.../chk-N
	@if [ -z "$(strip $(FLINK_RESTORE_PATH))" ]; then \
		echo "Set FLINK_RESTORE_PATH to a completed checkpoint or savepoint URI." >&2; \
		exit 1; \
	fi
	$(MAKE) run-streaming-processor

test-flink-scripts: ## Test Flink launcher and recovery verification logic without Docker
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'

# Example: make run-flow PRODUCER_ARGS="-rate=1000 -maxEvents=100000"
run-flow: build-data-producer ## Start the processor and publish CSV events; use PRODUCER_ARGS="..." for producer flags
	@if [ -n "$(filter --bounded,$(FLINK_ARGS))" ]; then \
		echo "run-flow needs a continuous Flink job; remove --bounded from FLINK_ARGS." >&2; \
		exit 1; \
	fi
	$(MAKE) run-streaming-processor
	./bin/data-producer $(PRODUCER_ARGS)

# Stop the producer with Ctrl+C first. This permanently clears project topic records.
reset-flow: ## Cancel processor jobs and permanently clear trade-events, ema-windows, trade-advisories, and trade-events-dlq; stop the producer first
	python3 scripts/reset-flow.py

formatall: format-data-producer format-streaming-processor ## Format Go sources, Scala sources, and sbt build files

format-data-producer: ## Format Go producer application and test sources
	gofmt -w services/data-producer

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

setup-hooks: ## Run once after cloning to set this checkout's Git hooks path to .githooks
	git config core.hooksPath .githooks

verify-producer-recovery: build-data-producer build-streaming-processor ## Live producer SIGKILL/resume correctness test; requires running broker and one free Flink slot
	python3 scripts/verify-producer-recovery.py
