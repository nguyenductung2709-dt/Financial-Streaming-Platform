FLINK_ARGS ?= --starting-offsets earliest
PRODUCER_ARGS ?= -rate=1000
FLINK_START_TIMEOUT ?= 120

.PHONY: build-data-producer test-data-producer run-data-producer generate-protobuf build-streaming-processor test-streaming-processor format-streaming-processor lint-streaming-processor setup-hooks run-streaming-processor run-flow reset-flow

test-data-producer:
	go test -race -count=1 -timeout=2m -coverprofile=coverage.out ./services/data-producer

build-data-producer:
	mkdir -p bin
	go build -o bin/data-producer ./services/data-producer

# Example: make run-data-producer ARGS="-dlqEnabled -maxEvents=100"
run-data-producer: build-data-producer
	./bin/data-producer $(ARGS)

# Regenerate Go and Java bindings for every schema, matching CI.
generate-protobuf:
	mkdir -p gen/java
	PATH="$$(go env GOPATH)/bin:$$PATH"; export PATH; \
	find schemas -type f -name '*.proto' -print0 | \
		xargs -0 protoc -I schemas --go_out=. --go_opt=module=financial-streaming-platform --java_out=gen/java

# sbt compiles the shared Java bindings from gen/java.
test-streaming-processor:
	cd services/streaming-processor && sbt test

build-streaming-processor:
	cd services/streaming-processor && sbt verifyAssembly
	mkdir -p bin
	cp services/streaming-processor/target/scala-2.13/streaming-processor.jar bin/streaming-processor.jar

# Build, start local infrastructure, create topics, and submit one processor job.
run-streaming-processor: build-streaming-processor
	docker compose up -d redpanda-0 console jobmanager taskmanager
	docker compose run --rm create-topic
	python3 scripts/submit-flink-job.py --timeout $(FLINK_START_TIMEOUT) -- $(FLINK_ARGS)

# Example: make run-flow PRODUCER_ARGS="-rate=1000 -maxEvents=100000"
run-flow: build-data-producer
	@if [ -n "$(filter --bounded,$(FLINK_ARGS))" ]; then \
		echo "run-flow needs a continuous Flink job; remove --bounded from FLINK_ARGS." >&2; \
		exit 1; \
	fi
	$(MAKE) run-streaming-processor
	./bin/data-producer $(PRODUCER_ARGS)

# Stop the producer with Ctrl+C first. This permanently clears project topic records.
reset-flow:
	python3 scripts/reset-flow.py

# Format Scala application/test sources and sbt build files.
format-streaming-processor:
	cd services/streaming-processor && sbt scalafmtAll scalafmtSbt

# Check formatting and compiler lint without rewriting files.
lint-streaming-processor:
	cd services/streaming-processor && sbt scalafmtCheckAll scalafmtSbtCheck compile Test/compile

# Point git at the checked-in hooks (run once after cloning).
setup-hooks:
	git config core.hooksPath .githooks
