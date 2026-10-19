.PHONY: build-data-producer test-data-producer run-data-producer generate-protobuf build-streaming-processor test-streaming-processor

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
