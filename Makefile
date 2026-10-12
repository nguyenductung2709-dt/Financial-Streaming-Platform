.PHONY: build-data-producer run-data-producer generate-protobuf

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
