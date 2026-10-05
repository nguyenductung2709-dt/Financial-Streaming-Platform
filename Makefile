.PHONY: build-data-producer run-data-producer

build-data-producer:
	mkdir -p bin
	go build -o bin/data-producer ./services/data-producer

# Example: make run-data-producer ARGS="-dlqEnabled -maxEvents=100"
run-data-producer: build-data-producer
	./bin/data-producer $(ARGS)
