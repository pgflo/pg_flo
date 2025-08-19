.PHONY: test lint build clean

# Define the default goal
.DEFAULT_GOAL := build

# Build the application
build:
	go build -o bin/pg_flo

# Run tests with race detection and coverage
test:
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

# Run linter
lint:
	golangci-lint run --timeout=5m

# Clean build artifacts
clean:
	rm -rf bin/ coverage.txt

# Run all checks (lint and test)
check: lint test
