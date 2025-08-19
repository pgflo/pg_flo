.PHONY: test lint build build-ci build-prod clean check test-short

.DEFAULT_GOAL := build-prod

build: build-prod

build-ci:
	go build -race -o bin/pg_flo

build-prod:
	go build -o bin/pg_flo

test:
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

test-short:
	go test -v -race -short -timeout=5m ./...

lint:
	golangci-lint run --timeout=5m

clean:
	rm -rf bin/ coverage.txt

check: lint test
