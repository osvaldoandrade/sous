SHELL := /bin/bash

GO ?= go
BIN_DIR := bin

CMDS := cs-control cs-http-gateway cs-invoker-pool cs-scheduler cs-cadence-poller cs-cli

.PHONY: test lint build clean integration test-contract

test:
	$(GO) test ./...

lint:
	$(GO) vet ./...

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/cs-control ./cmd/cs-control
	$(GO) build -o $(BIN_DIR)/cs-http-gateway ./cmd/cs-http-gateway
	$(GO) build -o $(BIN_DIR)/cs-invoker-pool ./cmd/cs-invoker-pool
	$(GO) build -o $(BIN_DIR)/cs-scheduler ./cmd/cs-scheduler
	$(GO) build -o $(BIN_DIR)/cs-cadence-poller ./cmd/cs-cadence-poller
	$(GO) build -o $(BIN_DIR)/cs ./cmd/cs-cli

integration:
	$(GO) test -tags=integration ./test/integration/...

# test-contract runs the lifecycle contract suite (E1.01) that locks
# the function lifecycle CRUD semantics against cs-control. It is a
# subset of `make test` but is exposed as its own target so CI can
# surface contract regressions independently of unit failures.
test-contract:
	$(GO) test -run TestLifecycle -count=1 ./cmd/cs-control/...

clean:
	rm -rf $(BIN_DIR)
