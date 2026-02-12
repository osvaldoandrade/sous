SHELL := /bin/bash

GO ?= go
BIN_DIR := bin

CMDS := cs-control cs-http-gateway cs-invoker-pool cs-scheduler cs-cadence-poller cs-cli

.PHONY: test lint build clean integration

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

clean:
	rm -rf $(BIN_DIR)
