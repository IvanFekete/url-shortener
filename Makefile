.DEFAULT_GOAL := unit

GO ?= go
TEST_TIMEOUT ?= 2m
LOAD_DURATION ?= 5s
LOAD_CONCURRENCY ?= 8
LOAD_BASE_URL ?=
export LOAD_DURATION LOAD_CONCURRENCY LOAD_BASE_URL

TEST_COMPOSE = docker compose -p shortener-tests -f tests/compose.yaml

.PHONY: unit integration load stress loadgen all

unit:
	$(GO) test -race ./... -skip '^Test(Integration|Load|Stress)' -count=1 -timeout=$(TEST_TIMEOUT)

# Start disposable services and clean them up, including on failure or interruption.
integration:
	@set -eu; \
	trap 'status=$$?; $(TEST_COMPOSE) down || status=1; exit $$status' EXIT; \
	trap 'exit 130' INT; \
	trap 'exit 143' TERM; \
	$(TEST_COMPOSE) up -d --wait --wait-timeout 60; \
	RUN_INTEGRATION=1 \
	INTEGRATION_DYNAMODB_URL=http://localhost:18000 \
	INTEGRATION_REDIS_URL=redis://localhost:16379 \
	INTEGRATION_SQS_URL=http://localhost:14566 \
	$(GO) test -race ./... -run '^TestIntegration' -count=1 -timeout=$(TEST_TIMEOUT)

load:
	RUN_LOAD=1 $(GO) test ./internal/api -run '^TestLoad' -count=1 -v -timeout=$(TEST_TIMEOUT)

stress:
	RUN_STRESS=1 $(GO) test -race ./internal/api -run '^TestStress' -count=1 -v -timeout=$(TEST_TIMEOUT)

# Open-loop 95/5 redirect/create workload against a running stack (docker compose up -d).
LOADGEN_BASE_URL ?= http://localhost:8080
LOADGEN_RATES ?= 200,500,1000,2000
LOADGEN_STAGE ?= 30s
loadgen:
	$(GO) run ./cmd/loadgen -base $(LOADGEN_BASE_URL) -rates $(LOADGEN_RATES) -stage $(LOADGEN_STAGE)

# Keep suites sequential even with make -j; all explicitly opts into load/stress.
all:
	$(MAKE) unit
	$(MAKE) integration
	$(MAKE) load
	$(MAKE) stress
