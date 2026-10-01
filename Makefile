# raft-kv developer commands. Run `make help` for the list.
#
# Requires Go and, for the Docker targets, Docker with Compose v2. On Windows
# use WSL2 (see README). Code generation and lint tools are installed, at
# pinned versions, by `make tools`.

SHELL       := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

export PATH := $(HOME)/.local/bin:$(shell go env GOPATH)/bin:$(PATH)

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)
PKGS    := ./...

# Seeds for the randomised suites. Raise them for a deeper local run, e.g.
#   make sim RAFT_SIM_SEEDS=2000
RAFT_SIM_SEEDS      ?= 300
STORAGE_CRASH_SEEDS ?= 60
LIN_SIM_SEEDS       ?= 300
FUZZTIME            ?= 20s

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "} {printf "  %-18s %s\n", $$1, $$2}'

# ---- build -------------------------------------------------------------------

.PHONY: tools
tools: ## Install the pinned protoc, plugins, staticcheck and govulncheck
	scripts/install-tools.sh

.PHONY: generate
generate: ## Regenerate Go code from api/raft/v1/raft.proto
	scripts/generate.sh

.PHONY: generate-check
generate-check: generate ## Fail if the committed generated code is out of date
	@git diff --exit-code -- internal/gen || { echo "generated code is out of date: run 'make generate' and commit the result" >&2; exit 1; }

.PHONY: build
build: ## Build kvserver, kvctl and kvbench into bin/
	mkdir -p bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

.PHONY: build-windows
build-windows: ## Check that everything compiles for Windows (kvctl and kvbench run there; kvserver needs Linux)
	GOOS=windows GOARCH=amd64 go build -o /dev/null ./cmd/...

# ---- static checks -----------------------------------------------------------

.PHONY: fmt
fmt: ## Format the code
	gofmt -w cmd internal tests

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt-formatted
	@out="$$(gofmt -l cmd internal tests)"; if [[ -n "$$out" ]]; then echo "not formatted:"; echo "$$out"; exit 1; fi

.PHONY: vet
vet: ## Run go vet, including the integration tests
	go vet $(PKGS)
	go vet -tags integration ./tests/integration/

.PHONY: staticcheck
staticcheck: ## Run staticcheck
	staticcheck $(PKGS)
	staticcheck -tags integration ./tests/integration/

.PHONY: vuln
vuln: ## Check dependencies for known vulnerabilities (exceptions: .vuln-exceptions)
	scripts/vulncheck.sh

.PHONY: lint
lint: fmt-check vet staticcheck ## fmt-check, vet and staticcheck

# ---- tests -------------------------------------------------------------------

.PHONY: test
test: ## Unit tests and the default-size simulations (fast)
	go test $(PKGS)

.PHONY: test-race
test-race: ## The same tests under the race detector (needs a C compiler)
	go test -race $(PKGS)

.PHONY: sim
sim: ## Deterministic Raft simulation over many seeds
	RAFT_SIM_SEEDS=$(RAFT_SIM_SEEDS) go test ./internal/raft/ -run 'TestSim' -count=1

.PHONY: crash
crash: ## Storage crash-injection sweep over many seeds
	STORAGE_CRASH_SEEDS=$(STORAGE_CRASH_SEEDS) go test ./internal/storage/ -run 'Crash' -count=1

.PHONY: linearizability
linearizability: ## History checking: simulated cluster over many seeds, plus a live in-process cluster
	LIN_SIM_SEEDS=$(LIN_SIM_SEEDS) go test ./tests/linearizability/ -count=1 -v -run 'TestModel|TestKeys|TestChecker|TestSimulated|TestLive' | grep -E '^(--- |=== RUN   Test[A-Za-z]+$$|\s+(sim|live)_test.go|ok|FAIL|PASS)'

.PHONY: integration
integration: ## Real kvserver processes on loopback: kill, restart, snapshot, TLS
	go test -tags integration ./tests/integration/ -count=1 -timeout 15m

.PHONY: fuzz
fuzz: ## Run each fuzz target for FUZZTIME (default 20s)
	go test ./internal/kv/ -run '^$$' -fuzz '^FuzzDecodeCommand$$' -fuzztime $(FUZZTIME)
	go test ./internal/kv/ -run '^$$' -fuzz '^FuzzApply$$' -fuzztime $(FUZZTIME)
	go test ./internal/kv/ -run '^$$' -fuzz '^FuzzRestore$$' -fuzztime $(FUZZTIME)
	go test ./internal/storage/ -run '^$$' -fuzz '^FuzzWALRecovery$$' -fuzztime $(FUZZTIME)
	go test ./internal/storage/ -run '^$$' -fuzz '^FuzzSnapshotDecoding$$' -fuzztime $(FUZZTIME)

.PHONY: cover
cover: ## Write a coverage profile to coverage.out and print the total
	go test -coverprofile=coverage.out -coverpkg=./internal/...,./cmd/... $(PKGS) >/dev/null
	go tool cover -func=coverage.out | tail -n 1

.PHONY: check
check: lint generate-check test-race sim crash linearizability integration ## Everything CI runs, in one go

# ---- Docker cluster and demos -----------------------------------------------

.PHONY: up
up: build ## Start the 3-node Docker cluster (with fault injection enabled)
	scripts/cluster.sh up

.PHONY: down
down: ## Stop the Docker cluster, keeping its data
	scripts/cluster.sh down

.PHONY: demo
demo: build ## Run every demo with commentary: normal operations, then six failure scenarios
	scripts/demo.sh

.PHONY: chaos
chaos: build ## Run the fault demos against the Docker cluster; non-zero exit if any check fails
	scripts/cluster.sh up
	scripts/demo-kill-leader.sh
	scripts/demo-partition.sh
	scripts/demo-link-failure.sh
	scripts/demo-quorum-loss.sh
	scripts/demo-snapshot.sh
	scripts/demo-five-node.sh
	docker compose -f docker-compose.5node.yml stop

# ---- benchmarks --------------------------------------------------------------

# The WAL benchmark fsyncs real files. /tmp is often tmpfs, where fsync is
# free, so the benchmark's temporary directory is placed on a real disk.
BENCH_TMP ?= $(HOME)/.cache/raftkv-bench-tmp

.PHONY: bench-micro
bench-micro: ## Go benchmarks for the WAL, the state machine and the codec
	mkdir -p $(BENCH_TMP)
	TMPDIR=$(BENCH_TMP) go test ./internal/storage/ ./internal/kv/ -run '^$$' -bench . -benchmem -count=3

.PHONY: bench
bench: build ## Full benchmark matrix against Docker clusters; writes benchmarks/results/
	scripts/bench.sh

.PHONY: clean
clean: ## Remove build outputs (never touches cluster data)
	rm -rf bin coverage.out
