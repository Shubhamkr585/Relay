.PHONY: all help proto build test lint demo demo-check chaos chaos-long chaos-readme bench bench-dry-run bench-full bench-readme resume-numbers docs-check clean

# Default Target
all: build

# -----------------------------------------------------------------------------
# Help & Prerequisites
# -----------------------------------------------------------------------------
help:
	@echo "================================================================================"
	@echo "  RELAY BUILD & TEST TOOLCHAIN (Makefile)"
	@echo "================================================================================"
	@echo "Prerequisites:"
	@echo "  - Go 1.22+ installed and on PATH"
	@echo "  - Protocol Buffers (protoc) compiler with Go plugins for 'make proto'"
	@echo "  - Docker & Docker Compose for 'make demo' / 'make demo-check'"
	@echo ""
	@echo "Available Targets:"
	@echo "  build         : Compiles relayd and relay-cli binaries into bin/"
	@echo "  proto         : Generates gRPC protobuf code from api/proto/v1/relay.proto"
	@echo "  test          : Runs race detector test suite across all packages (count=3)"
	@echo "  lint          : Runs go vet and gofmt formatting validation"
	@echo "  demo          : Runs 3-node cluster leader failover demo"
	@echo "  demo-check    : Runs non-interactive Docker leader failover verification"
	@echo "  chaos         : Runs quick chaos fault-injection tests (~2 min)"
	@echo "  chaos-long    : Runs comprehensive chaos testing suite (50 runs per scenario)"
	@echo "  chaos-readme  : Injects latest chaos verification totals into README.md"
	@echo "  bench         : Runs quick benchmark suite (1 repetition, shorter duration)"
	@echo "  bench-dry-run : Displays the planned benchmark matrix without executing"
	@echo "  bench-full    : Runs full benchmark suite (5 repetitions, 20 failover trials)"
	@echo "  bench-readme  : Injects latest full benchmark report into README.md"
	@echo "  docs-check    : Validates documentation integrity, stale commits, and claims"
	@echo "  clean         : Cleans compiled binaries and temporary data directories"
	@echo "================================================================================"

# -----------------------------------------------------------------------------
# Protobuf & Code Generation
# -----------------------------------------------------------------------------
proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/proto/v1/relay.proto

# -----------------------------------------------------------------------------
# Compilation
# -----------------------------------------------------------------------------
build: proto
	go build -o bin/relayd ./cmd/relayd
	go build -o bin/relay-cli ./cmd/relay-cli

# -----------------------------------------------------------------------------
# Linting & Testing
# -----------------------------------------------------------------------------
lint:
	go vet ./...
	go run ./scripts/check-fmt

test:
	go test -race -count=3 ./...

# -----------------------------------------------------------------------------
# Demo & Failover Verification
# -----------------------------------------------------------------------------
demo: build
	@powershell -ExecutionPolicy Bypass -File ./scripts/demo-failover.ps1 2>/dev/null || bash ./scripts/demo-failover.sh

demo-check: build
	@powershell -ExecutionPolicy Bypass -File ./scripts/demo-failover.ps1 2>/dev/null || bash ./scripts/demo-failover.sh

# -----------------------------------------------------------------------------
# Chaos Testing
# -----------------------------------------------------------------------------
chaos:
	go test -v -race -timeout 30m ./internal/chaos

chaos-long:
	@mkdir -p chaos-results
	@TS=$$(date +%Y%m%d-%H%M%S 2>/dev/null || powershell -Command "Get-Date -Format 'yyyyMMdd-HHmmss'"); \
	DIR="$$(pwd)/chaos-results/$$TS"; \
	mkdir -p "$$DIR"; \
	echo "Running chaos-long into $$DIR..."; \
	CHAOS_REPORT_DIR="$$DIR" CHAOS_RUNS=50 CHAOS_JOBS_PER_RUN=200 CHAOS_SUBPROCESS=1 go test -v -race -timeout 60m ./internal/chaos 2>&1 | tee "$$DIR/chaos-long-output.txt"

chaos-readme:
	go run ./scripts/chaos-readme

# -----------------------------------------------------------------------------
# Benchmarks
# -----------------------------------------------------------------------------
bench: build
	go run ./cmd/relay-bench --quick

bench-dry-run: build
	go run ./cmd/relay-bench --dry-run

bench-full: build
	go run ./cmd/relay-bench --repetitions 5 --trials 20

bench-readme:
	go run ./scripts/bench-readme

resume-numbers: build
	go run ./scripts/resume-numbers.go

# -----------------------------------------------------------------------------
# Documentation Check
# -----------------------------------------------------------------------------
docs-check:
	go run ./scripts/docs-check

# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------
clean:
	rm -rf bin/ data/
