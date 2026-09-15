MAKEFLAGS += --silent

LINT_WORKFLOW   ?= .github/workflows/all.yml
K6_CI_REF       := $(shell grep -oE 'grafana/k6-ci/[^@[:space:]]+@[A-Za-z0-9._/-]+' $(LINT_WORKFLOW) | head -n1 | cut -d@ -f2)
LINT_CONFIG_URL := https://raw.githubusercontent.com/grafana/k6-ci/$(K6_CI_REF)/.golangci.yml
LINT_CONFIG     ?= .golangci.yml

all: clean lint test build

## help: Prints a list of available build targets.
help:
	echo "Usage: make <OPTIONS> ... <TARGETS>"
	echo ""
	echo "Available targets are:"
	echo ''
	sed -n 's/^##//p' ${PWD}/Makefile | column -t -s ':' | sed -e 's/^/ /'
	echo
	echo "Targets run by default are: `sed -n 's/^all: //p' ./Makefile | sed -e 's/ /, /g' | sed -e 's/\(.*\), /\1, and /'`"


## build: Builds a custom 'k6' with the local extension. 
build:
	xk6 build --with $(shell go list -m)=.

$(LINT_CONFIG): $(LINT_WORKFLOW)
	curl -fsSL $(LINT_CONFIG_URL) -o $@

## grpc-server-run: Runs the gRPC server example.
grpc-server-run:	
	go run -mod=mod examples/grpc_server/*.go

## test: Executes any tests.
test:
	echo "Running tests..."
	go test -race -timeout 30s ./...

## lint: Runs golangci-lint with the config pinned by the k6-ci workflow.
lint: $(LINT_CONFIG)
	echo "Running linters..."
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$$(head -n1 $(LINT_CONFIG) | tr -d '# ') \
	  run --config=$(LINT_CONFIG) ./...

## check: Runs the linters and tests.
check: lint test

## clean: Removes any previously created artifacts/downloads.
clean:
	echo "Cleaning up..."
	rm -f ./k6
	rm -f $(LINT_CONFIG)
	rm -rf vendor

.PHONY: test clean help lint check grpc-server-run build
