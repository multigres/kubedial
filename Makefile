# kubedial is a library, so there is no binary to build. The targets here are
# the gate a change must pass before it is called done, plus a Kind cluster for
# the e2e-tagged tests.

KIND_CLUSTER ?= e2e-kubedial

.DEFAULT_GOAL := help

###----------------------------------------
##   Targets
#------------------------------------------

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Verification

.PHONY: check
check: ## Vet, lint and run the whole suite with the race detector. The gate before claiming something works.
	go vet ./...
	golangci-lint run ./...
	go test -race ./...

.PHONY: check-e2e
check-e2e: ## Vet and lint the e2e suite, which lives in its own module.
	cd e2e && go vet -tags e2e ./...
	cd e2e && golangci-lint run --build-tags e2e ./...

##@ End-to-end

.PHONY: e2e
e2e: ## Run the e2e suite against a cluster (run kind-up first).
	cd e2e && go test -tags e2e -count=1 ./...

##@ Kind cluster

.PHONY: kind-up
kind-up: ## Create the e2e Kind cluster.
	@command -v kind >/dev/null 2>&1 || { \
		echo "ERROR: kind is not installed."; \
		echo "Install it from: https://kind.sigs.k8s.io/docs/user/quick-start/"; \
		exit 1; \
	}
	@if kind get clusters | grep -qx "$(KIND_CLUSTER)"; then \
		echo "Kind cluster '$(KIND_CLUSTER)' already exists."; \
	else \
		echo "Creating kind cluster '$(KIND_CLUSTER)'..."; \
		kind create cluster --name $(KIND_CLUSTER); \
	fi

.PHONY: kind-down
kind-down: ## Delete the e2e Kind cluster.
	@echo "Deleting kind cluster '$(KIND_CLUSTER)'..."
	kind delete cluster --name $(KIND_CLUSTER)
