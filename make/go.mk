.PHONY: build test lint run-api run-local submit report

build: ## Build services
	go build -o bin/ ./cmd/...

test:
	go test ./... -race -count=1

lint:
	golangci-lint run

run-local:
	go run ./cmd/runlocal submit $(SPEC)

submit: ## Submit and follow live, then print the report
	go run ./cmd/simfleet submit -watch $(SPEC)

report: ## Display report
	go run ./cmd/simfleet report $(EXP)
	