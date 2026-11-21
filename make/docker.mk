.PHONY: images compose-up compose-down compose-reset compose-logs migrate

images: ## Build docker images for all services
	@for c in $(IMAGES); do \
	  docker build -f build/Dockerfile --build-arg CMD=$$c --build-arg VERSION=$(VERSION) \
	    -t simfleet/$$c:$(VERSION) -t simfleet/$$c:latest . ; \
	done

compose-up: ## Start dependencies (postgres, kafka)
	docker compose up -d

compose-down: ## Stop dependencies, keep data
	docker compose down

compose-reset: ## Stop dependencies and wipe volumes
	docker compose down -v

compose-logs: ## Follow dependency logs
	docker compose logs -f

migrate: ## Upgrade db to the latest version
	go run ./cmd/migrate
