.PHONY: cluster-up cluster-down k8s-load k8s-secrets k8s-apply

cluster-up: ## Create the k3d cluster
	@docker network inspect $(DOCKER_NETWORK) >/dev/null 2>&1 || \
	  { echo "network $(DOCKER_NETWORK) not found — run 'make compose-up' first"; exit 1; }
	k3d cluster create simfleet --agents 2 \
	  --k3s-arg "--disable=traefik@server:0" --network $(DOCKER_NETWORK)

k8s-load: images ## Import images into the cluster
	k3d image import -c simfleet $(addprefix simfleet/,$(addsuffix :$(VERSION),$(IMAGES)))

k8s-secrets: ## Create secrets from the environment
	kubectl create secret generic simfleet-secrets \
	  --from-literal=DATABASE_URL="$(DATABASE_URL)" \
	  --dry-run=client -o yaml | kubectl apply -f -