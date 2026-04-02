#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Variables
REGISTRY ?= accumulo
TAG ?= 4.0.0-SNAPSHOT
RELEASE_NAME ?= accumulo-dev
NAMESPACE ?= default
VALUES_FILE ?= charts/accumulo/values-dev.yaml

# Colors
BLUE = \033[0;34m
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

# Helper function to print colored output
define log_info
	@echo -e "$(BLUE)[INFO]$(NC) $(1)"
endef

define log_success  
	@echo -e "$(GREEN)[SUCCESS]$(NC) $(1)"
endef

define log_warning
	@echo -e "$(YELLOW)[WARNING]$(NC) $(1)"
endef

define log_error
	@echo -e "$(RED)[ERROR]$(NC) $(1)"
endef

.PHONY: help
help: ## Show this help message
	@echo "Apache Accumulo with Alluxio on Kubernetes"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Variables:"
	@echo "  REGISTRY=$(REGISTRY)        - Docker registry"
	@echo "  TAG=$(TAG)              - Docker tag"
	@echo "  RELEASE_NAME=$(RELEASE_NAME)      - Helm release name"
	@echo "  NAMESPACE=$(NAMESPACE)            - Kubernetes namespace"
	@echo "  VALUES_FILE=$(VALUES_FILE) - Helm values file"

.PHONY: build
build: ## Build Accumulo distribution
	$(call log_info,"Building Accumulo distribution...")
	mvn clean package -DskipTests -pl assemble -am
	$(call log_success,"Accumulo distribution built successfully")

.PHONY: docker-build
docker-build: build ## Build Docker image
	$(call log_info,"Building Docker image: $(REGISTRY)/accumulo:$(TAG)")
	./scripts/build-docker.sh -r $(REGISTRY) -t $(TAG)
	$(call log_success,"Docker image built successfully")

.PHONY: docker-push
docker-push: build ## Build and push Docker image
	$(call log_info,"Building and pushing Docker image: $(REGISTRY)/accumulo:$(TAG)")
	./scripts/build-docker.sh -r $(REGISTRY) -t $(TAG) -p
	$(call log_success,"Docker image built and pushed successfully")

.PHONY: generate-config
generate-config: ## Generate configuration with secrets
	$(call log_info,"Generating configuration...")
	./scripts/generate-secrets.sh -o values-generated.yaml --non-interactive -i $(RELEASE_NAME)
	$(call log_success,"Configuration generated: values-generated.yaml")

.PHONY: generate-config-interactive
generate-config-interactive: ## Generate configuration interactively
	$(call log_info,"Generating configuration interactively...")
	./scripts/generate-secrets.sh -o values-generated.yaml -i $(RELEASE_NAME)
	$(call log_success,"Configuration generated: values-generated.yaml")

.PHONY: deploy-dev
deploy-dev: ## Deploy development environment
	$(call log_info,"Deploying development environment...")
	./scripts/helm-deploy.sh install -r $(RELEASE_NAME) -f $(VALUES_FILE) --create-namespace -n $(NAMESPACE)
	$(call log_success,"Development environment deployed successfully")

.PHONY: deploy
deploy: generate-config ## Deploy with generated configuration
	$(call log_info,"Deploying with generated configuration...")
	./scripts/helm-deploy.sh install -r $(RELEASE_NAME) -f values-generated.yaml --create-namespace -n $(NAMESPACE)
	$(call log_success,"Deployment completed successfully")

.PHONY: upgrade
upgrade: ## Upgrade existing deployment
	$(call log_info,"Upgrading deployment...")
	./scripts/helm-deploy.sh upgrade -r $(RELEASE_NAME) -f $(VALUES_FILE) -n $(NAMESPACE)
	$(call log_success,"Upgrade completed successfully")

.PHONY: test
test: ## Run smoke tests
	$(call log_info,"Running smoke tests...")
	./scripts/helm-deploy.sh test -r $(RELEASE_NAME) -n $(NAMESPACE)
	$(call log_success,"Tests completed successfully")

.PHONY: validate-init
validate-init: ## Validate Accumulo initialization with Alluxio
	$(call log_info,"Validating Accumulo initialization...")
	./scripts/validate-accumulo-init.sh $(RELEASE_NAME) $(NAMESPACE)
	$(call log_success,"Validation completed")

.PHONY: status
status: ## Show deployment status
	./scripts/helm-deploy.sh status -r $(RELEASE_NAME) -n $(NAMESPACE)

.PHONY: uninstall
uninstall: ## Uninstall deployment
	$(call log_warning,"Uninstalling deployment...")
	./scripts/helm-deploy.sh uninstall -r $(RELEASE_NAME) -n $(NAMESPACE)
	$(call log_success,"Deployment uninstalled successfully")

.PHONY: logs
logs: ## Show logs from all Accumulo components
	$(call log_info,"Showing logs from Accumulo components...")
	kubectl logs -l app.kubernetes.io/name=accumulo -n $(NAMESPACE) --tail=100

.PHONY: shell
shell: ## Access Accumulo shell
	$(call log_info,"Connecting to Accumulo shell...")
	kubectl exec -it deployment/$(RELEASE_NAME)-manager -n $(NAMESPACE) -- /opt/accumulo/bin/accumulo shell -u root

.PHONY: port-forward
port-forward: ## Forward ports for local access
	$(call log_info,"Setting up port forwarding...")
	@echo "Accumulo Monitor will be available at: http://localhost:9995"
	@echo "Alluxio Master will be available at: http://localhost:19999"
	@echo "Press Ctrl+C to stop port forwarding"
	kubectl port-forward svc/$(RELEASE_NAME)-monitor 9995:9995 -n $(NAMESPACE) &
	kubectl port-forward svc/$(RELEASE_NAME)-alluxio-master 19999:19999 -n $(NAMESPACE) &
	wait

.PHONY: clean-docker
clean-docker: ## Clean up Docker images and containers
	$(call log_info,"Cleaning up Docker images...")
	docker images | grep $(REGISTRY)/accumulo | awk '{print $$3}' | xargs -r docker rmi -f
	$(call log_success,"Docker cleanup completed")

.PHONY: validate
validate: ## Validate Helm chart
	$(call log_info,"Validating Helm chart...")
	helm lint charts/accumulo
	$(call log_success,"Helm chart validation passed")

.PHONY: template
template: ## Generate Kubernetes templates
	$(call log_info,"Generating Kubernetes templates...")
	helm template $(RELEASE_NAME) charts/accumulo -f $(VALUES_FILE) --namespace $(NAMESPACE) > accumulo-templates.yaml
	$(call log_success,"Templates generated: accumulo-templates.yaml")

.PHONY: debug
debug: ## Debug deployment issues
	$(call log_info,"Gathering debug information...")
	@echo "=== Helm Status ==="
	-helm status $(RELEASE_NAME) -n $(NAMESPACE)
	@echo ""
	@echo "=== Pod Status ==="
	-kubectl get pods -l app.kubernetes.io/name=accumulo -n $(NAMESPACE)
	@echo ""
	@echo "=== Service Status ==="
	-kubectl get services -l app.kubernetes.io/name=accumulo -n $(NAMESPACE)
	@echo ""
	@echo "=== Recent Events ==="
	-kubectl get events -n $(NAMESPACE) --sort-by='.lastTimestamp' | tail -10
	@echo ""
	@echo "=== Pod Descriptions ==="
	-kubectl describe pods -l app.kubernetes.io/name=accumulo -n $(NAMESPACE)

.PHONY: kind-create
kind-create: ## Create KinD cluster for local development
	$(call log_info,"Creating KinD cluster...")
	kind create cluster --name accumulo-dev --config - <<EOF
	kind: Cluster
	apiVersion: kind.x-k8s.io/v1alpha4
	nodes:
	- role: control-plane
	  extraPortMappings:
	  - containerPort: 80
	    hostPort: 80
	    protocol: TCP
	  - containerPort: 443
	    hostPort: 443
	    protocol: TCP
	  - containerPort: 9995
	    hostPort: 9995
	    protocol: TCP
	EOF
	$(call log_success,"KinD cluster created successfully")

.PHONY: kind-delete
kind-delete: ## Delete KinD cluster
	$(call log_info,"Deleting KinD cluster...")
	kind delete cluster --name accumulo-dev
	$(call log_success,"KinD cluster deleted successfully")

.PHONY: full-dev-setup
full-dev-setup: kind-create deploy-dev test ## Complete development setup
	$(call log_success,"Full development setup completed!")
	@echo ""
	@echo "Access services:"
	@echo "  Accumulo Monitor: http://localhost:9995 (after running 'make port-forward')"
	@echo "  Accumulo Shell: make shell"
	@echo ""
	@echo "Useful commands:"
	@echo "  make logs     - View logs"
	@echo "  make status   - Check status"
	@echo "  make test     - Run tests"

.PHONY: full-cleanup
full-cleanup: uninstall kind-delete clean-docker ## Complete cleanup
	$(call log_success,"Full cleanup completed!")

# Default target
.DEFAULT_GOAL := help