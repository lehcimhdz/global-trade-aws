.DEFAULT_GOAL := help
SHELL := /bin/bash

# ── Colours ───────────────────────────────────────────────────────────────────
BOLD  := $(shell tput bold 2>/dev/null)
RESET := $(shell tput sgr0 2>/dev/null)
GREEN := $(shell tput setaf 2 2>/dev/null)
CYAN  := $(shell tput setaf 6 2>/dev/null)

.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "$(CYAN)%-22s$(RESET) %s\n", $$1, $$2}'

# ─────────────────────────────────────────────────────────────────────────────
# Dependencies
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: install
install: ## Install runtime dependencies
	pip install -r requirements.txt

.PHONY: install-dev
install-dev: ## Install runtime + dev/test dependencies and pre-commit hooks
	pip install -r requirements.txt -r tests/requirements-test.txt
	pip install black isort flake8 flake8-bugbear flake8-comprehensions mypy types-requests pre-commit
	pre-commit install
	@echo "$(GREEN)Dev environment ready.$(RESET)"

# ─────────────────────────────────────────────────────────────────────────────
# Code quality
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: format
format: ## Auto-format code with black and isort
	black plugins/ dags/ tests/
	isort plugins/ dags/ tests/

.PHONY: lint
lint: ## Run black, isort, and flake8 checks (no auto-fix)
	black --check plugins/ dags/ tests/
	isort --check-only plugins/ dags/ tests/
	flake8 plugins/ dags/ tests/

.PHONY: type-check
type-check: ## Run mypy on the plugin package
	mypy plugins/comtrade/ --ignore-missing-imports

.PHONY: check
check: lint type-check ## Run all quality checks (lint + type-check)

.PHONY: pre-commit
pre-commit: ## Run all pre-commit hooks against all files
	pre-commit run --all-files

# ─────────────────────────────────────────────────────────────────────────────
# Testing
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run the always-available client unit tests (no Airflow required)
	pytest tests/unit/test_client.py -v

.PHONY: test-full
test-full: ## Run the complete test suite (requires Airflow installed)
	pytest tests/ -v --tb=short

.PHONY: test-cov
test-cov: ## Run full suite with HTML coverage report
	pytest tests/ -v --cov=comtrade --cov-report=term-missing --cov-report=html:htmlcov
	@echo "$(GREEN)Coverage report: htmlcov/index.html$(RESET)"

# ─────────────────────────────────────────────────────────────────────────────
# Terraform
# ─────────────────────────────────────────────────────────────────────────────

ENV ?= dev

.PHONY: tf-init
tf-init: ## Initialise Terraform (ENV=dev|prod)
	cd terraform && terraform init

.PHONY: tf-validate
tf-validate: ## Validate Terraform configuration
	cd terraform && terraform validate

.PHONY: tf-plan
tf-plan: ## Preview infrastructure changes (ENV=dev|prod)
	cd terraform && terraform plan -var-file=environments/$(ENV).tfvars

.PHONY: tf-apply
tf-apply: ## Apply infrastructure changes (ENV=dev|prod) — prompts for confirmation
	cd terraform && terraform apply -var-file=environments/$(ENV).tfvars

.PHONY: tf-destroy
tf-destroy: ## Destroy infrastructure (ENV=dev|prod) — prompts for confirmation
	cd terraform && terraform destroy -var-file=environments/$(ENV).tfvars

.PHONY: tf-fmt
tf-fmt: ## Auto-format Terraform files
	cd terraform && terraform fmt -recursive

# ─────────────────────────────────────────────────────────────────────────────
# Secrets
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: bootstrap-secrets
bootstrap-secrets: ## Populate AWS Secrets Manager with values from .env (ENV=dev|prod)
	./scripts/bootstrap_secrets.sh $(ENV)

# ─────────────────────────────────────────────────────────────────────────────
# Docker / Airflow
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: up
up: ## Start the full Airflow stack in the background
	docker compose up -d
	@echo "$(GREEN)Airflow UI: http://localhost:8080$(RESET)"

.PHONY: down
down: ## Stop all containers (data is preserved)
	docker compose down

.PHONY: down-volumes
down-volumes: ## Stop all containers and delete volumes (destructive)
	docker compose down -v

.PHONY: restart
restart: down up ## Restart all containers

.PHONY: logs
logs: ## Tail scheduler and worker logs
	docker compose logs -f airflow-scheduler airflow-worker

.PHONY: init
init: ## Initialise Airflow DB (run once before first `make up`)
	docker compose up airflow-init

.PHONY: import-vars
import-vars: ## Import Airflow Variables from config/airflow_variables.json
	./scripts/init_variables.sh

.PHONY: trigger
trigger: ## Trigger a DAG run — usage: make trigger DAG=comtrade_preview
	docker compose exec airflow-webserver airflow dags trigger $(DAG)
