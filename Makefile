# Every target runs inside the dev container (see compose.yaml); only docker runs on the host.
SHELL := /bin/bash

export HOST_UID := $(shell id -u)
export HOST_GID := $(shell id -g)

RUN := docker compose run --rm dev

.DEFAULT_GOAL := help

.PHONY: help build shell lint format test check auth-login whoami bundle-validate setup-uc ingest clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

build: .env ## Build the dev image
	docker compose build

shell: .env ## Open a shell in the dev container
	$(RUN) bash

lint: .env ## Ruff lint + format check
	$(RUN) sh -c "ruff check . && ruff format --check ."

format: .env ## Auto-fix lint issues and format
	$(RUN) sh -c "ruff check --fix . && ruff format ."

test: .env ## Run the pytest suite
	$(RUN) pytest

check: lint test ## Lint and test (what CI runs)

auth-login: .env ## OAuth login: make auth-login HOST=https://<workspace>.cloud.databricks.com
	@test -n "$(HOST)" || { echo "usage: make auth-login HOST=https://<workspace-url>"; exit 1; }
	docker compose run --rm -p 8020:8021 dev scripts/auth-login.sh "$(HOST)"

whoami: .env ## Check the CLI profile works
	$(RUN) databricks current-user me

bundle-validate: .env ## Validate databricks.yml against the workspace
	$(RUN) databricks bundle validate

setup-uc: .env ## Create the UC schemas and volumes (safe to rerun)
	$(RUN) python -m gh_lakehouse.workspace_setup

# make ingest START=2026-10-06 [END=2026-10-07] [ARGS=--dry-run]
ingest: .env ## Upload GH Archive hours to the landing volume: make ingest START=YYYY-MM-DD [END=...]
	@test -n "$(START)" || { echo "usage: make ingest START=YYYY-MM-DD [END=YYYY-MM-DD] [ARGS=--dry-run]"; exit 1; }
	$(RUN) python -m gh_lakehouse.ingestion --start $(START) $(if $(END),--end $(END)) $(ARGS)

clean: ## Remove local caches (keeps the image and your Databricks login)
	rm -rf .pytest_cache .ruff_cache spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
