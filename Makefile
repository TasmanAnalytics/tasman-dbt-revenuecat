.DEFAULT_GOAL := help

.PHONY: help
help: ## Show targets and comments (must have ##)
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

.PHONY: setup
setup: ## Install project dependencies
	@if ! command -v uv; then \
		echo "Installing uv..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
	fi
	uv sync
	uv run pre-commit install --install-hooks

.PHONY: lint
lint: ## Run linters and formatters
	@echo "\\033[0;34mpre-commit checks\\033[0m"
	SKIP=identity uv run pre-commit run --all-files --hook-stage pre-commit
	@echo "\\033[0;34mpre-push checks\\033[0m"
	SKIP=identity uv run pre-commit run --all-files --hook-stage pre-push

.PHONY: integration_tests
integration_tests: ## Run integration tests
	uv run ./run_test.sh
