#!make
.PHONY: setup integration_tests

# Initialisation recipes
setup: ## Install uv
	@if ! command -v uv; then \
		echo "Installing uv..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
	fi
	uv sync

integration_tests: ## Run integration tests
	uv run ./run_test.sh
