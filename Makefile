#!make
.PHONY: help poetry dbt integration_tests
.DEFAULT_GOAL := help

# Initialisation recipes
poetry: ## Install poetry
	@if ! command -v poetry; then\
		curl -sSL https://install.python-poetry.org | python3 -;\
	fi
	poetry install --directory ../

# dbt Development recipes
dbt: poetry ## Start a dbt shell
	export DBT_PROFILES_DIR=~/.dbt/ && export SHELL=/bin/zsh && poetry shell

integration_tests: ## Run integration tests
	./run_test.sh
