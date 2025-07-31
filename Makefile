# Makefile for Kafka Ops Agent

.PHONY: help install test test-unit test-integration test-all lint format clean build docker-build docker-test setup-dev

# Default target
help:
	@echo "Kafka Ops Agent - Available targets:"
	@echo ""
	@echo "Development:"
	@echo "  install          Install dependencies and package in development mode"
	@echo "  setup-dev        Set up development environment"
	@echo "  lint             Run code linting (flake8, mypy)"
	@echo "  format           Format code (black, isort)"
	@echo "  clean            Clean build artifacts and cache files"
	@echo ""
	@echo "Testing:"
	@echo "  test             Run all tests"
	@echo "  test-unit        Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  test-performance Run performance tests only"
	@echo "  test-coverage    Run tests with coverage report"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build     Build Docker images"
	@echo "  docker-test      Run tests in Docker environment"
	@echo "  docker-clean     Clean Docker images and containers"
	@echo ""
	@echo "Services:"
	@echo "  start-api        Start the API server"
	@echo "  start-monitoring Start the monitoring server"
	@echo "  start-all        Start all services"
	@echo ""
	@echo "Integration Test Options:"
	@echo "  test-integration-quick    Run integration tests (skip setup/cleanup)"
	@echo "  test-integration-clean    Run integration tests with full cleanup"
	@echo "  test-integration-pattern  Run integration tests matching pattern (PATTERN=...)"

# Variables
PYTHON := python3
PIP := pip3
PYTEST := python -m pytest
DOCKER_COMPOSE := docker-compose
INTEGRATION_TEST_SCRIPT := scripts/run_integration_tests.py

# Development setup
install:
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e .

setup-dev: install
	pre-commit install
	@echo "Development environment set up successfully!"

# Code quality
lint:
	flake8 kafka_ops_agent tests scripts
	mypy kafka_ops_agent --ignore-missing-imports
	@echo "Linting completed!"

format:
	black kafka_ops_agent tests scripts
	isort kafka_ops_agent tests scripts
	@echo "Code formatting completed!"

# Testing
test: test-unit test-integration

test-unit:
	$(PYTEST) tests/ -v --ignore=tests/integration/ \
		--junitxml=test-results/unit-tests.xml \
		--cov=kafka_ops_agent \
		--cov-report=html:test-results/coverage-html \
		--cov-report=xml:test-results/coverage.xml

test-integration:
	$(PYTHON) $(INTEGRATION_TEST_SCRIPT)

test-integration-quick:
	$(PYTHON) $(INTEGRATION_TEST_SCRIPT) --skip-setup --skip-cleanup

test-integration-clean:
	$(PYTHON) $(INTEGRATION_TEST_SCRIPT) --clean-images

test-integration-pattern:
	$(PYTHON) $(INTEGRATION_TEST_SCRIPT) --test-pattern "$(PATTERN)"

test-performance:
	$(PYTHON) $(INTEGRATION_TEST_SCRIPT) --test-file test_performance.py

test-coverage: test-unit
	@echo "Coverage report generated in test-results/coverage-html/"

# Docker
docker-build:
	$(DOCKER_COMPOSE) -f docker-compose.test.yml build

docker-test: docker-build
	$(DOCKER_COMPOSE) -f docker-compose.test.yml up --abort-on-container-exit test-runner
	$(DOCKER_COMPOSE) -f docker-compose.test.yml down -v

docker-clean:
	$(DOCKER_COMPOSE) -f docker-compose.test.yml down -v --remove-orphans
	docker image prune -f --filter label=test=true

# Services
start-api:
	$(PYTHON) -m kafka_ops_agent.api

start-monitoring:
	$(PYTHON) scripts/start_monitoring_server.py

start-all:
	@echo "Starting all services..."
	@echo "This would start API server, monitoring server, and other services"
	@echo "Use docker-compose for full service orchestration"

# Build and packaging
build:
	$(PYTHON) setup.py sdist bdist_wheel

# Cleanup
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .coverage
	rm -rf test-results/
	rm -rf logs/
	@echo "Cleanup completed!"

# Demo and examples
demo-monitoring:
	$(PYTHON) scripts/demo_monitoring_system.py

demo-interactive:
	$(PYTHON) scripts/demo_monitoring_system.py --interactive

demo-provisioning:
	$(PYTHON) scripts/test_provisioning.py

demo-topics:
	$(PYTHON) scripts/test_topic_management.py

# Validation scripts
validate-docker:
	$(PYTHON) scripts/test_docker_provider.py

validate-kubernetes:
	$(PYTHON) scripts/test_kubernetes_provider.py

validate-terraform:
	$(PYTHON) scripts/test_terraform_provider.py

# CI/CD helpers
ci-test: lint test-unit test-integration

ci-build: clean build

ci-docker: docker-build docker-test

# Development helpers
dev-setup: setup-dev
	@echo "Creating sample configuration..."
	mkdir -p config/
	cp config/config.example.yaml config/config.yaml || true
	@echo "Development setup complete!"

dev-reset:
	$(PYTHON) scripts/run_integration_tests.py --skip-setup --test-file test_data_cleanup.py
	@echo "Development environment reset!"

# Quick commands for common workflows
quick-test: test-unit

full-test: clean lint test

release-test: clean lint test-unit docker-test

# Help for specific test categories
test-help:
	@echo "Test Categories:"
	@echo ""
	@echo "Unit Tests:"
	@echo "  - Fast, isolated tests"
	@echo "  - No external dependencies"
	@echo "  - Run with: make test-unit"
	@echo ""
	@echo "Integration Tests:"
	@echo "  - End-to-end workflow tests"
	@echo "  - Requires Docker environment"
	@echo "  - Run with: make test-integration"
	@echo ""
	@echo "Performance Tests:"
	@echo "  - Load and concurrency tests"
	@echo "  - Run with: make test-performance"
	@echo ""
	@echo "Integration Test Options:"
	@echo "  --skip-setup     Skip environment setup"
	@echo "  --skip-cleanup   Skip environment cleanup"
	@echo "  --test-pattern   Run tests matching pattern"
	@echo "  --test-file      Run specific test file"
	@echo "  --verbose        Verbose output"