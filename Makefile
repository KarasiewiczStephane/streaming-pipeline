.PHONY: install test lint clean run docker-build docker-up docker-down docker-logs docker-ps

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v --tb=short --cov=src --cov-report=term-missing -m "not integration"

test-all:
	pytest tests/ -v --tb=short --cov=src --cov-report=term-missing

lint:
	ruff check src/ tests/ --fix
	ruff format src/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage

run:
	python -m src.main

docker-build:
	docker compose -f docker/docker-compose.yml build

docker-up:
	docker compose -f docker/docker-compose.yml up -d

docker-down:
	docker compose -f docker/docker-compose.yml down -v

docker-logs:
	docker compose -f docker/docker-compose.yml logs -f

docker-ps:
	docker compose -f docker/docker-compose.yml ps
