.PHONY: check lint test test-integration test-chaos up down scale logs load

# One command for "done": lint, docs rule, unit tests. Exits non-zero on any failure.
check: lint test

lint:
	uv run ruff check .
	uv run ruff check --isolated --select D100,D101,D102,D103 --extend-exclude tests,migrations,versions,scripts,.claude .

test:
	uv run pytest tests/unit -q

test-integration:
	uv run pytest tests/integration -q -m integration

test-chaos:
	uv run pytest tests/integration -q -m chaos

up:
	docker compose up -d --build --wait

down:
	docker compose down -v

scale:
	docker compose up -d --scale worker=$(or $(N),4) --no-recreate

logs:
	docker compose logs -f --tail=100

load:
	uv run --group load locust -f loadtest/locustfile.py --headless -u $(or $(USERS),300) -r 50 -t $(or $(TIME),2m) \
		--host http://localhost:8000 --csv loadtest/results/run --html loadtest/results/run.html
