.PHONY: check lint test test-integration test-chaos chaos-campaign up down scale logs load

# One command for "done": lint, docs rule, unit tests. Exits non-zero on any failure.
check: lint test

lint:
	uv run ruff check .
	uv run ruff check --isolated --select D100,D101,D102,D103 --extend-exclude tests,migrations,versions,scripts,.claude .

test:
	uv run pytest tests/unit -q

test-integration:
	uv run pytest tests/integration -q -m integration

chaos-campaign:
	uv run python loadtest/chaos_campaign.py --run $(or $(RUN),local) --rps $(or $(RPS),40) --minutes $(or $(MIN),4)

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
	loadtest/run.sh $(or $(WORKERS),1) $(or $(USERS),300) $(or $(TIME),60s)
