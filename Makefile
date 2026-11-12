.PHONY: setup demo test dbt clean down
setup:      ## install Python deps
	uv sync
demo:       ## full local run: emulator -> Beam -> DuckDB -> dbt -> report (60s of traffic)
	./scripts/run_demo.sh 60 20
test:       ## unit tests for the rule engine
	uv run pytest -q tests
dbt:        ## rebuild Gold models only
	cd dbt && uv run dbt build --profiles-dir .
down:       ## stop the emulator
	docker compose down
clean: down
	rm -f data/warehouse.duckdb data/pipeline.log
