.PHONY: run test lint format

run:
	PYTHONPATH=src INTERNAL_HOSTS=esshopzilla.com python -m external_search_revenue.main data/hit_data.tsv

test:
	PYTHONPATH=src pytest -q

lint:
	ruff check .

format:
	ruff format .