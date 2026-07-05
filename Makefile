.PHONY: build test run docker-build docker-run clean

build:
	pip install -e .

test:
	pytest -q

run:
	uvicorn reconciliation.app:app --host 0.0.0.0 --port 8080

docker-build:
	docker build -t ai-crypto-onramp/reconciliation .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/reconciliation

clean:
	rm -rf dist build *.egg-info .pytest_cache
