PY ?= python3
export PYTHONPATH := src

.PHONY: install demo generate run report summary test lint spark-demo report-spark up down clean

install:
	$(PY) -m pip install -r requirements-dev.txt

## Local engine end to end: generate → stream 12 batches → "restart" → finish → charts → summary
demo: clean generate
	$(PY) -m clickstream run --max-batches 12
	@echo "--- simulated restart: resuming from the checkpoint ---"
	$(PY) -m clickstream run
	$(PY) -m clickstream report
	$(PY) -m clickstream summary

generate:
	$(PY) -m clickstream generate

run:
	$(PY) -m clickstream run

report:
	$(PY) -m clickstream report

summary:
	$(PY) -m clickstream summary

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check src tests

## Same stream through Spark Structured Streaming (file source, local[*]); needs requirements-stream.txt + Java 17
spark-demo:
	rm -rf data/spark_input data/spark_lake data/spark_checkpoints
	$(PY) -m clickstream export-files --out data/spark_input
	$(PY) -m clickstream spark --source files --files data/spark_input
	$(PY) -m clickstream report --lake data/spark_lake --checkpoint data/spark_checkpoints --out data/spark_report

## Charts from the docker-compose Spark output
report-spark:
	$(PY) -m clickstream report --lake data/spark_lake --checkpoint data/spark_checkpoints --out data/spark_report

up:
	docker compose up --build

down:
	docker compose down -v

clean:
	rm -rf data/source data/lake data/checkpoints
