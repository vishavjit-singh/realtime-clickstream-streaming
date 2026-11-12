# One image for every role in the stack: producer, Spark job and local engine.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    IVY_HOME=/opt/ivy

WORKDIR /app
COPY requirements.txt requirements-stream.txt ./
RUN pip install --no-cache-dir -r requirements-stream.txt

COPY config ./config
COPY src ./src

VOLUME ["/app/data"]
ENTRYPOINT ["python", "-m", "clickstream"]
CMD ["--help"]
