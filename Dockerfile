FROM python:3.11-slim as builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

FROM python:3.11-slim as runtime

WORKDIR /app

# Non-root security user
RUN useradd -m -u 1000 appuser

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/pulsequeue /usr/local/bin/pulsequeue
COPY examples/ ./examples/
COPY pyproject.toml ./

USER appuser

ENTRYPOINT ["pulsequeue"]
CMD ["worker", "--broker", "redis://redis:6379/0", "--concurrency", "10"]
