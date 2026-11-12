# Container image for the Beam pipeline (also usable as a Dataflow Flex Template base).
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY pipeline/ pipeline/
COPY config/ config/
ENTRYPOINT ["uv", "run", "--no-sync", "python", "pipeline/fraud_pipeline.py"]
