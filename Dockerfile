FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=1.8.5 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    SPARK_MASTER=local[*] \
    SPARK_UI_ENABLED=false \
    SPARK_DRIVER_MEMORY=1g \
    JAVA_HOME=/usr/lib/jvm/default-java

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && JAVA_BIN="$(readlink -f "$(command -v java)")" \
    && ln -sfn "$(dirname "$(dirname "$JAVA_BIN")")" "${JAVA_HOME}" \
    && pip install --no-cache-dir "poetry==${POETRY_VERSION}"

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY tests ./tests
COPY initial_batch.json second_batch.json /data/

RUN poetry install --no-ansi --no-root \
    && poetry install --no-ansi \
    && python -c "from scrm_etl.spark import create_spark_session; spark = create_spark_session(); spark.stop()"

WORKDIR /app
ENTRYPOINT ["python", "-m", "scrm_etl"]
CMD ["--input", "/data/initial_batch.json", "--output", "/output/events"]
