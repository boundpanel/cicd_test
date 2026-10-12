# scrm-etl

Base PySpark job that standardizes event-batch ingestion into Delta Lake.

## What the job does

1. Reads `initial_batch.json` (JSON array, multiline).
2. Converts `event_timestamp` from epoch milliseconds to a UTC timestamp.
3. Adds `date_key` as `YYYY-MM-DD` from that timestamp.
4. Explodes `device[]` so the table has **no structs** and every field sits at the root.
5. Writes a `main_id` that concatenates the primary-key fields.
6. Adds an explicit `null_field` column (always null).
7. Creates the Delta table on the first batch, then **merges** later files on `main_id` (update matches, insert new rows). New producer columns are kept via Delta schema auto-merge.

## Proposed primary key

`(event_id, device_id)`

- `event_id` is the natural identifier of the source event.
- After flattening `device[]`, one event becomes one row per device, so `device_id` is required to keep the grain unique.
- `main_id` is `{event_id}|{device_id}`.

`platform` stays as the event-level platform. The exploded device platform is stored as `device_platform`.

## Project layout

```
src/scrm_etl/     # Poetry package (Spark session, transforms, CLI)
tests/            # pytest coverage of the ETL rules
Dockerfile        # Java 17 + Poetry + PySpark + Delta
docker-compose.yml
initial_batch.json
```

## Docker (recommended)

```bash
docker compose build
docker compose run --rm etl --mode overwrite --input /data/initial_batch.json
docker compose run --rm etl --input /data/second_batch.json
docker compose run --rm --entrypoint pytest etl
docker compose run --rm --entrypoint ruff etl check src tests
```

Or load both batches in one run (first file creates/overwrites, the rest merge):

```bash
docker compose run --rm etl --mode overwrite \
  --input /data/initial_batch.json \
  --input /data/second_batch.json
```

The job writes `./output/events` on the host. Default `--mode` is `merge`, so future files upsert into the same table.

## Local Poetry (optional)

```bash
poetry install
poetry run pytest
poetry run ruff check src tests
poetry run python -m scrm_etl --input initial_batch.json --output output/events
```

Requires Python 3.11+ and a JVM (Java 17).

## CI/CD (Azure DevOps)

`azure-pipelines.yml` is a DataOps promotion pipeline. It never rebuilds between environments: tests gate the build, one wheel is published, then that same artifact is deployed to DEV and promoted to PROD.

| Stage | What it does |
| --- | --- |
| **Test Execution** | Installs Python 3.11 + Java 17 + Poetry, runs `ruff`, then `pytest`. Publishes JUnit results. |
| **Package / Build** | Stamps `0.1.$(Build.BuildId)`, runs `poetry build`, publishes the wheel plus env configs. |
| **Deploy DEV** | Installs the published wheel into a simulated DEV release (`scrm_dev.events`, 7-day retention). |
| **Deploy PROD** | Promotes the **same** wheel to PROD (`scrm_prod.events`, 90-day retention). |

Environment differences live in `config/environments/{dev,prod}.yaml`. The release step is `scripts/deploy.py`.

In Azure DevOps:

1. Create a pipeline from `azure-pipelines.yml`.
2. Create environments named `DEV` and `PROD` (the first run can also create them).
3. Add an approval check on `PROD` so production needs a reviewer after DEV succeeds.
# cicd_test
