from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from scrm_etl.job import RAW_SCHEMA, read_events
from scrm_etl.load import merge_into_delta
from scrm_etl.transform import transform_events

REPO_ROOT = Path(__file__).resolve().parents[1]


def _event(
    event_id: str,
    event_name: str,
    timestamp: int,
    devices: list[dict],
    customer_id: str = "93874923480",
    **extra,
) -> dict:
    row = {
        "sdk_version": "2.0.0",
        "event_timestamp": timestamp,
        "event_id": event_id,
        "event_name": event_name,
        "device": devices,
        "platform": devices[0]["platform"],
        "batch_ordering_id": None,
        "customer_id": customer_id,
    }
    row.update(extra)
    return row


def test_merge_inserts_new_rows_and_updates_existing(spark, tmp_path) -> None:
    output = str(tmp_path / "events")
    first = spark.createDataFrame(
        [
            _event(
                "evt-1",
                "search",
                1787721610000,
                [
                    {"platform": "android", "device_id": "dev-a"},
                    {"platform": "windows", "device_id": "dev-w"},
                ],
            )
        ],
        schema=RAW_SCHEMA,
    )
    created = merge_into_delta(spark, transform_events(first), output)
    assert created == {"mode": "create", "inserted": 2, "updated": 0}

    second = spark.createDataFrame(
        [
            _event(
                "evt-1",
                "search_corrected",
                1787721610000,
                [{"platform": "android", "device_id": "dev-a"}],
            ),
            _event(
                "evt-2",
                "log_in",
                1787721521591,
                [{"platform": "mac", "device_id": "dev-m"}],
            ),
        ],
        schema=RAW_SCHEMA,
    )
    stats = merge_into_delta(spark, transform_events(second), output)
    assert stats == {"mode": "merge", "inserted": 1, "updated": 1}

    table = spark.read.format("delta").load(output)
    by_id = {row.main_id: row for row in table.collect()}
    assert table.count() == 3
    assert by_id["evt-1|dev-a"].event_name == "search_corrected"
    assert by_id["evt-1|dev-w"].event_name == "search"
    assert by_id["evt-2|dev-m"].event_name == "log_in"


def test_merge_keeps_new_producer_columns(spark, tmp_path) -> None:
    output = str(tmp_path / "events")
    base = spark.createDataFrame(
        [
            _event(
                "evt-1",
                "search",
                1787721610000,
                [{"platform": "android", "device_id": "dev-a"}],
            )
        ],
        schema=RAW_SCHEMA,
    )
    merge_into_delta(spark, transform_events(base), output)

    richer_schema = StructType(
        list(RAW_SCHEMA.fields) + [StructField("campaign_id", StringType(), True)]
    )
    richer = spark.createDataFrame(
        [
            _event(
                "evt-1",
                "search",
                1787721610000,
                [{"platform": "android", "device_id": "dev-a"}],
                campaign_id="cmp-9",
            )
        ],
        schema=richer_schema,
    )
    merge_into_delta(spark, transform_events(richer), output)

    row = spark.read.format("delta").load(output).collect()[0]
    assert row.campaign_id == "cmp-9"


def test_real_json_batches_upsert_into_one_table(spark, tmp_path) -> None:
    output = str(tmp_path / "events")
    initial = REPO_ROOT / "initial_batch.json"
    second = REPO_ROOT / "second_batch.json"
    if not initial.exists():
        initial = Path("/data/initial_batch.json")
        second = Path("/data/second_batch.json")

    first = transform_events(read_events(spark, str(initial)))
    later = transform_events(read_events(spark, str(second)))
    merge_into_delta(spark, first, output)
    stats = merge_into_delta(spark, later, output)

    table = spark.read.format("delta").load(output)
    assert stats["mode"] == "merge"
    assert stats["inserted"] == later.count()
    assert stats["updated"] == 0
    assert table.count() == first.count() + later.count()
    assert table.select("event_id").distinct().count() == 8
    assert table.filter(F.col("event_name") == "log_in").count() == 3
    assert "null_field" in table.columns
