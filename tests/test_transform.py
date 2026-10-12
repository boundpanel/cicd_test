from __future__ import annotations

from pyspark.sql import Row
from pyspark.sql.types import ArrayType, StructType

from scrm_etl.job import RAW_SCHEMA
from scrm_etl.transform import (
    PRIMARY_KEY_FIELDS,
    assert_flat_schema,
    assert_unique_main_id,
    transform_events,
)


def _sample_rows():
    return [
        {
            "sdk_version": "2.0.0",
            "event_timestamp": 1787721610000,
            "event_id": "a10f3c82-1234-4a21-9876-0192837465ab",
            "event_name": "search",
            "device": [
                {"platform": "android", "device_id": "dev_and_99999"},
                {"platform": "windows", "device_id": "dev_win_88888"},
            ],
            "platform": "android",
            "batch_ordering_id": None,
            "customer_id": "93874923480",
        }
    ]


def test_primary_key_is_composite() -> None:
    assert PRIMARY_KEY_FIELDS == ("event_id", "device_id")


def test_timestamp_and_date_key(spark) -> None:
    raw = spark.createDataFrame(_sample_rows(), schema=RAW_SCHEMA)
    result = transform_events(raw).orderBy("device_id").collect()

    assert len(result) == 2
    assert result[0].event_timestamp.isoformat() == "2026-08-26T05:20:10"
    assert {row.date_key for row in result} == {"2026-08-26"}


def test_schema_is_fully_flattened(spark) -> None:
    raw = spark.createDataFrame(_sample_rows(), schema=RAW_SCHEMA)
    result = transform_events(raw)

    assert_flat_schema(result)
    nested = [
        field.name
        for field in result.schema.fields
        if isinstance(field.dataType, StructType | ArrayType)
    ]
    assert nested == []
    assert "device_platform" in result.columns
    assert "device_id" in result.columns
    assert "device" not in result.columns


def test_main_id_combines_primary_key_and_is_unique(spark) -> None:
    raw = spark.createDataFrame(_sample_rows(), schema=RAW_SCHEMA)
    result = transform_events(raw)

    assert_unique_main_id(result)
    rows = {row.device_id: row for row in result.collect()}
    assert rows["dev_and_99999"].main_id == "a10f3c82-1234-4a21-9876-0192837465ab|dev_and_99999"
    assert rows["dev_win_88888"].main_id == "a10f3c82-1234-4a21-9876-0192837465ab|dev_win_88888"


def test_null_field_is_always_null(spark) -> None:
    raw = spark.createDataFrame(_sample_rows(), schema=RAW_SCHEMA)
    result = transform_events(raw).collect()

    assert all(row.null_field is None for row in result)
    assert all(row.batch_ordering_id is None for row in result)


def test_original_event_attributes_are_preserved(spark) -> None:
    raw = spark.createDataFrame(_sample_rows(), schema=RAW_SCHEMA)
    android = (
        transform_events(raw).filter("device_id = 'dev_and_99999'").collect()[0]
    )

    assert android.event_name == "search"
    assert android.sdk_version == "2.0.0"
    assert android.platform == "android"
    assert android.device_platform == "android"
    assert android.customer_id == "93874923480"


def test_unique_main_id_rejects_duplicates(spark) -> None:
    df = spark.createDataFrame(
        [Row(main_id="dup"), Row(main_id="dup")],
    )
    try:
        assert_unique_main_id(df)
    except ValueError as exc:
        assert "not unique" in str(exc)
    else:
        raise AssertionError("expected duplicate main_id to fail")
