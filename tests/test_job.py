"""Tests for the ETL script: CLI parsing and end-to-end Delta writes."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql.types import ArrayType, StructType

from scrm_etl.job import parse_args, run


def _batch_path(name: str) -> str:
    repo_copy = Path(__file__).resolve().parents[1] / name
    if repo_copy.exists():
        return str(repo_copy)
    return f"/data/{name}"


def test_parse_args_accepts_multiple_future_batches() -> None:
    args = parse_args(
        [
            "--input",
            "initial_batch.json",
            "--input",
            "second_batch.json",
            "--output",
            "/tmp/events",
            "--mode",
            "merge",
        ]
    )

    assert args.inputs == ["initial_batch.json", "second_batch.json"]
    assert args.output == "/tmp/events"
    assert args.mode == "merge"


def test_script_writes_flat_delta_table_from_initial_batch(spark, tmp_path) -> None:
    output = str(tmp_path / "events")
    exit_code = run(
        [_batch_path("initial_batch.json")],
        output,
        mode="overwrite",
        spark=spark,
    )
    table = spark.read.format("delta").load(output)
    nested = [
        field.name
        for field in table.schema.fields
        if isinstance(field.dataType, StructType | ArrayType)
    ]

    assert exit_code == 0
    assert table.count() == 11
    assert nested == []
    assert table.select("main_id").distinct().count() == 11
    date_keys = {row.date_key for row in table.select("date_key").distinct().collect()}
    assert date_keys == {"2026-08-26"}
    assert all(row.null_field is None for row in table.select("null_field").collect())
    search = table.filter("event_name = 'search'").collect()
    assert {row.device_id for row in search} == {"dev_and_99999", "dev_win_88888"}
    assert search[0].event_timestamp.isoformat().startswith("2026-08-26T05:20:10")


def test_script_merges_second_batch_into_existing_table(spark, tmp_path) -> None:
    output = str(tmp_path / "events")
    first = run(
        [_batch_path("initial_batch.json")],
        output,
        mode="overwrite",
        spark=spark,
    )
    second = run(
        [_batch_path("second_batch.json")],
        output,
        mode="merge",
        spark=spark,
    )
    table = spark.read.format("delta").load(output)
    names = {row.event_name for row in table.select("event_name").distinct().collect()}

    assert first == 0
    assert second == 0
    assert table.count() == 18
    assert {"search", "select_item", "begin_checkout", "purchase", "log_out"} <= names
    assert {"log_in", "view_item", "add_to_cart"} <= names
    login_id = "cb692f71-ea67-465d-89a3-31a8f4cd505a"
    assert table.filter(f"event_id = '{login_id}'").count() == 3
