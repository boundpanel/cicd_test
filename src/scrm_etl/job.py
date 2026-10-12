"""CLI entrypoint: read JSON event batches and upsert them into Delta Lake."""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from scrm_etl.load import merge_into_delta, write_new_table
from scrm_etl.spark import create_spark_session
from scrm_etl.transform import assert_flat_schema, assert_unique_main_id, transform_events

LOGGER = logging.getLogger("scrm_etl")

RAW_SCHEMA = StructType(
    [
        StructField("sdk_version", StringType(), True),
        StructField("event_timestamp", LongType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField(
            "device",
            ArrayType(
                StructType(
                    [
                        StructField("platform", StringType(), True),
                        StructField("device_id", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("platform", StringType(), True),
        StructField("batch_ordering_id", StringType(), True),
        StructField("customer_id", StringType(), True),
    ]
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest event JSON batches to Delta")
    parser.add_argument(
        "--input",
        dest="inputs",
        action="append",
        help="Path to a source JSON array. Repeat for multiple batches in order.",
    )
    parser.add_argument(
        "--output",
        default="/output/events",
        help="Directory of the Delta table to create or update",
    )
    parser.add_argument(
        "--mode",
        choices=("merge", "overwrite"),
        default="merge",
        help="merge upserts into an existing table; overwrite rebuilds it first",
    )
    return parser.parse_args(argv)


def read_events(spark: SparkSession, input_path: str) -> DataFrame:
    """Read a JSON array, keeping known columns even when they are all-null.

    Extra fields present in a later producer version are preserved so they
    can be merged into the Delta table.
    """
    inferred = spark.read.option("multiLine", "true").json(input_path)
    extras = [field for field in inferred.schema.fields if field.name not in RAW_SCHEMA.names]
    schema = StructType(list(RAW_SCHEMA.fields) + extras)
    df = spark.read.schema(schema).option("multiLine", "true").json(input_path)
    return df.withColumn("event_timestamp", F.col("event_timestamp").cast("long"))


def prepare_batch(spark: SparkSession, input_path: str) -> DataFrame:
    transformed = transform_events(read_events(spark, input_path))
    assert_flat_schema(transformed)
    assert_unique_main_id(transformed)
    return transformed


def run(
    input_paths: list[str],
    output_path: str,
    mode: str = "merge",
    spark: SparkSession | None = None,
) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    owned_session = spark is None
    spark = spark or create_spark_session()
    try:
        for index, input_path in enumerate(input_paths):
            transformed = prepare_batch(spark, input_path)
            if mode == "overwrite" and index == 0:
                write_new_table(transformed, output_path)
                stats = {"mode": "overwrite", "inserted": transformed.count(), "updated": 0}
            else:
                stats = merge_into_delta(spark, transformed, output_path)
            LOGGER.info("Loaded %s into %s (%s)", input_path, output_path, stats)

        final = spark.read.format("delta").load(output_path)
        LOGGER.info("Table now has %s rows at %s", final.count(), output_path)
        final.printSchema()
        final.orderBy("event_timestamp", "main_id").show(truncate=False)
        return 0
    finally:
        if owned_session:
            spark.stop()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    inputs = args.inputs or ["/data/initial_batch.json"]
    return run(inputs, args.output, mode=args.mode)


if __name__ == "__main__":
    raise SystemExit(main())
