"""ETL transforms that flatten nested event batches onto a single grain."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType

# Composite primary key after exploding device[] onto one row per (event, device).
# event_id identifies the source event; device_id distinguishes each flattened row.
PRIMARY_KEY_FIELDS: tuple[str, str] = ("event_id", "device_id")

OUTPUT_COLUMNS: tuple[str, ...] = (
    "main_id",
    "event_id",
    "device_id",
    "event_timestamp",
    "date_key",
    "event_name",
    "sdk_version",
    "platform",
    "device_platform",
    "customer_id",
    "batch_ordering_id",
    "null_field",
)


def _flatten_device(df: DataFrame) -> DataFrame:
    if "device" not in df.columns:
        return (
            df.withColumn("device_id", F.lit(None).cast(StringType()))
            .withColumn("device_platform", F.lit(None).cast(StringType()))
        )
    return (
        df.withColumn("device", F.explode_outer("device"))
        .withColumn("device_id", F.col("device.device_id"))
        .withColumn("device_platform", F.col("device.platform"))
        .drop("device")
    )


def _flatten_remaining_structs(df: DataFrame) -> DataFrame:
    """Promote leftover struct fields so future nested attributes stay at root."""
    struct_fields = [field for field in df.schema.fields if isinstance(field.dataType, StructType)]
    for field in struct_fields:
        for nested in field.dataType.fields:
            col_name = f"{field.name}_{nested.name}"
            if col_name in df.columns:
                col_name = f"{field.name}__{nested.name}"
            df = df.withColumn(col_name, F.col(f"{field.name}.{nested.name}"))
        df = df.drop(field.name)
    return df


def transform_events(df: DataFrame) -> DataFrame:
    """Flatten, type, and key an event batch.

    Extra scalar columns from later producer versions are kept so future
    batches can add information without changing the job contract.
    """
    flattened = _flatten_remaining_structs(_flatten_device(df))

    keyed = (
        flattened.withColumn(
            "event_timestamp",
            (F.col("event_timestamp") / F.lit(1000)).cast("timestamp"),
        )
        .withColumn("date_key", F.date_format("event_timestamp", "yyyy-MM-dd"))
        .withColumn(
            "main_id",
            F.concat_ws(
                "|",
                F.coalesce(F.col("event_id"), F.lit("")),
                F.coalesce(F.col("device_id"), F.lit("")),
            ),
        )
        .withColumn("null_field", F.lit(None).cast(StringType()))
    )
    extras = [column for column in keyed.columns if column not in OUTPUT_COLUMNS]
    return keyed.select(*OUTPUT_COLUMNS, *extras)


def assert_flat_schema(df: DataFrame) -> None:
    """Raise if any column is still a struct or array."""
    nested = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StructType | ArrayType)
    ]
    if nested:
        raise ValueError(f"Transformed table still contains nested fields: {nested}")


def assert_unique_main_id(df: DataFrame) -> None:
    """Raise if ``main_id`` is not unique at the current grain."""
    duplicates = df.groupBy("main_id").count().filter(F.col("count") > 1)
    if duplicates.head(1):
        raise ValueError("main_id is not unique; check the primary-key combination")
