"""Create or incrementally merge flattened event batches into a Delta table."""

from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

MERGE_CONDITION = "t.main_id = s.main_id"


def write_new_table(df: DataFrame, output_path: str) -> None:
    Path(output_path).mkdir(parents=True, exist_ok=True)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("date_key")
        .save(output_path)
    )


def merge_into_delta(
    spark: SparkSession, incoming: DataFrame, output_path: str
) -> dict[str, int | str]:
    """Insert new ``main_id`` rows and update existing ones.

    The first call creates the table. Later batches are upserted so future
    files can add columns (schema auto-merge) and correct earlier records.
    """
    incoming = incoming.dropDuplicates(["main_id"])
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    if not DeltaTable.isDeltaTable(spark, output_path):
        write_new_table(incoming, output_path)
        return {"mode": "create", "inserted": incoming.count(), "updated": 0}

    target = DeltaTable.forPath(spark, output_path)
    current = target.toDF()
    updated = current.join(incoming, on="main_id", how="inner").count()
    inserted = incoming.join(current, on="main_id", how="left_anti").count()

    (
        target.alias("t")
        .merge(incoming.alias("s"), MERGE_CONDITION)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"mode": "merge", "inserted": inserted, "updated": updated}
