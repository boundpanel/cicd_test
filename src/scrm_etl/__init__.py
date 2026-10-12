"""Base PySpark ETL package for flattening event batches into Delta Lake."""

from scrm_etl.load import merge_into_delta
from scrm_etl.transform import PRIMARY_KEY_FIELDS, transform_events

__all__ = ["PRIMARY_KEY_FIELDS", "merge_into_delta", "transform_events"]
