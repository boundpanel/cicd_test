"""Active-client risk mart."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from models.base import TableModel


class RiskDataPipeline(TableModel):
    """Filter active clients, flag high risk, and project mart columns.

    Column names are the table contract. Keep them here so notebooks and
    jobs do not scatter string literals.

    Example
    -------
    >>> mart = RiskDataPipeline(risk_threshold=0.75)
    >>> result = mart(source_df)
    """

    class Col:
        CLIENT_ID = "client_id"
        STATUS = "status"
        SCORE = "score"
        IS_HIGH_RISK = "is_high_risk"
        UPDATED_AT = "updated_at"
        ACTIVE_STATUS = "ACTIVE"

    def __init__(self, risk_threshold: float = 0.7) -> None:
        if not 0.0 <= risk_threshold <= 1.0:
            raise ValueError("risk_threshold must be between 0 and 1 inclusive")
        self.risk_threshold = float(risk_threshold)

    @staticmethod
    def _filter_active_clients(df: DataFrame) -> DataFrame:
        return df.filter(F.col(RiskDataPipeline.Col.STATUS) == RiskDataPipeline.Col.ACTIVE_STATUS)

    def _flag_high_risk(self, df: DataFrame) -> DataFrame:
        """Bound method: ``transform`` receives ``self`` without a closure."""
        return df.withColumn(
            self.Col.IS_HIGH_RISK,
            F.when(F.col(self.Col.SCORE) > self.risk_threshold, True).otherwise(False),
        )

    @staticmethod
    def _select_columns(df: DataFrame) -> DataFrame:
        col = RiskDataPipeline.Col
        return df.select(col.CLIENT_ID, col.SCORE, col.IS_HIGH_RISK, col.UPDATED_AT)

    def run(self, df: DataFrame) -> DataFrame:
        return (
            df.transform(self._filter_active_clients)
            .transform(self._flag_high_risk)
            .transform(self._select_columns)
        )
