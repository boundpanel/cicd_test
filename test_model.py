from __future__ import annotations

from datetime import datetime

import pytest

from models import RiskDataPipeline


def test_threshold_must_be_unit_interval() -> None:
    with pytest.raises(ValueError, match="risk_threshold"):
        RiskDataPipeline(risk_threshold=1.5)


def test_pipeline_is_callable(spark) -> None:
    mart = RiskDataPipeline(risk_threshold=0.75)
    rows = [
        ("c1", "ACTIVE", 0.9, datetime(2026, 1, 1)),
        ("c2", "ACTIVE", 0.2, datetime(2026, 1, 1)),
        ("c3", "INACTIVE", 0.99, datetime(2026, 1, 1)),
    ]
    source = spark.createDataFrame(
        rows,
        schema=["client_id", "status", "score", "updated_at"],
    )

    result = mart(source).orderBy("client_id").collect()

    assert [row.client_id for row in result] == ["c1", "c2"]
    assert result[0].is_high_risk is True
    assert result[1].is_high_risk is False
    assert set(result[0].asDict()) == {"client_id", "score", "is_high_risk", "updated_at"}
