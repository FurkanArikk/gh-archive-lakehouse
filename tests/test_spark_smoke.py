"""Toolchain check: local Spark starts, and Delta Lake writes and MERGEs work.

Silver relies on Delta MERGE, so this proves the image is ready for it before any real
transformation exists.
"""

from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark


def test_spark_matches_databricks_serverless(spark: SparkSession):
    assert spark.version == "4.2.0"


def test_delta_merge_inserts_only_new_keys(spark: SparkSession, tmp_path: Path):
    path = str(tmp_path / "events")
    initial = spark.createDataFrame([(1, "a"), (2, "b")], "id BIGINT, v STRING")
    initial.write.format("delta").save(path)

    updates = spark.createDataFrame([(2, "b-dup"), (3, "c")], "id BIGINT, v STRING")
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(updates.alias("u"), "t.id = u.id")
        .whenNotMatchedInsertAll()
        .execute()
    )

    rows = {r.id: r.v for r in spark.read.format("delta").load(path).collect()}
    assert rows == {1: "a", 2: "b", 3: "c"}
