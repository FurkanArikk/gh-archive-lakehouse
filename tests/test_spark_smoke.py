"""Toolchain check: local Spark starts, and Delta Lake writes and MERGEs work.

Uses SQL MERGE, like the silver layer: the DeltaTable Python API needs an extra server
plugin under Spark Connect, SQL works the same in both modes.
"""

import pytest
from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark


def test_spark_matches_databricks_serverless(spark: SparkSession):
    assert spark.version == "4.2.0"


def test_delta_merge_inserts_only_new_keys(spark: SparkSession):
    spark.sql("DROP TABLE IF EXISTS smoke_events")
    initial = spark.createDataFrame([(1, "a"), (2, "b")], "id BIGINT, v STRING")
    initial.write.format("delta").saveAsTable("smoke_events")

    updates = spark.createDataFrame([(2, "b-dup"), (3, "c")], "id BIGINT, v STRING")
    updates.createOrReplaceTempView("smoke_updates")
    spark.sql(
        """
        MERGE INTO smoke_events AS t USING smoke_updates AS u ON t.id = u.id
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    rows = {r.id: r.v for r in spark.table("smoke_events").collect()}
    assert rows == {1: "a", 2: "b", 3: "c"}
