import gzip
from collections.abc import Iterator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """One local Spark session with Delta Lake for the whole test run.

    Starting a JVM takes several seconds, so it's session-scoped. Settings are tuned for
    tiny fixtures: 2 shuffle partitions instead of 200, and no UI.
    """
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("gh-lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # GH Archive timestamps are UTC; Databricks serverless sessions default to UTC too.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def make_bronze(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory):
    """Turn GH Archive JSON lines into a bronze DataFrame via the real bronze transformation."""
    from gh_lakehouse.bronze import CORRUPT_RECORD, RAW_SCHEMA, to_bronze

    def build(lines: list[str]) -> DataFrame:
        root = tmp_path_factory.mktemp("landing")
        path = root / "gharchive" / "date=2026-10-06" / "2026-10-06-15.json.gz"
        path.parent.mkdir(parents=True)
        path.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))
        raw = (
            spark.read.schema(RAW_SCHEMA)
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
            .json(str(root / "gharchive"))
        )
        # Materialize: Spark refuses some queries that touch only _corrupt_record on raw JSON.
        return spark.createDataFrame(to_bronze(raw).collect(), to_bronze(raw).schema)

    return build
