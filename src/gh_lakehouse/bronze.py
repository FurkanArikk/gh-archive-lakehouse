"""Bronze: GH Archive JSON lines from the landing volume into one Delta table.

Bronze keeps the data as close to the source as possible:

- The top-level envelope (id, type, actor, repo, org, public, created_at) is read with an
  explicit schema. It has been stable since at least 2024.
- ``payload`` is declared as a string. When Spark's JSON reader meets an object where the
  schema says string, it keeps the object's JSON text, so every event type's payload
  survives unchanged no matter how GitHub reshapes it. Parsing happens in silver.
- ``id`` and ``created_at`` stay strings. Casting is silver's job, so a bad value goes to
  quarantine instead of silently becoming null here.
- Malformed lines are kept: the reader puts the raw line in ``_corrupt_record`` and leaves
  the other columns null.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import StreamingQuery

    from gh_lakehouse.config import Settings

CORRUPT_RECORD = "_corrupt_record"
RESCUED_DATA = "_rescued_data"
BRONZE_TABLE = "events"
CHECKPOINT_NAME = "bronze_events"

_ACTOR = StructType(
    [
        StructField("id", LongType()),
        StructField("login", StringType()),
        StructField("display_login", StringType()),
        StructField("gravatar_id", StringType()),
        StructField("url", StringType()),
        StructField("avatar_url", StringType()),
    ]
)
_REPO = StructType(
    [
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("url", StringType()),
    ]
)
_ORG = StructType(
    [
        StructField("id", LongType()),
        StructField("login", StringType()),
        StructField("gravatar_id", StringType()),
        StructField("url", StringType()),
        StructField("avatar_url", StringType()),
    ]
)

# Schema applied when reading the landing files.
RAW_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("actor", _ACTOR),
        StructField("repo", _REPO),
        StructField("org", _ORG),
        StructField("public", BooleanType()),
        StructField("created_at", StringType()),
        StructField("payload", StringType()),
        StructField(CORRUPT_RECORD, StringType()),
    ]
)


def to_bronze(raw: DataFrame) -> DataFrame:
    """Rename envelope columns and add lineage metadata.

    ``raw`` must come straight from a file source (batch or streaming) so that Spark's
    hidden ``_metadata`` column is available.
    """
    rescued: Column = (
        F.col(RESCUED_DATA) if RESCUED_DATA in raw.columns else F.lit(None).cast(StringType())
    )
    return raw.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        "created_at",
        "actor",
        "repo",
        "org",
        "public",
        "payload",
        F.col(CORRUPT_RECORD),
        # Only Auto Loader produces this column (unknown top-level fields land here).
        rescued.alias(RESCUED_DATA),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )


def read_landing_stream(spark: SparkSession, path: str, *, auto_loader: bool = True) -> DataFrame:
    """Stream new files under ``path``.

    On Databricks this is Auto Loader (``cloudFiles``), which tracks discovered files in
    the checkpoint and scales to millions of files. Open-source Spark has no Auto Loader,
    so local tests use the plain JSON file source; it gives the same exactly-once,
    new-files-only behaviour for the volumes in this project.
    """
    reader = spark.readStream.schema(RAW_SCHEMA).option("columnNameOfCorruptRecord", CORRUPT_RECORD)
    if auto_loader:
        reader = (
            reader.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("rescuedDataColumn", RESCUED_DATA)
        )
    else:
        reader = reader.format("json")
    return reader.load(path)


def write_bronze(df: DataFrame, table: str, checkpoint: str) -> StreamingQuery:
    """Append to the bronze Delta table, processing everything new and then stopping.

    ``availableNow`` makes a stream behave like an incremental batch job: it processes
    whatever arrived since the last run (tracked in the checkpoint) and stops, so we pay
    only for the work there is. Serverless compute supports only this kind of trigger.
    """
    return (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(table)
    )


def run_bronze(spark: SparkSession, settings: Settings) -> int:
    """Load new landing files into bronze. Returns the number of rows appended."""
    source = f"{settings.landing_path}/gharchive/"
    raw = read_landing_stream(spark, source)
    query = write_bronze(
        to_bronze(raw),
        settings.bronze_table(BRONZE_TABLE),
        f"{settings.checkpoint_path}/{CHECKPOINT_NAME}",
    )
    query.awaitTermination()
    return input_rows(query.recentProgress)


def input_rows(progress: list[dict[str, Any]]) -> int:
    """Total rows read across a query's micro-batches.

    On serverless (Spark Connect) a batch that found no new data can report
    ``numInputRows`` as None rather than 0.
    """
    return sum(p.get("numInputRows") or 0 for p in progress)
