"""Silver: typed, validated, deduplicated tables per event type, plus a quarantine table.

Each modelled event type is an ``EventSpec``: the payload fields we keep, which of them
are required, and which ``action`` values are expected. Every type runs as its own
incremental stream over bronze (own checkpoint), so a failure in one type doesn't block the
others, and a type added later automatically processes all of bronze's history.

Per micro-batch (``process_batch``):

1. ``parse_events``   cast the envelope, parse the payload with the type's schema, and
                      collect every validation failure into an ``_errors`` array
2. valid rows         deduplicate on event_id, then MERGE (insert-only) into the silver table
3. invalid rows       append to quarantine with the reasons and the raw payload

Events never change after GitHub emits them, so the MERGE only inserts ids it hasn't seen:
reruns and duplicate deliveries can't create duplicates.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from gh_lakehouse.config import Settings

ERRORS = "_errors"
QUARANTINE_TABLE = "quarantine"
GH_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"  # e.g. 2026-10-06T15:00:03Z

# Columns every silver table starts with, in order.
ENVELOPE_COLUMNS = [
    "event_id",
    "created_at",
    "event_date",
    "actor_id",
    "actor_login",
    "is_bot",
    "repo_id",
    "repo_name",
    "org_login",
]
LINEAGE_COLUMNS = ["_source_file", "_ingested_at", "_processed_at"]


@dataclass(frozen=True)
class EventSpec:
    """How one GitHub event type becomes one silver table."""

    event_type: str
    table: str
    payload_schema: StructType
    # Payload columns added after the envelope: name -> expression over the parsed struct `p`.
    columns: Callable[[Column], dict[str, Column]] = field(default=lambda p: {})
    # Payload fields (dotted paths into `p`) that must not be null.
    required: tuple[str, ...] = ()
    # Expected payload.action values; None means the type has no action.
    actions: frozenset[str] | None = None
    # Extra checks beyond required/actions: reason -> condition that is true when invalid.
    checks: Callable[[Column], dict[str, Column]] = field(default=lambda p: {})


WATCH = EventSpec(
    event_type="WatchEvent",
    table="watch_events",
    payload_schema=StructType([StructField("action", StringType())]),
    # The only payload field is action, always "started"; it's validated, not stored.
    required=("action",),
    actions=frozenset({"started"}),
)

SPECS: tuple[EventSpec, ...] = (WATCH,)


# --------------------------------------------------------------------------- transformations


def _envelope() -> dict[str, Column]:
    login = F.col("actor.login")
    created = F.try_to_timestamp(F.col("created_at"), F.lit(GH_TIMESTAMP_FORMAT))
    return {
        "event_id": F.col("event_id").try_cast("bigint"),
        "created_at": created,
        "event_date": F.to_date(created),
        "actor_id": F.col("actor.id"),
        "actor_login": login,
        # GitHub App accounts end in [bot]. Precise but a lower bound: some bots are plain
        # user accounts (e.g. LizardByte-bot). See docs in Phase 0.
        "is_bot": login.endswith("[bot]"),
        "repo_id": F.col("repo.id"),
        "repo_name": F.col("repo.name"),
        "org_login": F.col("org.login"),
    }


def _reason(condition: Column, reason: str | Column) -> Column:
    return F.when(condition, reason if isinstance(reason, Column) else F.lit(reason))


def parse_events(bronze: DataFrame, spec: EventSpec) -> DataFrame:
    """Typed columns for ``spec`` plus ``_errors`` (empty array = valid).

    Keeps the raw bronze columns that quarantine needs. Expects bronze rows of
    ``spec.event_type`` only.
    """
    p = F.col("_p")
    df = bronze.withColumns(
        {
            # Keep the id as received: quarantine must show "abc", not the failed cast's null.
            "_raw_event_id": F.col("event_id"),
            "_p": F.from_json(F.col("payload"), spec.payload_schema),
        }
    )
    df = df.withColumns(_envelope()).withColumns(spec.columns(p))

    reasons = [
        # Spark keeps fields parsed before a JSON error, so check this first and
        # don't trust the other values of a corrupt row.
        _reason(F.col("_corrupt_record").isNotNull(), "corrupt_record"),
        _reason(F.col("event_id").isNull(), "invalid_event_id"),
        _reason(F.col("created_at").isNull(), "invalid_created_at"),
        _reason(F.col("actor_id").isNull(), "missing_actor_id"),
        _reason(F.col("repo_id").isNull(), "missing_repo_id"),
        _reason(F.col("payload").isNull() | p.isNull(), "invalid_payload"),
    ]
    reasons += [_reason(p.getField(f).isNull(), f"missing_{f}") for f in spec.required]
    if spec.actions is not None:
        action = p.getField("action")
        allowed = F.lit(sorted(spec.actions))
        reasons.append(
            _reason(
                action.isNotNull() & ~F.array_contains(allowed, action),
                F.concat(F.lit("unexpected_action:"), action),
            )
        )
    reasons += [_reason(cond, name) for name, cond in spec.checks(p).items()]

    return df.withColumn(ERRORS, F.array_compact(F.array(*reasons))).drop("_p")


def silver_columns(spec: EventSpec) -> list[str]:
    payload_cols = list(spec.columns(F.col("_p")))
    return [*ENVELOPE_COLUMNS, *payload_cols, *LINEAGE_COLUMNS]


def valid_rows(parsed: DataFrame, spec: EventSpec) -> DataFrame:
    """Rows with no errors, in the silver table's shape, one row per event_id."""
    valid = parsed.filter(F.size(ERRORS) == 0).withColumn("_processed_at", F.current_timestamp())
    return dedupe(valid).select(*silver_columns(spec))


def dedupe(df: DataFrame) -> DataFrame:
    """Keep one row per event_id: the first ingested (ties broken by source file)."""
    w = Window.partitionBy("event_id").orderBy("_ingested_at", "_source_file")
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def quarantine_rows(parsed: DataFrame, target_table: str) -> DataFrame:
    """Rows with errors, keeping the raw values needed to investigate or replay them."""
    return parsed.filter(F.size(ERRORS) > 0).select(
        F.col("_raw_event_id").alias("event_id"),
        F.col("event_type"),
        F.lit(target_table).alias("target_table"),
        F.col(ERRORS).alias("reasons"),
        F.col("payload"),
        F.col("_corrupt_record").alias("raw_record"),
        "_source_file",
        "_ingested_at",
        F.current_timestamp().alias("_quarantined_at"),
    )


# --------------------------------------------------------------------------- writes


def _ensure_table(df: DataFrame, table: str) -> None:
    spark = df.sparkSession
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)


def merge_new_events(df: DataFrame, table: str) -> None:
    """Insert rows whose event_id isn't in ``table`` yet. ``df`` must be unique on event_id."""
    _ensure_table(df, table)
    view = f"_silver_batch_{table.replace('.', '_')}"
    df.createOrReplaceTempView(view)
    # SQL MERGE rather than the DeltaTable API: identical locally and on serverless
    # (Spark Connect), and readable in the query history.
    df.sparkSession.sql(
        f"""
        MERGE INTO {table} AS t
        USING {view} AS s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def append_quarantine(df: DataFrame, table: str, app_id: str, batch_id: int) -> None:
    """Append idempotently: Delta skips a (txnAppId, txnVersion) pair it has already committed,
    so a retried micro-batch can't quarantine the same rows twice."""
    _ensure_table(df, table)
    (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .saveAsTable(table)
    )


def process_batch(
    bronze_batch: DataFrame, batch_id: int, *, spec: EventSpec, target: str, quarantine: str
) -> None:
    # No persist(): serverless doesn't support caching. `parsed` is evaluated twice
    # (valid + quarantine), which is cheap at this volume.
    parsed = parse_events(bronze_batch, spec)
    merge_new_events(valid_rows(parsed, spec), target)
    bad = quarantine_rows(parsed, target)
    if not bad.isEmpty():
        append_quarantine(bad, quarantine, app_id=f"silver:{target}", batch_id=batch_id)


def run_spec(
    spark: SparkSession,
    spec: EventSpec,
    *,
    source: str,
    target: str,
    quarantine: str,
    checkpoint: str,
) -> None:
    """Process bronze rows of ``spec.event_type`` added since the last run, then stop."""
    stream = spark.readStream.table(source).filter(F.col("event_type") == spec.event_type)
    query = (
        stream.writeStream.foreachBatch(
            lambda df, batch_id: process_batch(
                df, batch_id, spec=spec, target=target, quarantine=quarantine
            )
        )
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def run_silver(
    spark: SparkSession, settings: Settings, specs: tuple[EventSpec, ...] = SPECS
) -> None:
    from gh_lakehouse.bronze import BRONZE_TABLE

    for spec in specs:
        run_spec(
            spark,
            spec,
            source=settings.bronze_table(BRONZE_TABLE),
            target=settings.silver_table(spec.table),
            quarantine=settings.silver_table(QUARANTINE_TABLE),
            checkpoint=f"{settings.checkpoint_path}/silver_{spec.table}",
        )
