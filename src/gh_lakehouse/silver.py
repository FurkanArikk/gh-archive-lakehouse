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
from pyspark.sql.types import (
    ArrayType,
    DataType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

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


def _path(p: Column, dotted: str) -> Column:
    """``_path(p, "pull_request.head.sha")`` -> p.pull_request.head.sha"""
    col = p
    for part in dotted.split("."):
        col = col.getField(part)
    return col


def _ts(col: Column) -> Column:
    return F.try_to_timestamp(col, F.lit(GH_TIMESTAMP_FORMAT))


def _struct(**fields: DataType) -> StructType:
    return StructType([StructField(name, dtype) for name, dtype in fields.items()])


# SHA-1 (40 hex) or SHA-256 (64 hex): GitHub supports SHA-256 repositories, and one showed
# up on 2026-10-12, quarantined by the original 40-only rule.
_SHA = r"^([0-9a-f]{40}|[0-9a-f]{64})$"
_NAME = _struct(name=StringType())
_LOGIN = _struct(login=StringType())

# ---- WatchEvent: someone starred a repo.
WATCH = EventSpec(
    event_type="WatchEvent",
    table="watch_events",
    payload_schema=_struct(action=StringType()),
    # The only payload field is action, always "started"; it's validated, not stored.
    required=("action",),
    actions=frozenset({"started"}),
)

# ---- PushEvent: since the 2025 Events API change there are no commits or sizes, only refs.
PUSH = EventSpec(
    event_type="PushEvent",
    table="push_events",
    payload_schema=_struct(
        push_id=LongType(),
        ref=StringType(),
        head=StringType(),
        before=StringType(),
    ),
    columns=lambda p: {
        "push_id": p.push_id,
        "ref": p.ref,
        # refs/heads/main -> main; anything that isn't a branch ref stays null.
        "branch": F.when(
            p.ref.startswith("refs/heads/"), F.regexp_replace(p.ref, "^refs/heads/", "")
        ),
        "head_sha": p.head,
        "before_sha": p.before,
    },
    required=("push_id", "ref", "head"),
    checks=lambda p: {"invalid_head_sha": p.head.isNotNull() & ~p.head.rlike(_SHA)},
)

# ---- PullRequestEvent: the payload's pull_request now has only id, number, url, head, base,
# so timestamps (created_at, merged_at) come from the event's own created_at.
_PR_SIDE = _struct(
    ref=StringType(), sha=StringType(), repo=_struct(id=LongType(), name=StringType())
)
PULL_REQUEST = EventSpec(
    event_type="PullRequestEvent",
    table="pull_request_events",
    payload_schema=_struct(
        action=StringType(),
        number=IntegerType(),
        pull_request=_struct(id=LongType(), number=IntegerType(), head=_PR_SIDE, base=_PR_SIDE),
        label=_NAME,
        assignee=_LOGIN,
    ),
    columns=lambda p: {
        "action": p.action,
        "pr_id": p.pull_request.id,
        "pr_number": p.number,
        "head_ref": p.pull_request.head.ref,
        "head_sha": p.pull_request.head.sha,
        "head_repo_id": p.pull_request.head.repo.id,
        "base_ref": p.pull_request.base.ref,
        "base_sha": p.pull_request.base.sha,
        "base_repo_id": p.pull_request.base.repo.id,
        # Head in a different repo = PR from a fork. Null when the fork was deleted.
        "is_from_fork": p.pull_request.head.repo.id != p.pull_request.base.repo.id,
        "label_name": p.label.getField("name"),
        "assignee_login": p.assignee.login,
    },
    required=("action", "number", "pull_request.id"),
    # Set from a full day of real data (see docs). "merged" is new: merges used to be
    # "closed" with merged=true. Anything else is quarantined so new actions get noticed.
    actions=frozenset(
        {"opened", "closed", "reopened", "merged", "labeled", "unlabeled", "assigned", "unassigned"}
    ),
)

# ---- IssuesEvent: still carries the full issue, including its own timestamps.
ISSUES = EventSpec(
    event_type="IssuesEvent",
    table="issues_events",
    payload_schema=_struct(
        action=StringType(),
        issue=_struct(
            id=LongType(),
            number=IntegerType(),
            title=StringType(),
            state=StringType(),
            state_reason=StringType(),
            user=_struct(login=StringType(), type=StringType()),
            created_at=StringType(),
            closed_at=StringType(),
            comments=IntegerType(),
            labels=ArrayType(_NAME),
        ),
        label=_NAME,
    ),
    columns=lambda p: {
        "action": p.action,
        "issue_id": p.issue.id,
        "issue_number": p.issue.number,
        "title": p.issue.title,
        "state": p.issue.state,
        "state_reason": p.issue.state_reason,
        "author_login": p.issue.user.login,
        # The issue author's account type is in the payload (unlike the event actor's).
        "author_is_bot": p.issue.user.getField("type") == "Bot",
        "issue_created_at": _ts(p.issue.created_at),
        "issue_closed_at": _ts(p.issue.closed_at),
        "comment_count": p.issue.comments,
        "label_names": F.transform(p.issue.labels, lambda label: label.getField("name")),
        "label_name": p.label.getField("name"),
    },
    required=("action", "issue.id", "issue.number", "issue.created_at"),
    actions=frozenset(
        {"opened", "closed", "reopened", "labeled", "unlabeled", "assigned", "unassigned"}
    ),
    checks=lambda p: {
        "invalid_issue_created_at": p.issue.created_at.isNotNull()
        & _ts(p.issue.created_at).isNull(),
        "invalid_issue_closed_at": p.issue.closed_at.isNotNull() & _ts(p.issue.closed_at).isNull(),
        # Deliberately not checked: a "closed" event whose issue snapshot has no closed_at.
        # On 2026-10-06, 17 of 3,714 closes look like that: the snapshot still says
        # state=open, taken just before the close. The event is genuine, so it stays in
        # silver with issue_closed_at null; gold uses the event's created_at as close time.
    },
)

SPECS: tuple[EventSpec, ...] = (WATCH, PUSH, PULL_REQUEST, ISSUES)
MODELED_TYPES = frozenset(spec.event_type for spec in SPECS)


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
    df = df.withColumns(_envelope())
    if payload_columns := spec.columns(p):
        # Guarded: Spark Connect (serverless) rejects withColumns({}), local Spark allows it.
        df = df.withColumns(payload_columns)

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
    reasons += [
        _reason(_path(p, f).isNull(), f"missing_{f.replace('.', '_')}") for f in spec.required
    ]
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
    """Create ``table`` with ``df``'s schema unless it exists.

    mode("ignore") lets the server decide atomically, instead of asking
    catalog.tableExists() first: on serverless, foreachBatch runs in a cloned session whose
    catalog view can be stale.
    """
    df.limit(0).write.format("delta").mode("ignore").saveAsTable(table)


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


UNCLASSIFIED = "unclassified"


def unclassified_rows(bronze: DataFrame) -> DataFrame:
    """Corrupt bronze rows that no event-type stream will pick up.

    A corrupt row whose partially parsed ``event_type`` is a modelled type reaches that
    type's stream and is quarantined there. This catches the rest (type missing or not
    modelled), so "never drop records" holds for every corrupt line.
    """
    modeled = sorted(MODELED_TYPES)
    unmatched = F.col("event_type").isNull() | ~F.col("event_type").isin(*modeled)
    return (
        bronze.filter(F.col("_corrupt_record").isNotNull() & unmatched)
        .withColumn("_raw_event_id", F.col("event_id"))
        .withColumn(ERRORS, F.array(F.lit("corrupt_record")))
    )


def run_unclassified(spark: SparkSession, *, source: str, quarantine: str, checkpoint: str) -> None:
    def batch(df: DataFrame, batch_id: int) -> None:
        bad = quarantine_rows(unclassified_rows(df), UNCLASSIFIED)
        if not bad.isEmpty():
            append_quarantine(bad, quarantine, app_id=f"silver:{UNCLASSIFIED}", batch_id=batch_id)

    query = (
        spark.readStream.table(source)
        .writeStream.foreachBatch(batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def run_silver(
    spark: SparkSession, settings: Settings, specs: tuple[EventSpec, ...] = SPECS
) -> None:
    from gh_lakehouse.bronze import BRONZE_TABLE

    source = settings.bronze_table(BRONZE_TABLE)
    quarantine = settings.silver_table(QUARANTINE_TABLE)
    for spec in specs:
        run_spec(
            spark,
            spec,
            source=source,
            target=settings.silver_table(spec.table),
            quarantine=quarantine,
            checkpoint=f"{settings.checkpoint_path}/silver_{spec.table}",
        )
    run_unclassified(
        spark,
        source=source,
        quarantine=quarantine,
        checkpoint=f"{settings.checkpoint_path}/silver_{UNCLASSIFIED}",
    )
