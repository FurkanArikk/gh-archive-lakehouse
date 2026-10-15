"""Gold: daily aggregates for the dashboard, rebuilt only for the days that got new data.

Incremental strategy:

- ``pipeline_state`` (one row) stores ``processed_until``: the highest silver
  ``_processed_at`` that gold has already consumed.
- A run finds the event dates of silver rows processed after that mark ("pending dates"),
  recomputes every gold table for exactly those dates, and replaces them with Delta's
  ``replaceWhere``. Old days are never rebuilt; reruns produce the same result.
- The mark only moves after all tables are written, so a failed run is simply redone.

Two tables reach beyond the pending dates, because their values depend on other days:

- ``repo_star_growth.cumulative_stars`` changes for every later day, so it is rebuilt from
  the earliest pending date onwards.
- ``pr_time_to_merge`` rows live on the merge date, but the matching ``opened`` event can be
  on an earlier day. When a PR gets any new event, its merge date is rebuilt too.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DateType, StructField, StructType, TimestampType

from gh_lakehouse.silver import ISSUES, PULL_REQUEST, PUSH, WATCH

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from gh_lakehouse.config import Settings

REPO_DAILY_ACTIVITY = "repo_daily_activity"
REPO_STAR_GROWTH = "repo_star_growth"
PR_TIME_TO_MERGE = "pr_time_to_merge"
ISSUE_TIME_TO_CLOSE = "issue_time_to_close"
DAILY_BOT_SHARE = "daily_bot_share"
HOURLY_EVENT_VOLUME = "hourly_event_volume"
STATE_TABLE = "pipeline_state"
GOLD_TABLES = (
    REPO_DAILY_ACTIVITY,
    REPO_STAR_GROWTH,
    PR_TIME_TO_MERGE,
    ISSUE_TIME_TO_CLOSE,
    DAILY_BOT_SHARE,
    HOURLY_EVENT_VOLUME,
)

PR_KEY = ["repo_id", "pr_number"]

STATE_SCHEMA = StructType(
    [
        StructField("processed_until", TimestampType()),
        StructField("last_dates", ArrayType(DateType())),
        StructField("updated_at", TimestampType()),
    ]
)


@dataclass(frozen=True)
class Silver:
    watch: DataFrame
    push: DataFrame
    pull_requests: DataFrame
    issues: DataFrame

    def all(self) -> list[DataFrame]:
        return [self.watch, self.push, self.pull_requests, self.issues]

    def on_dates(self, dates: list[date]) -> Silver:
        keep = F.col("event_date").isin(dates)
        return Silver(*(df.filter(keep) for df in self.all()))


def load_silver(spark: SparkSession, settings: Settings) -> Silver:
    return Silver(
        *(spark.table(settings.silver_table(s.table)) for s in (WATCH, PUSH, PULL_REQUEST, ISSUES))
    )


# --------------------------------------------------------------------------- transformations


def activity_events(silver: Silver) -> DataFrame:
    """The four silver tables as one stream of events with the columns gold needs."""

    def pick(df: DataFrame, event_type: str, action: Column) -> DataFrame:
        return df.select(
            "event_id",
            "event_date",
            "created_at",
            "repo_id",
            "repo_name",
            "actor_id",
            "is_bot",
            F.lit(event_type).alias("event_type"),
            action.alias("action"),
        )

    no_action = F.lit(None).cast("string")
    return (
        pick(silver.watch, WATCH.event_type, no_action)
        .unionByName(pick(silver.push, PUSH.event_type, no_action))
        .unionByName(pick(silver.pull_requests, PULL_REQUEST.event_type, F.col("action")))
        .unionByName(pick(silver.issues, ISSUES.event_type, F.col("action")))
    )


def repo_daily_activity(events: DataFrame) -> DataFrame:
    t, a = F.col("event_type"), F.col("action")
    return (
        events.groupBy("event_date", "repo_id")
        .agg(
            # A repo can be renamed mid-day; report its latest name.
            F.max_by("repo_name", "created_at").alias("repo_name"),
            F.count_if(t == PUSH.event_type).alias("pushes"),
            F.count_if((t == PULL_REQUEST.event_type) & (a == "opened")).alias("prs_opened"),
            F.count_if((t == PULL_REQUEST.event_type) & (a == "merged")).alias("prs_merged"),
            F.count_if((t == ISSUES.event_type) & (a == "opened")).alias("issues_opened"),
            F.count_if((t == ISSUES.event_type) & (a == "closed")).alias("issues_closed"),
            F.count_if(t == WATCH.event_type).alias("stars"),
            F.count("*").alias("events"),
            F.count_distinct("actor_id").alias("active_actors"),
            F.count_if("is_bot").alias("bot_events"),
        )
        .withColumn("bot_event_share", F.col("bot_events") / F.col("events"))
    )


def daily_bot_share(events: DataFrame) -> DataFrame:
    return (
        events.groupBy("event_date", "event_type")
        .agg(F.count("*").alias("events"), F.count_if("is_bot").alias("bot_events"))
        .withColumn("bot_share", F.col("bot_events") / F.col("events"))
    )


def hourly_event_volume(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("hour", F.hour("created_at"))
        .groupBy("event_date", "hour", "event_type")
        .agg(F.count("*").alias("events"), F.count_if("is_bot").alias("bot_events"))
    )


def repo_star_growth(watch: DataFrame) -> DataFrame:
    """Stars per repo and day, the running total since ingestion started, and daily rank.

    Needs all of watch_events for correct running totals; filter the result, not the input.
    """
    daily = watch.groupBy("event_date", "repo_id").agg(
        F.count("*").alias("stars"), F.max_by("repo_name", "created_at").alias("repo_name")
    )
    running = (
        Window.partitionBy("repo_id")
        .orderBy("event_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    by_stars = Window.partitionBy("event_date").orderBy(F.desc("stars"))
    return daily.withColumns(
        {
            "cumulative_stars": F.sum("stars").over(running),
            # Ties share a rank (rank, not row_number).
            "star_rank": F.rank().over(by_stars),
        }
    )


def pr_time_to_merge(pull_requests: DataFrame) -> DataFrame:
    """One row per merged PR, on its merge date.

    The PR payload has no timestamps any more, so time-to-merge needs the PR's ``opened``
    event to be ingested too. When it isn't (opened before ingestion started), opened_at and
    hours_to_merge are null; the dashboard reports that share as coverage.
    """
    action = F.col("action")
    opened = (
        pull_requests.filter(action == "opened")
        .groupBy(PR_KEY)
        .agg(
            F.min("created_at").alias("opened_at"),
            F.min_by("is_bot", "created_at").alias("opened_by_bot"),
        )
    )
    merged = (
        pull_requests.filter(action == "merged")
        .groupBy(PR_KEY)
        .agg(
            F.min("created_at").alias("merged_at"),
            F.max_by("repo_name", "created_at").alias("repo_name"),
            F.min_by("is_bot", "created_at").alias("merged_by_bot"),
        )
    )
    seconds = F.unix_timestamp("merged_at") - F.unix_timestamp("opened_at")
    return (
        merged.join(opened, PR_KEY, "left")
        .withColumns(
            {
                "event_date": F.to_date("merged_at"),
                "hours_to_merge": F.when(seconds >= 0, seconds / 3600),
            }
        )
        .select(
            "event_date",
            "repo_id",
            "repo_name",
            "pr_number",
            "opened_at",
            "merged_at",
            "hours_to_merge",
            "opened_by_bot",
            "merged_by_bot",
        )
    )


def issue_time_to_close(issues: DataFrame) -> DataFrame:
    """One row per ``closed`` event (an issue can be closed, reopened and closed again).

    About 0.5% of close events carry an issue snapshot taken just before the close, with no
    closed_at; the event's own timestamp is used for those, flagged by closed_at_from_event.
    """
    closed_at = F.coalesce("issue_closed_at", "created_at")
    seconds = F.unix_timestamp(closed_at) - F.unix_timestamp("issue_created_at")
    return issues.filter(F.col("action") == "closed").select(
        "event_date",
        "event_id",
        "repo_id",
        "repo_name",
        "issue_id",
        "issue_number",
        "issue_created_at",
        closed_at.alias("closed_at"),
        F.col("issue_closed_at").isNull().alias("closed_at_from_event"),
        (seconds / 86400).alias("days_to_close"),
        "state_reason",
        "author_is_bot",
    )


# --------------------------------------------------------------------------- incremental run


@dataclass(frozen=True)
class GoldPlan:
    """What a run will rebuild: per table, the rows and the predicate they replace."""

    dates: list[date]
    processed_until: datetime | None
    tables: dict[str, tuple[DataFrame, str]]


def _dates_predicate(dates: list[date]) -> str:
    return "event_date IN (" + ", ".join(f"DATE'{d.isoformat()}'" for d in dates) + ")"


def read_state(spark: SparkSession, table: str) -> tuple[datetime | None, list[date]]:
    try:
        row = spark.table(table).first()
    except AnalysisException:  # first run: no state yet
        return None, []
    return (row.processed_until, list(row.last_dates or [])) if row else (None, [])


def write_state(
    spark: SparkSession, table: str, processed_until: datetime, dates: list[date]
) -> None:
    row = [(processed_until, dates, datetime.now())]
    spark.createDataFrame(row, STATE_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(
        table
    )


def pending(silver: Silver, processed_until: datetime | None) -> tuple[list[date], datetime | None]:
    """Event dates of silver rows newer than ``processed_until``, and the new high mark."""
    rows = F.lit(True) if processed_until is None else F.col("_processed_at") > processed_until
    new = None
    for df in silver.all():
        part = df.filter(rows).select("event_date", "_processed_at")
        new = part if new is None else new.unionByName(part)
    assert new is not None
    result = new.agg(
        F.array_sort(F.collect_set("event_date")).alias("dates"),
        F.max("_processed_at").alias("until"),
    ).first()
    return list(result.dates or []), result.until


def plan_gold(silver: Silver, dates: list[date], processed_until: datetime | None) -> GoldPlan:
    """Build every gold table for ``dates`` (plus the wider ranges explained above)."""
    day = _dates_predicate(dates)
    events = activity_events(silver.on_dates(dates))

    # PRs with any event on a pending date may have their merge on another day.
    touched_prs = silver.pull_requests.filter(F.col("event_date").isin(dates)).select(PR_KEY)
    merge_dates = [
        r.event_date
        for r in silver.pull_requests.filter(F.col("action") == "merged")
        .join(touched_prs.distinct(), PR_KEY, "left_semi")
        .select("event_date")
        .distinct()
        .collect()
    ]
    pr_dates = sorted(set(dates) | set(merge_dates))

    since = f"event_date >= DATE'{min(dates).isoformat()}'"
    return GoldPlan(
        dates=dates,
        processed_until=processed_until,
        tables={
            REPO_DAILY_ACTIVITY: (repo_daily_activity(events), day),
            DAILY_BOT_SHARE: (daily_bot_share(events), day),
            HOURLY_EVENT_VOLUME: (hourly_event_volume(events), day),
            ISSUE_TIME_TO_CLOSE: (issue_time_to_close(silver.on_dates(dates).issues), day),
            REPO_STAR_GROWTH: (repo_star_growth(silver.watch).filter(since), since),
            PR_TIME_TO_MERGE: (
                pr_time_to_merge(silver.pull_requests).filter(F.col("event_date").isin(pr_dates)),
                _dates_predicate(pr_dates),
            ),
        },
    )


def replace_rows(df: DataFrame, table: str, predicate: str) -> None:
    """Atomically replace the rows matching ``predicate`` with ``df`` (Delta replaceWhere)."""
    df.limit(0).write.format("delta").mode("ignore").saveAsTable(table)  # create if missing
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(table)
    )


def run_gold(spark: SparkSession, settings: Settings) -> GoldPlan | None:
    """Rebuild gold for the dates silver has new rows for. Returns None if there are none."""
    state = settings.gold_table(STATE_TABLE)
    processed_until, _ = read_state(spark, state)
    silver = load_silver(spark, settings)
    dates, new_until = pending(silver, processed_until)
    if not dates or new_until is None:
        return None

    plan = plan_gold(silver, dates, new_until)
    for name, (df, predicate) in plan.tables.items():
        replace_rows(df, settings.gold_table(name), predicate)
    write_state(spark, state, new_until, dates)
    return plan
