"""Data quality checks between layers. A failed ``error`` check fails the job.

Checks are scoped to the dates a run processes, so their cost doesn't grow with history.
Bronze, silver and quarantine rows are matched on the *file* date (from the landing path)
rather than the event date: a row whose created_at failed to parse has no event date but
still belongs to exactly one file.

Severity:
- ``error`` breaks an invariant (lost rows, duplicates, missing files); the task raises.
- ``warn`` is worth seeing but not worth stopping for. Freshness is a warning because this
  project deliberately backfills past days.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

from gh_lakehouse.silver import SPECS, EventSpec

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from gh_lakehouse.config import Settings

FILES_PER_DAY = 24
MAX_CORRUPT_RATE = 0.001
MAX_QUARANTINE_RATE = 0.01
MAX_DATA_AGE_HOURS = 48
DQ_RESULTS_TABLE = "dq_results"

ERROR, WARN = "error", "warn"
WATCH_T, PUSH_T, PR_T, ISSUES_T = (spec.table for spec in SPECS)


@dataclass(frozen=True)
class CheckResult:
    layer: str
    check: str
    target: str
    passed: bool
    observed: str
    expected: str
    severity: str = ERROR


class DataQualityError(Exception):
    def __init__(self, failures: list[CheckResult]) -> None:
        lines = [
            f"{f.layer}.{f.check}[{f.target}]: {f.observed} (expected {f.expected})"
            for f in failures
        ]
        super().__init__(f"{len(failures)} data quality check(s) failed:\n  " + "\n  ".join(lines))
        self.failures = failures


def file_date(df: DataFrame) -> DataFrame:
    """Add ``_file_date`` parsed from the ``date=YYYY-MM-DD`` folder in ``_source_file``."""
    raw = F.regexp_extract("_source_file", r"date=(\d{4}-\d{2}-\d{2})", 1)
    return df.withColumn("_file_date", F.to_date(F.nullif(raw, F.lit(""))))


def _on(df: DataFrame, dates: list[date]) -> DataFrame:
    return file_date(df).filter(F.col("_file_date").isin(dates))


# --------------------------------------------------------------------------- bronze


def bronze_checks(
    bronze: DataFrame, dates: list[date], *, now: datetime | None = None
) -> list[CheckResult]:
    per_day = {
        r._file_date: r
        for r in _on(bronze, dates)
        .groupBy("_file_date")
        .agg(
            F.count_distinct("_source_file").alias("files"),
            F.count("*").alias("rows"),
            F.count_if(F.col("_corrupt_record").isNotNull()).alias("corrupt"),
        )
        .collect()
    }
    results = []
    for d in dates:
        row = per_day.get(d)
        files, rows, corrupt = (row.files, row.rows, row.corrupt) if row else (0, 0, 0)
        results.append(
            CheckResult(
                "bronze",
                "files_per_day",
                str(d),
                files == FILES_PER_DAY,
                str(files),
                f"== {FILES_PER_DAY}",
            )
        )
        rate = corrupt / rows if rows else 0.0
        results.append(
            CheckResult(
                "bronze",
                "corrupt_rate",
                str(d),
                rate <= MAX_CORRUPT_RATE,
                f"{rate:.5f}",
                f"<= {MAX_CORRUPT_RATE}",
            )
        )

    results += hourly_completeness(bronze, dates)

    newest = bronze.agg(F.max("created_at")).first()[0]  # ISO strings sort chronologically
    now = now or datetime.now(UTC)
    age = None
    if newest:
        newest_at = datetime.strptime(newest, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        age = (now - newest_at).total_seconds() / 3600
    results.append(
        CheckResult(
            "bronze",
            "freshness_hours",
            "events",
            age is not None and age <= MAX_DATA_AGE_HOURS,
            "no data" if age is None else f"{age:.1f}",
            f"<= {MAX_DATA_AGE_HOURS}",
            WARN,
        )
    )
    return results


def hourly_completeness(bronze: DataFrame, dates: list[date]) -> list[CheckResult]:
    """Warn when a modelled event type is absent for whole hours of a day.

    Added after real data showed it: on 2026-10-06 GH Archive has no PushEvents at all
    from 20:00 to 23:59 UTC, although all 24 files are present and well-formed. File counts
    and reconciliation can't see a gap that is in the source itself. A warning, not an
    error: failing the job can't fix GitHub's feed, but the gap must be visible.
    """
    modelled = [spec.event_type for spec in SPECS]
    hours_seen = {
        (r._file_date, r.event_type): set(r.hours)
        for r in _on(bronze, dates)
        .filter(F.col("event_type").isin(modelled))
        .groupBy("_file_date", "event_type")
        .agg(F.collect_set(F.substring("created_at", 12, 2).cast("int")).alias("hours"))
        .collect()
    }
    results = []
    for d in dates:
        for event_type in modelled:
            missing = sorted(set(range(24)) - hours_seen.get((d, event_type), set()))
            results.append(
                CheckResult(
                    "bronze",
                    "hourly_completeness",
                    f"{d}/{event_type}",
                    not missing,
                    f"no events in hours {missing}" if missing else "all 24 hours",
                    "events in all 24 hours",
                    WARN,
                )
            )
    return results


# --------------------------------------------------------------------------- silver


def silver_checks(
    bronze: DataFrame,
    silver: dict[str, DataFrame],
    quarantine: DataFrame | None,
    dates: list[date],
    *,
    target_names: dict[str, str],
    specs: tuple[EventSpec, ...] = SPECS,
) -> list[CheckResult]:
    """Keys, uniqueness, and the reconciliation that proves nothing was dropped:

        distinct bronze ids  ==  silver rows  +  distinct quarantined ids     (per type, per day)

    ``silver`` and ``target_names`` are keyed by EventSpec.table; target_names maps to the
    fully qualified name quarantine stores in ``target_table``.
    """
    if not dates:  # nothing new: a "0 == 0" pass would only be noise in dq_results
        return []
    b = _on(bronze, dates)
    q = _on(quarantine, dates) if quarantine is not None else None
    results = []
    for spec in specs:
        s = _on(silver[spec.table], dates)
        stats = s.agg(
            F.count("*").alias("rows"),
            F.count_distinct("event_id").alias("ids"),
            F.count_if(F.col("event_id").isNull()).alias("null_ids"),
        ).first()
        results += [
            CheckResult(
                "silver",
                "null_event_id",
                spec.table,
                stats.null_ids == 0,
                str(stats.null_ids),
                "== 0",
            ),
            CheckResult(
                "silver",
                "duplicate_event_id",
                spec.table,
                stats.rows == stats.ids,
                str(stats.rows - stats.ids),
                "== 0",
            ),
        ]
        bronze_ids = (
            b.filter(F.col("event_type") == spec.event_type)
            .agg(F.count_distinct("event_id"))
            .first()[0]
        )
        quarantined = (
            q.filter(F.col("target_table") == target_names[spec.table])
            .agg(F.count_distinct("event_id"))
            .first()[0]
            if q is not None
            else 0
        )
        accounted = stats.rows + quarantined
        results.append(
            CheckResult(
                "silver",
                "bronze_reconciliation",
                spec.table,
                accounted == bronze_ids,
                f"silver {stats.rows} + quarantine {quarantined} = {accounted}",
                f"== bronze distinct ids {bronze_ids}",
            )
        )

    bronze_rows = b.count()
    quarantined_rows = q.count() if q is not None else 0
    rate = quarantined_rows / bronze_rows if bronze_rows else 0.0
    results.append(
        CheckResult(
            "silver",
            "quarantine_rate",
            "quarantine",
            rate <= MAX_QUARANTINE_RATE,
            f"{rate:.5f}",
            f"<= {MAX_QUARANTINE_RATE}",
        )
    )
    return results


# --------------------------------------------------------------------------- gold


def gold_checks(
    silver: dict[str, DataFrame], gold: dict[str, DataFrame], dates: list[date]
) -> list[CheckResult]:
    """Every daily table has rows for each date, and gold totals match silver."""
    from gh_lakehouse import gold as g

    on_dates = F.col("event_date").isin(dates)
    s = {name: df.filter(on_dates) for name, df in silver.items()}
    results = []

    for table in (
        g.REPO_DAILY_ACTIVITY,
        g.DAILY_BOT_SHARE,
        g.HOURLY_EVENT_VOLUME,
        g.REPO_STAR_GROWTH,
    ):
        present = {
            r.event_date
            for r in gold[table].filter(on_dates).select("event_date").distinct().collect()
        }
        missing = sorted(set(dates) - present)
        results.append(
            CheckResult(
                "gold",
                "rows_for_every_date",
                table,
                not missing,
                f"missing {missing}" if missing else "ok",
                "no missing dates",
            )
        )

    def equal(check: str, table: str, got: int, want: int, what: str) -> CheckResult:
        return CheckResult("gold", check, table, got == want, str(got), f"== {what} {want}")

    activity = (
        gold[g.REPO_DAILY_ACTIVITY]
        .filter(on_dates)
        .agg(
            F.sum("stars").alias("stars"),
            F.sum("pushes").alias("pushes"),
            F.sum("events").alias("events"),
        )
        .first()
    )
    total_events = sum(df.count() for df in s.values())
    results += [
        equal(
            "stars_match_silver",
            g.REPO_DAILY_ACTIVITY,
            activity.stars or 0,
            s[WATCH_T].count(),
            "silver watch_events",
        ),
        equal(
            "pushes_match_silver",
            g.REPO_DAILY_ACTIVITY,
            activity.pushes or 0,
            s[PUSH_T].count(),
            "silver push_events",
        ),
        equal(
            "events_match_silver",
            g.REPO_DAILY_ACTIVITY,
            activity.events or 0,
            total_events,
            "silver events",
        ),
        equal(
            "events_match_silver",
            g.DAILY_BOT_SHARE,
            gold[g.DAILY_BOT_SHARE].filter(on_dates).agg(F.sum("events")).first()[0] or 0,
            total_events,
            "silver events",
        ),
        equal(
            "events_match_silver",
            g.HOURLY_EVENT_VOLUME,
            gold[g.HOURLY_EVENT_VOLUME].filter(on_dates).agg(F.sum("events")).first()[0] or 0,
            total_events,
            "silver events",
        ),
        equal(
            "one_row_per_merged_pr",
            g.PR_TIME_TO_MERGE,
            gold[g.PR_TIME_TO_MERGE].filter(on_dates).count(),
            s[PR_T].filter(F.col("action") == "merged").select(*g.PR_KEY).distinct().count(),
            "merged PRs in silver",
        ),
        equal(
            "one_row_per_close",
            g.ISSUE_TIME_TO_CLOSE,
            gold[g.ISSUE_TIME_TO_CLOSE].filter(on_dates).count(),
            s[ISSUES_T].filter(F.col("action") == "closed").count(),
            "closed events in silver",
        ),
    ]
    return results


# --------------------------------------------------------------------------- recording


RESULTS_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("checked_at", TimestampType()),
        StructField("layer", StringType()),
        StructField("check", StringType()),
        StructField("target", StringType()),
        StructField("passed", BooleanType()),
        StructField("severity", StringType()),
        StructField("observed", StringType()),
        StructField("expected", StringType()),
    ]
)


def record_results(
    spark: SparkSession, results: list[CheckResult], table: str, run_id: str
) -> None:
    now = datetime.now()
    rows = [
        (run_id, now, r.layer, r.check, r.target, r.passed, r.severity, r.observed, r.expected)
        for r in results
    ]
    spark.createDataFrame(rows, RESULTS_SCHEMA).write.format("delta").mode("append").saveAsTable(
        table
    )


def raise_on_failure(results: list[CheckResult]) -> None:
    failures = [r for r in results if not r.passed and r.severity == ERROR]
    if failures:
        raise DataQualityError(failures)


def summarize(results: list[CheckResult]) -> str:
    lines = []
    for r in results:
        status = "PASS" if r.passed else ("FAIL" if r.severity == ERROR else "WARN")
        lines.append(
            f"{status}  {r.layer}.{r.check}[{r.target}]: {r.observed} (expected {r.expected})"
        )
    return "\n".join(lines)


# --------------------------------------------------------------------------- task entry


def _table_or_none(spark: SparkSession, name: str) -> DataFrame | None:
    from pyspark.errors import AnalysisException

    try:
        df = spark.table(name)
        df.schema  # noqa: B018 - forces resolution, raising if the table is missing
        return df
    except AnalysisException:
        return None


def run_checks(
    spark: SparkSession,
    settings: Settings,
    layer: str,
    run_id: str,
    dates: list[date] | None = None,
) -> list[CheckResult]:
    """Run one layer's checks, record them in dq_results, and return them.

    - ``silver``: bronze + silver checks for the dates gold is about to process
      (silver rows newer than gold's high-water mark), i.e. before they reach gold.
    - ``gold``: gold checks for the dates the last gold run rebuilt.

    ``dates`` overrides that choice, to re-check specific days on demand.
    """
    from gh_lakehouse import gold as g
    from gh_lakehouse.bronze import BRONZE_TABLE
    from gh_lakehouse.silver import QUARANTINE_TABLE

    silver = {s.table: spark.table(settings.silver_table(s.table)) for s in SPECS}
    state = settings.gold_table(g.STATE_TABLE)
    if layer == "silver":
        if dates is None:
            processed_until, _ = g.read_state(spark, state)
            dates, _ = g.pending(g.Silver(*(silver[s.table] for s in SPECS)), processed_until)
        bronze = spark.table(settings.bronze_table(BRONZE_TABLE))
        quarantine = _table_or_none(spark, settings.silver_table(QUARANTINE_TABLE))
        targets = {s.table: settings.silver_table(s.table) for s in SPECS}
        results = bronze_checks(bronze, dates) + silver_checks(
            bronze, silver, quarantine, dates, target_names=targets
        )
    elif layer == "gold":
        if dates is None:
            _, dates = g.read_state(spark, state)
        if not dates:
            return []
        gold = {t: spark.table(settings.gold_table(t)) for t in g.GOLD_TABLES}
        results = gold_checks(silver, gold, dates)
    else:
        raise ValueError(f"unknown layer {layer!r}; expected 'silver' or 'gold'")

    record_results(spark, results, settings.gold_table(DQ_RESULTS_TABLE), run_id)
    return results
