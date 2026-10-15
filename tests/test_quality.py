from datetime import UTC, date, datetime

import pytest
from pyspark.sql import functions as F

from gh_lakehouse import gold as g
from gh_lakehouse import quality as q
from gh_lakehouse.silver import SPECS, WATCH, parse_events, quarantine_rows
from helpers import FIXTURE_LINES, edit, lines_of

pytestmark = pytest.mark.spark

DAY = date(2026, 10, 6)
TARGETS = {s.table: f"c.s.{s.table}" for s in SPECS}


def by_check(results: list[q.CheckResult]) -> dict[tuple[str, str], q.CheckResult]:
    return {(r.check, r.target): r for r in results}


def silver_dict(silver: g.Silver) -> dict[str, object]:
    return dict(zip((s.table for s in SPECS), silver.all(), strict=True))


# --------------------------------------------------------------------------- bronze


def test_bronze_checks(make_bronze):
    bronze = make_bronze([*FIXTURE_LINES, '{"id": "1", "payload": {'])
    now = datetime(2026, 10, 7, 12, tzinfo=UTC)
    r = by_check(q.bronze_checks(bronze, [DAY], now=now))

    files = r[("files_per_day", str(DAY))]
    assert not files.passed and files.observed == "1"  # the fixture is one hour file, not 24

    corrupt = r[("corrupt_rate", str(DAY))]
    assert not corrupt.passed  # 1 of 22 lines is corrupt, far above 0.1%

    fresh = r[("freshness_hours", "events")]
    assert fresh.passed and fresh.severity == q.WARN  # newest event ~20.5h before `now`


def test_stale_data_is_only_a_warning(make_bronze):
    old = datetime(2026, 12, 1, tzinfo=UTC)
    fresh = by_check(q.bronze_checks(make_bronze(FIXTURE_LINES), [DAY], now=old))[
        ("freshness_hours", "events")
    ]
    assert not fresh.passed
    q.raise_on_failure([fresh])  # warnings never raise


# --------------------------------------------------------------------------- silver


def test_silver_reconciles_with_bronze(make_bronze, make_silver):
    results = q.silver_checks(
        make_bronze(FIXTURE_LINES),
        silver_dict(make_silver(FIXTURE_LINES)),
        None,
        [DAY],
        target_names=TARGETS,
    )
    failed = [r for r in results if not r.passed]
    assert failed == []
    assert {r.target for r in results if r.check == "bronze_reconciliation"} == set(TARGETS)


def test_a_dropped_silver_row_fails_reconciliation(make_bronze, make_silver):
    silver = silver_dict(make_silver(FIXTURE_LINES))
    watch = silver[WATCH.table]
    silver[WATCH.table] = watch.limit(watch.count() - 1)
    r = by_check(
        q.silver_checks(make_bronze(FIXTURE_LINES), silver, None, [DAY], target_names=TARGETS)
    )
    assert not r[("bronze_reconciliation", WATCH.table)].passed
    with pytest.raises(q.DataQualityError, match="bronze_reconciliation"):
        q.raise_on_failure(list(r.values()))


def test_quarantined_rows_count_towards_reconciliation(make_bronze, make_silver):
    bad = edit(lines_of("WatchEvent")[0], id="99", payload={"action": "starred"})
    lines = [*FIXTURE_LINES, bad]
    bronze = make_bronze(lines)
    quarantine = quarantine_rows(
        parse_events(bronze.filter(F.col("event_type") == "WatchEvent"), WATCH),
        TARGETS[WATCH.table],
    )
    r = by_check(
        q.silver_checks(
            bronze, silver_dict(make_silver(lines)), quarantine, [DAY], target_names=TARGETS
        )
    )
    assert r[("bronze_reconciliation", WATCH.table)].passed
    rate = r[("quarantine_rate", "quarantine")]
    assert not rate.passed  # 1 of 22 rows is far above the 1% limit


def test_duplicate_silver_ids_fail(make_bronze, make_silver):
    silver = silver_dict(make_silver(FIXTURE_LINES))
    silver[WATCH.table] = silver[WATCH.table].unionByName(silver[WATCH.table].limit(1))
    r = by_check(
        q.silver_checks(make_bronze(FIXTURE_LINES), silver, None, [DAY], target_names=TARGETS)
    )
    assert not r[("duplicate_event_id", WATCH.table)].passed


# --------------------------------------------------------------------------- gold


def gold_from(silver: g.Silver) -> dict[str, object]:
    plan = g.plan_gold(silver, [DAY], None)
    return {name: df for name, (df, _) in plan.tables.items()}


def test_gold_checks_pass_on_consistent_tables(make_silver):
    silver = make_silver(FIXTURE_LINES)
    results = q.gold_checks(silver_dict(silver), gold_from(silver), [DAY])
    assert [r for r in results if not r.passed] == []


def test_gold_checks_catch_missing_days_and_mismatched_totals(make_silver):
    silver = make_silver(FIXTURE_LINES)
    gold = gold_from(silver)
    gold[g.DAILY_BOT_SHARE] = gold[g.DAILY_BOT_SHARE].limit(0)
    gold[g.REPO_DAILY_ACTIVITY] = gold[g.REPO_DAILY_ACTIVITY].withColumn(
        "stars", F.col("stars") * 2
    )
    r = by_check(q.gold_checks(silver_dict(silver), gold, [DAY]))
    assert not r[("rows_for_every_date", g.DAILY_BOT_SHARE)].passed
    assert not r[("stars_match_silver", g.REPO_DAILY_ACTIVITY)].passed
    assert r[("pushes_match_silver", g.REPO_DAILY_ACTIVITY)].passed


# --------------------------------------------------------------------------- recording


def test_results_are_recorded(spark):
    spark.sql("DROP TABLE IF EXISTS dq_results_test")
    results = [
        q.CheckResult("silver", "x", "t", True, "1", "== 1"),
        q.CheckResult("bronze", "y", "t", False, "2", "<= 1", q.WARN),
    ]
    q.record_results(spark, results, "dq_results_test", run_id="42")
    q.record_results(spark, results, "dq_results_test", run_id="43")
    rows = spark.table("dq_results_test").collect()
    assert len(rows) == 4
    assert {(r.run_id, r.check, r.passed, r.severity) for r in rows} >= {("42", "y", False, "warn")}
    assert "FAIL" not in q.summarize(results) and "WARN" in q.summarize(results)


def test_hours_without_events_are_warned_about(make_bronze):
    push = lines_of("PushEvent")[0]
    lines = [
        edit(push, id=str(600 + h), created_at=f"2026-10-06T{h:02d}:30:00Z") for h in range(20)
    ]
    results = q.hourly_completeness(make_bronze(lines), [DAY])
    r = by_check(results)

    gap = r[("hourly_completeness", f"{DAY}/PushEvent")]
    assert not gap.passed and gap.severity == q.WARN
    assert gap.observed == "no events in hours [20, 21, 22, 23]"  # the real 2026-10-06 gap
    assert not r[("hourly_completeness", f"{DAY}/WatchEvent")].passed  # no stars at all
    q.raise_on_failure(results)  # warnings only


def test_no_dates_means_no_silver_results(make_bronze, make_silver):
    silver = silver_dict(make_silver(FIXTURE_LINES))
    assert q.silver_checks(make_bronze(FIXTURE_LINES), silver, None, [], target_names=TARGETS) == []
