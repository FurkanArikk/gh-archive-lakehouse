import json
from datetime import date, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from gh_lakehouse import gold as g
from gh_lakehouse.config import Settings
from gh_lakehouse.silver import ISSUES, PULL_REQUEST, PUSH, WATCH
from helpers import FIXTURE_LINES, edit, events_of, lines_of

pytestmark = pytest.mark.spark

DAY = date(2026, 10, 6)
MODELED = ("WatchEvent", "PushEvent", "PullRequestEvent", "IssuesEvent")


def ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def modeled_events() -> list[dict]:
    return [e for t in MODELED for e in events_of(t)]


# --------------------------------------------------------------------------- transformations


def test_repo_daily_activity_totals_match_the_fixture(make_silver):
    activity = g.repo_daily_activity(g.activity_events(make_silver(FIXTURE_LINES)))
    totals = activity.agg(
        *(
            F.sum(c).alias(c)
            for c in (
                "pushes",
                "prs_opened",
                "prs_merged",
                "issues_opened",
                "issues_closed",
                "stars",
                "events",
                "bot_events",
            )
        )
    ).first()

    def count(event_type: str, action: str | None = None) -> int:
        return sum(
            1
            for e in events_of(event_type)
            if action is None or e["payload"].get("action") == action
        )

    events = modeled_events()
    assert totals.pushes == count("PushEvent")
    assert totals.prs_opened == count("PullRequestEvent", "opened")
    assert totals.prs_merged == count("PullRequestEvent", "merged")
    assert totals.issues_opened == count("IssuesEvent", "opened")
    assert totals.issues_closed == count("IssuesEvent", "closed")
    assert totals.stars == count("WatchEvent")
    assert totals.events == len(events)
    assert totals.bot_events == sum(e["actor"]["login"].endswith("[bot]") for e in events)
    assert activity.filter("event_date != DATE'2026-10-06'").count() == 0


def test_repo_daily_activity_uses_the_latest_repo_name(make_silver):
    base = lines_of("WatchEvent")[0]
    lines = [
        edit(base, id="1", created_at="2026-10-06T10:00:00Z", repo={"name": "old/name"}),
        edit(base, id="2", created_at="2026-10-06T11:00:00Z", repo={"name": "new/name"}),
    ]
    row = g.repo_daily_activity(g.activity_events(make_silver(lines))).first()
    assert (row.repo_name, row.stars, row.events) == ("new/name", 2, 2)


def test_daily_bot_share_and_hourly_volume(make_silver):
    events = g.activity_events(make_silver(FIXTURE_LINES))
    share = {r.event_type: r for r in g.daily_bot_share(events).collect()}
    for event_type in MODELED:
        expected = events_of(event_type)
        bots = sum(e["actor"]["login"].endswith("[bot]") for e in expected)
        assert share[event_type].events == len(expected)
        assert share[event_type].bot_events == bots
        assert share[event_type].bot_share == pytest.approx(bots / len(expected))

    hours = {r.hour for r in g.hourly_event_volume(events).collect()}
    assert hours == {15}  # the fixture is one hour of data


def test_repo_star_growth_running_total_and_rank(make_silver):
    base = lines_of("WatchEvent")[0]
    repo = json.loads(base)["repo"]

    def star(i: int, day: str, repo_id: int = repo["id"]) -> str:
        return edit(base, id=str(1000 + i), created_at=f"{day}T12:00:00Z", repo={"id": repo_id})

    lines = [star(i, "2026-10-06") for i in range(3)]
    lines += [star(10 + i, "2026-10-07") for i in range(2)]
    lines += [star(20 + i, "2026-10-07", repo_id=1) for i in range(5)]  # another repo, more stars
    rows = {
        (r.event_date, r.repo_id): r for r in g.repo_star_growth(make_silver(lines).watch).collect()
    }

    first, second = rows[(DAY, repo["id"])], rows[(DAY + timedelta(days=1), repo["id"])]
    assert (first.stars, first.cumulative_stars, first.star_rank) == (3, 3, 1)
    assert (second.stars, second.cumulative_stars, second.star_rank) == (2, 5, 2)
    assert rows[(DAY + timedelta(days=1), 1)].star_rank == 1


def test_pr_time_to_merge_pairs_opened_and_merged(make_silver):
    rows = g.pr_time_to_merge(make_silver(lines_of("PullRequestEvent")).pull_requests).collect()

    prs: dict[tuple[int, int], dict[str, str]] = {}
    for e in events_of("PullRequestEvent"):
        key = (e["repo"]["id"], e["payload"]["number"])
        prs.setdefault(key, {}).setdefault(e["payload"]["action"], e["created_at"])
    merged = {k: v for k, v in prs.items() if "merged" in v}

    assert {(r.repo_id, r.pr_number) for r in rows} == set(merged)
    timed = [r for r in rows if r.hours_to_merge is not None]
    assert timed, "fixture has a PR opened and merged in the same hour"
    for r in rows:
        actions = merged[(r.repo_id, r.pr_number)]
        assert r.merged_at == ts(actions["merged"])
        if "opened" in actions:
            expected = (ts(actions["merged"]) - ts(actions["opened"])).total_seconds() / 3600
            assert r.hours_to_merge == pytest.approx(expected)
        else:  # opened before our data starts: counted, not timed
            assert r.opened_at is None and r.hours_to_merge is None


def test_issue_time_to_close(make_silver):
    closed = next(line for line in lines_of("IssuesEvent") if '"action":"closed"' in line)
    issue = json.loads(closed)["payload"]["issue"]
    stale = edit(closed, id="777", payload={"issue": {**issue, "closed_at": None, "state": "open"}})
    rows = {
        r.event_id: r
        for r in g.issue_time_to_close(
            make_silver([*lines_of("IssuesEvent"), stale]).issues
        ).collect()
    }

    closes = [e for e in events_of("IssuesEvent") if e["payload"]["action"] == "closed"]
    assert len(rows) == len(closes) + 1
    for e in closes:
        r, i = rows[int(e["id"])], e["payload"]["issue"]
        assert r.closed_at == ts(i["closed_at"]) and r.closed_at_from_event is False
        expected = (ts(i["closed_at"]) - ts(i["created_at"])).total_seconds() / 86400
        assert r.days_to_close == pytest.approx(expected)

    fallback = rows[777]
    assert fallback.closed_at_from_event is True
    assert fallback.closed_at == ts(json.loads(stale)["created_at"])


# --------------------------------------------------------------------------- incremental run


@pytest.fixture
def settings(spark):
    s = Settings(
        catalog="spark_catalog",
        raw_schema="t_raw",
        bronze_schema="t_bronze",
        silver_schema="t_silver",
        gold_schema="t_gold",
        landing_volume="landing",
        checkpoint_volume="checkpoints",
    )
    for schema in (s.silver_schema, s.gold_schema):
        spark.sql(f"DROP SCHEMA IF EXISTS {s.catalog}.{schema} CASCADE")
        spark.sql(f"CREATE SCHEMA {s.catalog}.{schema}")
    return s


def append_silver(spark, settings: Settings, silver: g.Silver, processed_at: datetime) -> None:
    """Write silver DataFrames to the silver tables, as one silver run at processed_at."""
    for spec, df in zip((WATCH, PUSH, PULL_REQUEST, ISSUES), silver.all(), strict=True):
        df = df.withColumn("_processed_at", F.lit(processed_at))
        df.write.format("delta").mode("append").saveAsTable(settings.silver_table(spec.table))


def gold_table(spark, settings: Settings, name: str):
    return spark.table(settings.gold_table(name))


def test_run_gold_rebuilds_only_new_days(spark, settings, make_silver):
    t0 = datetime(2026, 10, 7, 1, 0)
    append_silver(spark, settings, make_silver(FIXTURE_LINES), t0)

    first = g.run_gold(spark, settings)
    assert first is not None and first.dates == [DAY]
    activity_day1 = gold_table(spark, settings, g.REPO_DAILY_ACTIVITY).orderBy("repo_id").collect()
    assert g.run_gold(spark, settings) is None  # nothing new: nothing rebuilt
    assert g.read_state(spark, settings.gold_table(g.STATE_TABLE)) == (t0, [DAY])

    # Day 2 arrives: two more stars for a repo starred on day 1.
    base = lines_of("WatchEvent")[0]
    day2 = [edit(base, id=str(5000 + i), created_at="2026-10-07T09:00:00Z") for i in range(2)]
    append_silver(spark, settings, make_silver(day2), t0 + timedelta(hours=1))

    second = g.run_gold(spark, settings)
    assert second is not None and second.dates == [DAY + timedelta(days=1)]
    activity = gold_table(spark, settings, g.REPO_DAILY_ACTIVITY)
    assert activity.filter(F.col("event_date") == DAY).orderBy("repo_id").collect() == activity_day1
    repo_id = json.loads(base)["repo"]["id"]
    day1_stars = sum(e["repo"]["id"] == repo_id for e in events_of("WatchEvent"))
    growth = (
        gold_table(spark, settings, g.REPO_STAR_GROWTH)
        .filter((F.col("event_date") == DAY + timedelta(days=1)) & (F.col("repo_id") == repo_id))
        .first()
    )
    assert (growth.stars, growth.cumulative_stars) == (2, day1_stars + 2)


def test_run_gold_refreshes_a_merge_when_its_open_arrives_later(spark, settings, make_silver):
    """Backfill out of order: the merge (day 2) is processed before the open (day 1)."""
    base = lines_of("PullRequestEvent")[0]
    pr = json.loads(base)

    def pr_event(event_id: str, action: str, when: str) -> str:
        return edit(base, id=event_id, created_at=when, payload={"action": action, "number": 4242})

    t0 = datetime(2026, 10, 8)
    append_silver(
        spark, settings, make_silver([pr_event("8001", "merged", "2026-10-07T10:00:00Z")]), t0
    )
    g.run_gold(spark, settings)
    row = gold_table(spark, settings, g.PR_TIME_TO_MERGE).first()
    assert row.hours_to_merge is None

    append_silver(
        spark,
        settings,
        make_silver([pr_event("8000", "opened", "2026-10-06T22:00:00Z")]),
        t0 + timedelta(hours=1),
    )
    plan = g.run_gold(spark, settings)
    assert plan is not None and plan.dates == [DAY]  # only the open's day is new...
    rows = gold_table(spark, settings, g.PR_TIME_TO_MERGE).collect()
    assert len(rows) == 1  # ...but the merge's day was rebuilt, without duplicating it
    assert (rows[0].repo_id, rows[0].pr_number) == (pr["repo"]["id"], 4242)
    assert rows[0].hours_to_merge == pytest.approx(12.0)
