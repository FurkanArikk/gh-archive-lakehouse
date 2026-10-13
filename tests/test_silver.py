import json
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from gh_lakehouse.silver import (
    ENVELOPE_COLUMNS,
    ERRORS,
    ISSUES,
    LINEAGE_COLUMNS,
    PULL_REQUEST,
    PUSH,
    SPECS,
    UNCLASSIFIED,
    WATCH,
    parse_events,
    process_batch,
    quarantine_rows,
    run_spec,
    unclassified_rows,
    valid_rows,
)

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent / "fixtures" / "gharchive_sample.jsonl"
FIXTURE_LINES = FIXTURE.read_text().splitlines()
WATCH_LINES = [line for line in FIXTURE_LINES if '"type":"WatchEvent"' in line]


def edit(line: str, **changes: object) -> str:
    """Return the event with top-level fields replaced; nested dicts are merged."""
    event = json.loads(line)
    for key, value in changes.items():
        if isinstance(value, dict) and isinstance(event.get(key), dict):
            event[key] = {**event[key], **value}
        else:
            event[key] = value
    return json.dumps(event)


def test_fixture_has_watch_events():
    assert len(WATCH_LINES) == 3


def test_valid_watch_events_are_typed(make_bronze):
    parsed = parse_events(make_bronze(WATCH_LINES), WATCH)
    assert parsed.filter(F.size(ERRORS) > 0).count() == 0

    silver = valid_rows(parsed, WATCH)
    assert silver.columns == [*ENVELOPE_COLUMNS, *LINEAGE_COLUMNS]
    types = dict(silver.dtypes)
    assert types["event_id"] == "bigint"
    assert types["created_at"] == "timestamp"
    assert types["event_date"] == "date"

    originals = {int(json.loads(line)["id"]): json.loads(line) for line in WATCH_LINES}
    for row in silver.collect():
        src = originals[row.event_id]
        assert row.created_at == datetime.strptime(src["created_at"], "%Y-%m-%dT%H:%M:%SZ")
        assert row.event_date == date(2026, 10, 6)
        assert row.actor_login == src["actor"]["login"]
        assert row.is_bot == src["actor"]["login"].endswith("[bot]")
        assert row.repo_name == src["repo"]["name"]
        assert row.org_login == (src.get("org") or {}).get("login")


def test_is_bot_uses_the_app_suffix(make_bronze):
    lines = [
        edit(WATCH_LINES[0], id="1", actor={"login": "dependabot[bot]"}),
        edit(WATCH_LINES[0], id="2", actor={"login": "LizardByte-bot"}),
        edit(WATCH_LINES[0], id="3", actor={"login": "octocat"}),
    ]
    silver = valid_rows(parse_events(make_bronze(lines), WATCH), WATCH)
    rows = {r.event_id: r.is_bot for r in silver.collect()}
    assert rows == {1: True, 2: False, 3: False}  # a lower bound, by design


def test_invalid_events_are_quarantined_with_reasons(make_bronze):
    good = WATCH_LINES[0]
    lines = [
        edit(good, id="not-a-number"),
        edit(good, id="101", created_at="yesterday"),
        edit(good, id="102", payload={"action": "starred"}),
        edit(good, id="103", actor={"id": None}),
        edit(good, id="104", payload={"action": None}),
        '{"id": "105", "type": "WatchEvent", "payload": {"action": ',  # truncated line
    ]
    parsed = parse_events(make_bronze(lines), WATCH)
    assert valid_rows(parsed, WATCH).count() == 0

    q = {r.event_id: r.reasons for r in quarantine_rows(parsed, "c.s.watch_events").collect()}
    assert q["not-a-number"] == ["invalid_event_id"]  # raw id kept for investigation
    assert q["101"] == ["invalid_created_at"]
    assert q["102"] == ["unexpected_action:starred"]
    assert q["103"] == ["missing_actor_id"]
    assert q["104"] == ["missing_action"]
    assert "corrupt_record" in q["105"]


def test_quarantine_keeps_raw_values(make_bronze):
    line = edit(WATCH_LINES[0], id="102", payload={"action": "starred"})
    row = quarantine_rows(parse_events(make_bronze([line]), WATCH), "c.s.watch_events").first()
    assert row.event_type == "WatchEvent"
    assert row.target_table == "c.s.watch_events"
    assert json.loads(row.payload) == {"action": "starred"}
    assert row.raw_record is None
    assert row._source_file.endswith("2026-10-06-15.json.gz")
    assert row._quarantined_at is not None


def test_duplicates_collapse_to_one_row(make_bronze):
    lines = [WATCH_LINES[0], WATCH_LINES[0], WATCH_LINES[1]]
    assert valid_rows(parse_events(make_bronze(lines), WATCH), WATCH).count() == 2


def test_process_batch_is_idempotent(spark, make_bronze):
    target, quarantine = "silver_watch_batch", "silver_quarantine_batch"
    for t in (target, quarantine):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    batch = make_bronze([*WATCH_LINES, edit(WATCH_LINES[0], id="102", payload={"action": "x"})])

    for _ in range(2):  # a retried micro-batch has the same batch_id
        process_batch(batch, 0, spec=WATCH, target=target, quarantine=quarantine)

    assert spark.table(target).count() == 3
    assert spark.table(quarantine).count() == 1


def test_stream_processes_only_new_bronze_rows(spark, make_bronze, tmp_path):
    source, target, quarantine = "bronze_for_silver", "silver_watch_stream", "silver_q_stream"
    for t in (source, target, quarantine):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    checkpoint = str(tmp_path / "checkpoint")

    def run() -> None:
        run_spec(
            spark, WATCH, source=source, target=target, quarantine=quarantine, checkpoint=checkpoint
        )

    # Whole fixture lands in bronze; the WatchEvent stream must pick out only its 3 rows.
    make_bronze(FIXTURE_LINES).write.format("delta").saveAsTable(source)
    run()
    assert spark.table(target).count() == 3

    # A later bronze append: one new star, one duplicate of an existing one.
    new = make_bronze([edit(WATCH_LINES[0], id="999"), WATCH_LINES[1]])
    new.write.format("delta").mode("append").saveAsTable(source)
    run()
    assert spark.table(target).count() == 4

    run()  # nothing new
    assert spark.table(target).count() == 4
    assert not spark.catalog.tableExists(quarantine)  # no invalid WatchEvents in the data


# --------------------------------------------------------------------------- all event types


def lines_of(event_type: str) -> list[str]:
    return [line for line in FIXTURE_LINES if f'"type":"{event_type}"' in line]


def by_id(lines: list[str]) -> dict[int, dict]:
    return {int(json.loads(line)["id"]): json.loads(line) for line in lines}


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.event_type)
def test_every_real_fixture_event_is_valid(make_bronze, spec):
    lines = lines_of(spec.event_type)
    assert lines, f"fixture has no {spec.event_type}"
    parsed = parse_events(make_bronze(lines), spec)
    errors = [r[ERRORS] for r in parsed.collect() if r[ERRORS]]
    assert errors == []
    assert valid_rows(parsed, spec).count() == len(by_id(lines))  # duplicates collapse


def test_push_events(make_bronze):
    lines = lines_of("PushEvent")
    rows = valid_rows(parse_events(make_bronze(lines), PUSH), PUSH).collect()
    src = by_id(lines)
    for row in rows:
        p = src[row.event_id]["payload"]
        assert (row.push_id, row.ref, row.head_sha, row.before_sha) == (
            p["push_id"],
            p["ref"],
            p["head"],
            p["before"],
        )
        assert row.branch == p["ref"].removeprefix("refs/heads/")


def test_push_branch_is_null_for_tag_refs_and_bad_sha_is_quarantined(make_bronze):
    base = lines_of("PushEvent")[0]
    lines = [
        edit(base, id="201", payload={"ref": "refs/tags/v1.0"}),
        edit(base, id="202", payload={"head": "not-a-sha"}),
    ]
    parsed = parse_events(make_bronze(lines), PUSH)
    assert valid_rows(parsed, PUSH).first().branch is None
    q = {r.event_id: r.reasons for r in quarantine_rows(parsed, "t").collect()}
    assert q == {"202": ["invalid_head_sha"]}


def test_pull_request_events(make_bronze):
    lines = lines_of("PullRequestEvent")
    rows = valid_rows(parse_events(make_bronze(lines), PULL_REQUEST), PULL_REQUEST).collect()
    src = by_id(lines)
    assert {r.action for r in rows} >= {"opened", "merged", "closed", "labeled"}
    for row in rows:
        p = src[row.event_id]["payload"]
        pr = p["pull_request"]
        assert (row.action, row.pr_number, row.pr_id) == (p["action"], p["number"], pr["id"])
        assert (row.head_sha, row.base_ref) == (pr["head"]["sha"], pr["base"]["ref"])
        assert row.is_from_fork == (pr["head"]["repo"]["id"] != pr["base"]["repo"]["id"])
        assert row.label_name == (p.get("label") or {}).get("name")


def test_pull_request_opened_and_merged_pair_is_in_fixture(make_bronze):
    rows = valid_rows(
        parse_events(make_bronze(lines_of("PullRequestEvent")), PULL_REQUEST), PULL_REQUEST
    ).collect()
    actions_by_pr: dict[tuple[int, int], set[str]] = {}
    for r in rows:
        actions_by_pr.setdefault((r.repo_id, r.pr_number), set()).add(r.action)
    assert any({"opened", "merged"} <= a for a in actions_by_pr.values())  # gold relies on this


def test_pull_request_invalid_records(make_bronze):
    base = lines_of("PullRequestEvent")[0]
    original = json.loads(base)["payload"]
    lines = [
        edit(base, id="301", payload={"action": "synchronize"}),
        edit(base, id="302", payload={"pull_request": {**original["pull_request"], "id": None}}),
    ]
    q = {
        r.event_id: r.reasons
        for r in quarantine_rows(parse_events(make_bronze(lines), PULL_REQUEST), "t").collect()
    }
    assert q == {"301": ["unexpected_action:synchronize"], "302": ["missing_pull_request_id"]}


def test_issues_events(make_bronze):
    lines = lines_of("IssuesEvent")
    rows = valid_rows(parse_events(make_bronze(lines), ISSUES), ISSUES).collect()
    src = by_id(lines)

    def ts(value: str | None) -> datetime | None:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ") if value else None

    for row in rows:
        p = src[row.event_id]["payload"]
        issue = p["issue"]
        assert (row.action, row.issue_number, row.title) == (
            p["action"],
            issue["number"],
            issue["title"],
        )
        assert row.issue_created_at == ts(issue["created_at"])
        assert row.issue_closed_at == ts(issue["closed_at"])
        assert row.author_is_bot == (issue["user"]["type"] == "Bot")
        assert row.label_names == [label["name"] for label in issue["labels"]]
    closed = [r for r in rows if r.action == "closed"]
    assert closed and all(r.issue_closed_at is not None for r in closed)


def test_issues_invalid_records(make_bronze):
    base = next(line for line in lines_of("IssuesEvent") if '"action":"closed"' in line)
    issue = json.loads(base)["payload"]["issue"]
    lines = [
        # Real-data quirk: the issue snapshot was taken just before the close.
        edit(base, id="401", payload={"issue": {**issue, "closed_at": None, "state": "open"}}),
        edit(base, id="402", payload={"issue": {**issue, "created_at": "soon"}}),
        edit(base, id="403", payload={"issue": {**issue, "closed_at": "later"}}),
    ]
    parsed = parse_events(make_bronze(lines), ISSUES)
    q = {r.event_id: r.reasons for r in quarantine_rows(parsed, "t").collect()}
    assert q == {"402": ["invalid_issue_created_at"], "403": ["invalid_issue_closed_at"]}

    stale = valid_rows(parsed, ISSUES).collect()
    assert [(r.event_id, r.action, r.issue_closed_at) for r in stale] == [(401, "closed", None)]


def test_every_corrupt_row_is_quarantined_exactly_once(make_bronze):
    # Spark keeps partly parsed fields only when the JSON breaks inside a value, so a
    # corrupt row may or may not still know its type. Both cases must end up in quarantine
    # exactly once: via its type's stream if the type survived, else via unclassified.
    cut_in_value = '{{"id": "{id}", "type": "{type}", "payload": {{"ref": '
    cut_after_brace = '{{"id": "{id}", "type": "{type}", "payload": {{'
    lines = [
        cut_in_value.format(id=501, type="WatchEvent"),  # type kept -> WatchEvent stream
        cut_in_value.format(id=502, type="CreateEvent"),  # type kept, not modelled
        cut_after_brace.format(id=503, type="WatchEvent"),  # type lost
        '{"id": "504", "payload": {"ref": ',  # never had a type
        lines_of("CreateEvent")[0],  # valid, just not modelled: not quarantined
    ]
    bronze = make_bronze(lines)

    unclassified = quarantine_rows(unclassified_rows(bronze), UNCLASSIFIED).collect()
    watch_stream = quarantine_rows(
        parse_events(bronze.filter("event_type = 'WatchEvent'"), WATCH), "t"
    ).collect()

    quarantined = [r.raw_record for r in unclassified + watch_stream]
    assert sorted(quarantined) == sorted(lines[:4])  # each corrupt line once, none dropped
    assert [r.raw_record for r in watch_stream] == [lines[0]]
    assert all(r.target_table == UNCLASSIFIED for r in unclassified)
    assert all(r.reasons == ["corrupt_record"] for r in unclassified)
