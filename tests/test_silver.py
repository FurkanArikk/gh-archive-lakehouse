import json
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from gh_lakehouse.silver import (
    ENVELOPE_COLUMNS,
    ERRORS,
    LINEAGE_COLUMNS,
    WATCH,
    parse_events,
    process_batch,
    quarantine_rows,
    run_spec,
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
