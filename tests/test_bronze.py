import gzip
import json
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from gh_lakehouse.bronze import (
    CORRUPT_RECORD,
    RAW_SCHEMA,
    input_rows,
    read_landing_stream,
    to_bronze,
    write_bronze,
)

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent / "fixtures" / "gharchive_sample.jsonl"
FIXTURE_LINES = FIXTURE.read_text().splitlines()
BAD_LINE = '{"id": "1", "type": "PushEvent", "payload": {"ref": '  # truncated JSON


def land(root: Path, day: str, hour: int, lines: list[str]) -> Path:
    """Write lines as a gzipped GH Archive file under the volume's date= layout."""
    path = root / "gharchive" / f"date={day}" / f"{day}-{hour}.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))
    return path


def read_batch(spark: SparkSession, root: Path) -> DataFrame:
    raw = (
        spark.read.schema(RAW_SCHEMA)
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .json(str(root / "gharchive"))
    )
    return to_bronze(raw)


@pytest.fixture
def bronze_rows(spark, tmp_path):
    land(tmp_path, "2026-10-06", 15, [*FIXTURE_LINES, BAD_LINE])
    return {r.event_id: r for r in read_batch(spark, tmp_path).collect()}


def test_bronze_columns(spark, tmp_path):
    land(tmp_path, "2026-10-06", 15, FIXTURE_LINES)
    assert read_batch(spark, tmp_path).columns == [
        "event_id",
        "event_type",
        "created_at",
        "actor",
        "repo",
        "org",
        "public",
        "payload",
        "_corrupt_record",
        "_rescued_data",
        "_source_file",
        "_file_modified_at",
        "_ingested_at",
    ]


def test_every_line_becomes_a_row_including_duplicates(bronze_rows, spark, tmp_path):
    df = read_batch(spark, tmp_path)
    # 21 fixture lines (one id appears twice) + 1 malformed line; bronze drops nothing.
    assert df.count() == len(FIXTURE_LINES) + 1
    assert df.filter("event_id = '16557319348'").count() == 2


def test_payload_is_kept_as_the_original_json(bronze_rows):
    originals = {json.loads(line)["id"]: json.loads(line) for line in FIXTURE_LINES}
    for event_id, original in originals.items():
        assert json.loads(bronze_rows[event_id].payload) == original["payload"]

    watch = next(r for r in bronze_rows.values() if r.event_type == "WatchEvent")
    assert json.loads(watch.payload) == {"action": "started"}


def test_envelope_fields_are_read(bronze_rows):
    original = json.loads(FIXTURE_LINES[0])
    row = bronze_rows[original["id"]]
    assert row.event_type == original["type"]
    assert row.created_at == original["created_at"]  # still a string in bronze
    assert row.actor.id == original["actor"]["id"]
    assert row.actor.login == original["actor"]["login"]
    assert row.repo.name == original["repo"]["name"]
    assert row.public is True


def test_org_is_null_when_absent(bronze_rows):
    without_org = [json.loads(line)["id"] for line in FIXTURE_LINES if '"org":' not in line]
    assert without_org, "fixture should contain events without an org"
    assert all(bronze_rows[i].org is None for i in without_org)


def test_malformed_line_is_kept_not_dropped(bronze_rows):
    bad = [r for r in bronze_rows.values() if r[CORRUPT_RECORD] is not None]
    assert len(bad) == 1
    assert bad[0][CORRUPT_RECORD] == BAD_LINE
    # Spark keeps the fields it parsed before the error, so a corrupt row can still have
    # an id and type. Silver must check _corrupt_record, not just null keys.
    assert (bad[0].event_id, bad[0].event_type) == ("1", "PushEvent")
    assert bad[0].payload is None


def test_lineage_columns(bronze_rows):
    row = next(r for r in bronze_rows.values() if r[CORRUPT_RECORD] is None)
    assert row._source_file.endswith("/gharchive/date=2026-10-06/2026-10-06-15.json.gz")
    assert row._file_modified_at is not None
    assert row._ingested_at is not None
    assert row._rescued_data is None


def test_stream_processes_only_new_files(spark, tmp_path):
    landing = tmp_path / "landing"
    checkpoint = str(tmp_path / "checkpoints" / "bronze_events")
    table = "bronze_stream_test"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    def run() -> None:
        stream = read_landing_stream(spark, str(landing / "gharchive"), auto_loader=False)
        write_bronze(to_bronze(stream), table, checkpoint).awaitTermination()

    def count() -> int:
        return spark.table(table).count()

    land(landing, "2026-10-06", 0, FIXTURE_LINES[:10])
    run()
    assert count() == 10

    land(landing, "2026-10-06", 1, FIXTURE_LINES[10:])
    run()
    assert count() == len(FIXTURE_LINES)

    run()  # nothing new: the checkpoint remembers both files
    assert count() == len(FIXTURE_LINES)


def test_input_rows_treats_missing_counts_as_zero():
    # Serverless reports None for batches without new data; local Spark reports 0.
    progress = [{"numInputRows": 10}, {"numInputRows": None}, {}, {"numInputRows": 5}]
    assert input_rows(progress) == 15
    assert input_rows([]) == 0
