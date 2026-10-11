import gzip
import threading
from collections.abc import Iterator
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from gh_lakehouse.ingestion import (
    USER_AGENT,
    HourFile,
    HttpSource,
    Ingestor,
    Status,
    TransientError,
    check_gzip,
    hours_between,
    summarize,
    with_retries,
)

FIXTURE = Path(__file__).parent / "fixtures" / "gharchive_sample.jsonl"
BASE = "https://example.test"
ROOT = "/Volumes/c/raw/landing"
H3 = HourFile(date(2026, 10, 6), 3)
H15 = HourFile(date(2026, 10, 6), 15)


@pytest.fixture(scope="module")
def gz() -> bytes:
    """A valid GH Archive-style file made from real events."""
    return gzip.compress(FIXTURE.read_bytes())


class FakeSource:
    def __init__(self, files: dict[str, bytes], failures: dict[str, list[Exception]] | None = None):
        self.files = files
        self.failures = failures or {}  # raised in order by download(), then succeeds
        self.downloads: list[str] = []

    def size(self, url: str) -> int | None:
        name = url.rsplit("/", 1)[1]
        return len(self.files[name]) if name in self.files else None

    def download(self, url: str, dest: Path) -> int:
        name = url.rsplit("/", 1)[1]
        self.downloads.append(name)
        if self.failures.get(name):
            failure = self.failures[name].pop(0)
            if isinstance(failure, bytes):  # simulate a truncated/corrupt body
                dest.write_bytes(failure)
                return len(failure)
            raise failure
        dest.write_bytes(self.files[name])
        return len(self.files[name])


class FakeStore:
    def __init__(self, files: dict[str, bytes] | None = None, truncate_uploads: bool = False):
        self.files = dict(files or {})
        self.uploads: list[tuple[str, bool]] = []
        self.truncate_uploads = truncate_uploads

    def size(self, path: str) -> int | None:
        return len(self.files[path]) if path in self.files else None

    def upload(self, path: str, src: Path, *, overwrite: bool) -> None:
        if path in self.files and not overwrite:
            raise FileExistsError(path)
        data = src.read_bytes()
        self.files[path] = data[:-1] if self.truncate_uploads else data
        self.uploads.append((path, overwrite))


def make_ingestor(source, store, tmp_path: Path, **kw) -> tuple[Ingestor, list[float]]:
    sleeps: list[float] = []
    ingestor = Ingestor(
        source=source,
        store=store,
        landing_root=ROOT,
        workdir=tmp_path,
        base_url=BASE,
        attempts=3,
        base_delay=1.0,
        sleep=sleeps.append,
        **kw,
    )
    return ingestor, sleeps


# --------------------------------------------------------------------------- planning


def test_hour_is_not_zero_padded():
    assert H3.name == "2026-10-06-3.json.gz"
    assert H3.url() == "https://data.gharchive.org/2026-10-06-3.json.gz"
    assert H15.name == "2026-10-06-15.json.gz"


def test_landing_path_is_partitioned_by_date():
    assert H3.landing_path(ROOT) == f"{ROOT}/gharchive/date=2026-10-06/2026-10-06-3.json.gz"


def test_hours_between_covers_whole_days():
    one_day = hours_between(date(2026, 10, 6), date(2026, 10, 6))
    assert len(one_day) == 24
    assert one_day[0] == HourFile(date(2026, 10, 6), 0)
    assert one_day[-1] == HourFile(date(2026, 10, 6), 23)

    two_days = hours_between(date(2026, 10, 6), date(2026, 10, 7))
    assert len(two_days) == 48
    assert two_days[24] == HourFile(date(2026, 10, 7), 0)


def test_bad_ranges_are_rejected():
    with pytest.raises(ValueError, match="before start"):
        hours_between(date(2026, 10, 7), date(2026, 10, 6))
    with pytest.raises(ValueError, match="0-23"):
        HourFile(date(2026, 10, 6), 24)


# --------------------------------------------------------------------------- orchestration


def test_first_run_uploads_and_rerun_skips(gz, tmp_path):
    source, store = FakeSource({H3.name: gz, H15.name: gz}), FakeStore()
    ingestor, _ = make_ingestor(source, store, tmp_path)

    first = ingestor.run([H3, H15], workers=2)
    assert [r.status for r in first] == [Status.UPLOADED, Status.UPLOADED]
    assert store.files[H3.landing_path(ROOT)] == gz

    second = ingestor.run([H3, H15], workers=2)
    assert [r.status for r in second] == [Status.SKIPPED, Status.SKIPPED]
    assert len(source.downloads) == 2  # nothing downloaded the second time
    assert list(tmp_path.iterdir()) == []  # temp files cleaned up


def test_partial_file_in_volume_is_replaced(gz, tmp_path):
    target = H3.landing_path(ROOT)
    store = FakeStore({target: gz[:100]})
    ingestor, _ = make_ingestor(FakeSource({H3.name: gz}), store, tmp_path)

    result = ingestor.ingest_hour(H3)

    assert result.status == Status.UPLOADED
    assert result.detail == "replaced"
    assert store.uploads == [(target, True)]
    assert store.files[target] == gz


def test_unpublished_hour_is_missing_not_failed(tmp_path):
    ingestor, sleeps = make_ingestor(FakeSource({}), FakeStore(), tmp_path)
    result = ingestor.ingest_hour(H3)
    assert result.status == Status.MISSING
    assert sleeps == []  # a 404 is an answer, not something to retry


def test_transient_errors_are_retried_with_backoff(gz, tmp_path):
    source = FakeSource({H3.name: gz}, {H3.name: [TransientError("503"), TransientError("reset")]})
    ingestor, sleeps = make_ingestor(source, FakeStore(), tmp_path)

    result = ingestor.ingest_hour(H3)

    assert result.status == Status.UPLOADED
    assert sleeps == [1.0, 2.0]
    assert len(source.downloads) == 3


def test_corrupt_download_is_retried(gz, tmp_path):
    source = FakeSource({H3.name: gz}, {H3.name: [gz[: len(gz) // 2]]})
    ingestor, sleeps = make_ingestor(source, FakeStore(), tmp_path)

    assert ingestor.ingest_hour(H3).status == Status.UPLOADED
    assert sleeps == [1.0]


def test_one_failing_hour_does_not_stop_the_others(gz, tmp_path):
    failures = {H3.name: [TransientError("503")] * 3}
    store = FakeStore()
    ingestor, _ = make_ingestor(FakeSource({H3.name: gz, H15.name: gz}, failures), store, tmp_path)

    results = {r.file: r for r in ingestor.run([H3, H15])}

    assert results[H3].status == Status.FAILED
    assert "503" in results[H3].detail
    assert results[H15].status == Status.UPLOADED
    assert H3.landing_path(ROOT) not in store.files


def test_upload_is_verified_by_reading_size_back(gz, tmp_path):
    ingestor, _ = make_ingestor(
        FakeSource({H3.name: gz}), FakeStore(truncate_uploads=True), tmp_path
    )
    result = ingestor.ingest_hour(H3)
    assert result.status == Status.FAILED
    assert "uploaded size" in result.detail


def test_dry_run_changes_nothing(gz, tmp_path):
    source, store = FakeSource({H3.name: gz}), FakeStore()
    ingestor, _ = make_ingestor(source, store, tmp_path, dry_run=True)

    assert ingestor.ingest_hour(H3).status == Status.PLANNED
    assert source.downloads == []
    assert store.files == {}


def test_summary_counts_statuses(gz, tmp_path):
    ingestor, _ = make_ingestor(FakeSource({H3.name: gz}), FakeStore(), tmp_path)
    results = ingestor.run([H3, H15])
    assert summarize(results).startswith("2 hour(s): 1 uploaded, 1 missing;")


# --------------------------------------------------------------------------- helpers


def test_with_retries_does_not_retry_other_errors():
    calls = []

    def boom() -> None:
        calls.append(1)
        raise ValueError("bug")

    with pytest.raises(ValueError):
        with_retries(boom, label="x", attempts=5, base_delay=1, sleep=lambda _: None)
    assert len(calls) == 1


def test_check_gzip_accepts_real_file_and_rejects_truncated(gz, tmp_path):
    good, bad = tmp_path / "good.gz", tmp_path / "bad.gz"
    good.write_bytes(gz)
    bad.write_bytes(gz[:-20])
    check_gzip(good)
    with pytest.raises(TransientError, match="corrupt gzip"):
        check_gzip(bad)


# --------------------------------------------------------------------------- HttpSource


@pytest.fixture
def http_server(gz) -> Iterator[tuple[str, list[str]]]:
    """Local server: /ok.json.gz -> 200, /busy.json.gz -> 503, anything else -> 404."""
    user_agents: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def _respond(self, body: bool) -> None:
            user_agents.append(self.headers["User-Agent"])
            if self.path == "/ok.json.gz":
                self.send_response(200)
                self.send_header("Content-Type", "application/gzip")
                self.send_header("Content-Length", str(len(gz)))
                self.end_headers()
                if body:
                    self.wfile.write(gz)
            else:
                self.send_response(503 if self.path == "/busy.json.gz" else 404)
                self.send_header("Content-Length", "0")
                self.end_headers()

        def do_HEAD(self) -> None:
            self._respond(body=False)

        def do_GET(self) -> None:
            self._respond(body=True)

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", user_agents
    server.shutdown()


def test_http_source_size_download_and_errors(http_server, gz, tmp_path):
    base, user_agents = http_server
    source = HttpSource()

    assert source.size(f"{base}/ok.json.gz") == len(gz)
    assert source.size(f"{base}/nope.json.gz") is None

    dest = tmp_path / "ok.json.gz"
    assert source.download(f"{base}/ok.json.gz", dest) == len(gz)
    assert dest.read_bytes() == gz

    with pytest.raises(TransientError, match="503"):
        source.size(f"{base}/busy.json.gz")
    assert set(user_agents) == {USER_AGENT}


def test_http_source_connection_error_is_transient():
    with pytest.raises(TransientError):
        HttpSource(timeout=(0.5, 0.5)).size("http://127.0.0.1:9/x.json.gz")
