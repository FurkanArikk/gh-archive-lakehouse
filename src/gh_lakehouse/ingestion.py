"""Download GH Archive hourly files and upload them to the Unity Catalog landing volume.

Runs locally (in the dev container), because outbound internet from a Free Edition
workspace is restricted.

    python -m gh_lakehouse.ingestion --start 2026-10-06 [--end 2026-10-07] [--dry-run]

Each hour is handled independently:

1. ``HEAD`` the GH Archive URL. A 404 means the hour isn't published (or never will be);
   it's reported as missing rather than retried.
2. If the volume already holds a file of exactly that size, skip it. A file with a
   different size (an interrupted upload, say) is downloaded again and replaced.
3. Download to a temp file, check the byte count and gzip integrity, upload, then read
   the uploaded size back to confirm it.

Network errors, 429 and 5xx responses, truncated downloads and corrupt gzip are retried
with exponential backoff.
"""

from __future__ import annotations

import argparse
import gzip
import logging
import sys
import tempfile
import time
import zlib
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import requests
    from databricks.sdk import WorkspaceClient

log = logging.getLogger(__name__)

GHARCHIVE_BASE_URL = "https://data.gharchive.org"
# GH Archive rejects some generic client user agents (urllib's gets 403); identify ourselves.
USER_AGENT = "gh-lakehouse-ingestion/0.1"
LANDING_PREFIX = "gharchive"
_CHUNK = 1 << 20


# --------------------------------------------------------------------------- planning


@dataclass(frozen=True)
class HourFile:
    """One GH Archive hourly file, identified by its UTC hour."""

    day: date
    hour: int

    def __post_init__(self) -> None:
        if not 0 <= self.hour <= 23:
            raise ValueError(f"hour must be 0-23, got {self.hour}")

    @property
    def name(self) -> str:
        # GH Archive doesn't zero-pad the hour: 2026-10-06-3.json.gz, not -03.
        return f"{self.day:%Y-%m-%d}-{self.hour}.json.gz"

    @property
    def start(self) -> datetime:
        return datetime(self.day.year, self.day.month, self.day.day, self.hour, tzinfo=UTC)

    def url(self, base_url: str = GHARCHIVE_BASE_URL) -> str:
        return f"{base_url}/{self.name}"

    def landing_path(self, landing_root: str) -> str:
        """``<root>/gharchive/date=YYYY-MM-DD/<name>``.

        One folder per day keeps directory listings small for Auto Loader, and the
        ``date=`` form is the Hive partition convention Spark understands.
        """
        return f"{landing_root}/{LANDING_PREFIX}/date={self.day:%Y-%m-%d}/{self.name}"


def hours_between(start: date, end: date) -> list[HourFile]:
    """Every hour from ``start`` 00:00 to ``end`` 23:00 UTC, inclusive."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    days = (end - start).days + 1
    return [HourFile(start + timedelta(days=d), h) for d in range(days) for h in range(24)]


# --------------------------------------------------------------------------- interfaces


class TransientError(Exception):
    """A failure worth retrying: network trouble, 429/5xx, truncated or corrupt data."""


class Source(Protocol):
    def size(self, url: str) -> int | None:
        """Size in bytes, or None if the file doesn't exist."""
        ...

    def download(self, url: str, dest: Path) -> int:
        """Write the file to ``dest`` and return the number of bytes written."""
        ...


class Store(Protocol):
    def size(self, path: str) -> int | None:
        """Size in bytes, or None if the file doesn't exist."""
        ...

    def upload(self, path: str, src: Path, *, overwrite: bool) -> None: ...


class HttpSource:
    """GH Archive over HTTPS."""

    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: tuple[float, float] = (10, 120),
    ) -> None:
        import requests

        self._requests = requests
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.timeout = timeout

    def _check(self, response: requests.Response) -> None:
        if response.status_code == 429 or response.status_code >= 500:
            raise TransientError(f"HTTP {response.status_code} from {response.url}")
        response.raise_for_status()

    def size(self, url: str) -> int | None:
        try:
            r = self.session.head(url, timeout=self.timeout, allow_redirects=True)
        except self._requests.RequestException as exc:
            raise TransientError(f"HEAD {url}: {exc}") from exc
        if r.status_code == 404:
            return None
        self._check(r)
        return int(r.headers["Content-Length"])

    def download(self, url: str, dest: Path) -> int:
        try:
            with self.session.get(url, stream=True, timeout=self.timeout) as r:
                self._check(r)
                written = 0
                with dest.open("wb") as out:
                    # decode_content=False: keep the gzip bytes exactly as served.
                    for chunk in r.raw.stream(_CHUNK, decode_content=False):
                        out.write(chunk)
                        written += len(chunk)
        except self._requests.RequestException as exc:
            raise TransientError(f"GET {url}: {exc}") from exc
        expected = r.headers.get("Content-Length")
        if expected is not None and int(expected) != written:
            raise TransientError(f"GET {url}: got {written} of {expected} bytes")
        return written


class VolumeStore:
    """A Unity Catalog volume, through the Databricks Files API."""

    def __init__(self, client: WorkspaceClient) -> None:
        from databricks.sdk.errors import NotFound

        self.client = client
        self._not_found = NotFound

    def size(self, path: str) -> int | None:
        try:
            return self.client.files.get_metadata(path).content_length
        except self._not_found:
            return None

    def upload(self, path: str, src: Path, *, overwrite: bool) -> None:
        # Idempotent; creates the date folder (and parents) on first use.
        self.client.files.create_directory(path.rsplit("/", 1)[0])
        with src.open("rb") as f:
            self.client.files.upload(path, f, overwrite=overwrite)


# --------------------------------------------------------------------------- orchestration


class Status(StrEnum):
    UPLOADED = "uploaded"
    SKIPPED = "skipped"
    MISSING = "missing"
    FAILED = "failed"
    PLANNED = "planned"  # dry run: would upload


@dataclass(frozen=True)
class Result:
    file: HourFile
    status: Status
    bytes: int = 0
    seconds: float = 0.0
    detail: str = ""


def with_retries[T](
    fn: Callable[[], T],
    *,
    label: str,
    attempts: int,
    base_delay: float,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call ``fn``, retrying TransientError with exponential backoff (1x, 2x, 4x, ...)."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except TransientError as exc:
            if attempt == attempts:
                raise
            delay = base_delay * 2 ** (attempt - 1)
            log.warning(
                "%s: %s (attempt %d/%d, retry in %.0fs)", label, exc, attempt, attempts, delay
            )
            sleep(delay)
    raise AssertionError("unreachable")


def check_gzip(path: Path) -> None:
    """Read the whole file through gzip; raise TransientError if it's truncated or corrupt.

    Gzip ends with a CRC32 of the content, so reading to the end verifies every byte.
    """
    try:
        with gzip.open(path, "rb") as f:
            while f.read(_CHUNK):
                pass
    except (OSError, EOFError, zlib.error) as exc:
        raise TransientError(f"corrupt gzip {path.name}: {exc}") from exc


@dataclass
class Ingestor:
    source: Source
    store: Store
    landing_root: str
    workdir: Path
    base_url: str = GHARCHIVE_BASE_URL
    attempts: int = 5
    base_delay: float = 2.0
    dry_run: bool = False
    sleep: Callable[[float], None] = time.sleep

    def _retry[T](self, fn: Callable[[], T], label: str) -> T:
        return with_retries(
            fn, label=label, attempts=self.attempts, base_delay=self.base_delay, sleep=self.sleep
        )

    def ingest_hour(self, hf: HourFile) -> Result:
        started = time.monotonic()
        url, target = hf.url(self.base_url), hf.landing_path(self.landing_root)
        try:
            remote = self._retry(lambda: self.source.size(url), f"{hf.name} HEAD")
            if remote is None:
                return Result(hf, Status.MISSING, detail="not published (HTTP 404)")

            existing = self.store.size(target)
            if existing == remote:
                return Result(hf, Status.SKIPPED, existing, detail="already in volume")
            replacing = existing is not None
            if replacing:
                log.warning(
                    "%s: volume has %d bytes, source %d; replacing", hf.name, existing, remote
                )
            if self.dry_run:
                detail = "would replace" if replacing else ""
                return Result(hf, Status.PLANNED, remote, detail=detail)

            local = self.workdir / hf.name

            def fetch() -> int:
                n = self.source.download(url, local)
                check_gzip(local)
                return n

            try:
                size = self._retry(fetch, f"{hf.name} download")
                self.store.upload(target, local, overwrite=replacing)
            finally:
                local.unlink(missing_ok=True)

            uploaded = self.store.size(target)
            if uploaded != size:
                raise RuntimeError(f"uploaded size {uploaded} != downloaded size {size}")
            return Result(
                hf,
                Status.UPLOADED,
                size,
                time.monotonic() - started,
                "replaced" if replacing else "",
            )
        except Exception as exc:  # one bad hour must not stop the others
            return Result(hf, Status.FAILED, seconds=time.monotonic() - started, detail=str(exc))

    def run(self, hours: Iterable[HourFile], workers: int = 4) -> list[Result]:
        # Threads, not processes: the work is network-bound, so the GIL isn't a bottleneck.
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = []
            for r in pool.map(self.ingest_hour, hours):
                log_result(r)
                results.append(r)
        return results


def log_result(r: Result) -> None:
    mb = r.bytes / 1e6
    msg = f"{r.file.name:24s} {r.status:8s} {mb:7.1f} MB {r.seconds:6.1f}s {r.detail}".rstrip()
    level = logging.ERROR if r.status == Status.FAILED else logging.INFO
    if r.status == Status.MISSING:
        level = logging.WARNING
    log.log(level, msg)


def summarize(results: list[Result]) -> str:
    counts = Counter(r.status for r in results)
    total_mb = sum(r.bytes for r in results if r.status == Status.UPLOADED) / 1e6
    parts = [f"{counts[s]} {s}" for s in Status if counts[s]]
    return f"{len(results)} hour(s): {', '.join(parts)}; {total_mb:.1f} MB uploaded"


# --------------------------------------------------------------------------- CLI


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load GH Archive hours into the landing volume.")
    parser.add_argument("--start", type=_parse_date, required=True, help="first day, YYYY-MM-DD")
    parser.add_argument("--end", type=_parse_date, help="last day, inclusive (default: --start)")
    parser.add_argument("--workers", type=int, default=4, help="parallel hours (default 4)")
    parser.add_argument("--attempts", type=int, default=5, help="tries per request (default 5)")
    parser.add_argument("--dry-run", action="store_true", help="plan only; no download/upload")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    logging.getLogger("databricks.sdk").setLevel(logging.WARNING)

    from databricks.sdk import WorkspaceClient

    from gh_lakehouse.config import load_settings

    settings = load_settings()
    hours = hours_between(args.start, args.end or args.start)
    log.info(
        "%s %d hour(s) %s..%s -> %s",
        "planning" if args.dry_run else "ingesting",
        len(hours),
        hours[0].name,
        hours[-1].name,
        settings.landing_path,
    )

    with tempfile.TemporaryDirectory(prefix="gharchive-") as tmp:
        ingestor = Ingestor(
            source=HttpSource(),
            store=VolumeStore(WorkspaceClient()),
            landing_root=settings.landing_path,
            workdir=Path(tmp),
            attempts=args.attempts,
            dry_run=args.dry_run,
        )
        results = ingestor.run(hours, workers=args.workers)

    log.info(summarize(results))
    failed = [r for r in results if r.status == Status.FAILED]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
