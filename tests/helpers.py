"""Shared test helpers for working with the real-data fixture."""

import json
from pathlib import Path

FIXTURE = Path(__file__).parent / "fixtures" / "gharchive_sample.jsonl"
FIXTURE_LINES = FIXTURE.read_text().splitlines()
FIXTURE_DAY = "2026-10-06"


def lines_of(event_type: str) -> list[str]:
    return [line for line in FIXTURE_LINES if f'"type":"{event_type}"' in line]


def events_of(event_type: str) -> list[dict]:
    """Fixture events of one type, deduplicated on id (the fixture has one duplicate)."""
    return list({e["id"]: e for e in map(json.loads, lines_of(event_type))}.values())


def edit(line: str, **changes: object) -> str:
    """Return the event with top-level fields replaced; nested dicts are merged."""
    event = json.loads(line)
    for key, value in changes.items():
        if isinstance(value, dict) and isinstance(event.get(key), dict):
            event[key] = {**event[key], **value}
        else:
            event[key] = value
    return json.dumps(event)
