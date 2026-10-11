"""Project configuration.

Unity Catalog names are defined once, as variable defaults in databricks.yml. Values are
resolved in this order, later sources winning:

1. ``variables.<name>.default`` in databricks.yml, when the file is present (locally, CI)
2. environment variables ``GHL_<NAME>``, e.g. ``GHL_CATALOG`` (from .env in the container)
3. explicit overrides, e.g. parameters the Databricks job passes to each task

On Databricks the bundle file isn't next to the code, so the job passes every value
explicitly and step 1 is skipped.
"""

from __future__ import annotations

import argparse
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

import yaml

ENV_PREFIX = "GHL_"
BUNDLE_FILE_ENV = "GHL_BUNDLE_FILE"
DEFAULT_BUNDLE_FILE = "databricks.yml"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(ValueError):
    """Configuration is missing or invalid."""


def _check_identifier(label: str, value: object) -> None:
    # These names are interpolated into SQL and volume paths. Allowing only plain
    # identifiers is simpler and safer than quoting them everywhere.
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise ConfigError(f"{label}={value!r} is not a valid identifier (letters, digits, _)")


@dataclass(frozen=True)
class Settings:
    catalog: str
    raw_schema: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    landing_volume: str
    checkpoint_volume: str

    def __post_init__(self) -> None:
        for f in fields(self):
            _check_identifier(f.name, getattr(self, f.name))

    @property
    def landing_path(self) -> str:
        """Root of the landing volume, e.g. ``/Volumes/workspace/gh_raw/landing``."""
        return f"/Volumes/{self.catalog}/{self.raw_schema}/{self.landing_volume}"

    @property
    def checkpoint_path(self) -> str:
        """Root of the checkpoint volume; each stream gets a subdirectory."""
        return f"/Volumes/{self.catalog}/{self.raw_schema}/{self.checkpoint_volume}"

    def bronze_table(self, name: str) -> str:
        return self._table(self.bronze_schema, name)

    def silver_table(self, name: str) -> str:
        return self._table(self.silver_schema, name)

    def gold_table(self, name: str) -> str:
        return self._table(self.gold_schema, name)

    def _table(self, schema: str, name: str) -> str:
        _check_identifier("table name", name)
        return f"{self.catalog}.{schema}.{name}"


SETTING_NAMES: tuple[str, ...] = tuple(f.name for f in fields(Settings))


def _bundle_defaults(path: Path) -> dict[str, str]:
    """Read ``variables.<name>.default`` for every known setting in a bundle file."""
    doc: Any = yaml.safe_load(path.read_text()) or {}
    variables = doc.get("variables") or {}
    if not isinstance(variables, dict):
        raise ConfigError(f"{path}: 'variables' must be a mapping")
    defaults: dict[str, str] = {}
    for name in SETTING_NAMES:
        spec = variables.get(name)
        if isinstance(spec, dict) and spec.get("default") is not None:
            defaults[name] = str(spec["default"])
    return defaults


def load_settings(
    overrides: Mapping[str, str | None] | None = None,
    *,
    bundle_file: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> Settings:
    """Resolve settings from the bundle file, ``GHL_*`` env vars and explicit overrides.

    ``None`` values in ``overrides`` are ignored, so optional CLI arguments can be passed
    straight through.
    """
    env = os.environ if environ is None else environ
    overrides = overrides or {}

    unknown = sorted(set(overrides) - set(SETTING_NAMES))
    if unknown:
        raise ConfigError(f"unknown setting(s): {', '.join(unknown)}")

    path = bundle_file or Path(env.get(BUNDLE_FILE_ENV, DEFAULT_BUNDLE_FILE))
    values: dict[str, str] = _bundle_defaults(path) if path.is_file() else {}

    for name in SETTING_NAMES:
        if env_value := env.get(ENV_PREFIX + name.upper()):
            values[name] = env_value
    values.update({k: v for k, v in overrides.items() if v is not None})

    missing = [name for name in SETTING_NAMES if name not in values]
    if missing:
        raise ConfigError(
            f"missing setting(s): {', '.join(missing)}; looked in {path}, "
            f"{ENV_PREFIX}* environment variables and explicit overrides"
        )
    return Settings(**{name: values[name] for name in SETTING_NAMES})


def add_settings_args(parser: argparse.ArgumentParser) -> None:
    """Add an optional ``--<setting>`` flag per setting (e.g. ``--bronze-schema``)."""
    group = parser.add_argument_group("settings (override databricks.yml defaults)")
    for name in SETTING_NAMES:
        group.add_argument(f"--{name.replace('_', '-')}", dest=name, default=None)


def settings_from_args(args: argparse.Namespace) -> Settings:
    return load_settings({name: getattr(args, name) for name in SETTING_NAMES})
