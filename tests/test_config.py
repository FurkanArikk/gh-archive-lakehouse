from pathlib import Path

import pytest

from gh_lakehouse.config import SETTING_NAMES, ConfigError, Settings, load_settings

BUNDLE = Path(__file__).resolve().parents[1] / "databricks.yml"
ALL_VALUES = {name: f"v_{name}" for name in SETTING_NAMES}


def test_defaults_come_from_the_real_bundle_file():
    s = load_settings(bundle_file=BUNDLE, environ={})
    assert s == Settings(
        catalog="workspace",
        raw_schema="gh_raw",
        bronze_schema="gh_bronze",
        silver_schema="gh_silver",
        gold_schema="gh_gold",
        landing_volume="landing",
        checkpoint_volume="checkpoints",
    )


def test_env_overrides_bundle_and_explicit_overrides_env():
    env = {"GHL_CATALOG": "from_env", "GHL_GOLD_SCHEMA": "gold_env"}
    s = load_settings({"catalog": "from_job"}, bundle_file=BUNDLE, environ=env)
    assert s.catalog == "from_job"
    assert s.gold_schema == "gold_env"
    assert s.bronze_schema == "gh_bronze"


def test_none_overrides_are_ignored():
    s = load_settings({"catalog": None}, bundle_file=BUNDLE, environ={})
    assert s.catalog == "workspace"


def test_without_bundle_file_all_values_must_be_explicit(tmp_path: Path):
    missing = tmp_path / "nope.yml"
    s = load_settings(ALL_VALUES, bundle_file=missing, environ={})
    assert s.catalog == "v_catalog"

    with pytest.raises(ConfigError, match="missing setting"):
        load_settings({"catalog": "x"}, bundle_file=missing, environ={})


def test_bundle_file_location_can_come_from_env(tmp_path: Path):
    alt = tmp_path / "alt.yml"
    alt.write_text(BUNDLE.read_text().replace("default: workspace", "default: other"))
    s = load_settings(environ={"GHL_BUNDLE_FILE": str(alt)})
    assert s.catalog == "other"


@pytest.mark.parametrize("bad", ["gh-bronze", "x; DROP TABLE y", "", "1abc", "a.b"])
def test_rejects_non_identifiers(bad: str):
    with pytest.raises(ConfigError, match="not a valid identifier"):
        load_settings({"bronze_schema": bad}, bundle_file=BUNDLE, environ={})


def test_rejects_unknown_override_keys():
    with pytest.raises(ConfigError, match="unknown setting"):
        load_settings({"catalgo": "x"}, bundle_file=BUNDLE, environ={})


def test_paths_and_table_names():
    s = load_settings(bundle_file=BUNDLE, environ={})
    assert s.landing_path == "/Volumes/workspace/gh_raw/landing"
    assert s.checkpoint_path == "/Volumes/workspace/gh_raw/checkpoints"
    assert s.bronze_table("events") == "workspace.gh_bronze.events"
    assert s.silver_table("quarantine") == "workspace.gh_silver.quarantine"
    assert s.gold_table("daily_bot_share") == "workspace.gh_gold.daily_bot_share"
    with pytest.raises(ConfigError):
        s.gold_table("bad-name")


def test_settings_from_command_line_flags(monkeypatch):
    import argparse

    from gh_lakehouse.config import add_settings_args, settings_from_args

    monkeypatch.chdir(BUNDLE.parent)  # databricks.yml supplies the defaults
    parser = argparse.ArgumentParser()
    add_settings_args(parser)

    args = parser.parse_args(["--catalog=other", "--bronze-schema", "b"])
    s = settings_from_args(args)
    assert (s.catalog, s.bronze_schema, s.silver_schema) == ("other", "b", "gh_silver")
