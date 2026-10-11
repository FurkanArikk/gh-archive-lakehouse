"""Create the Unity Catalog schemas and volumes the pipeline needs. Safe to rerun.

Uses the Unity Catalog REST API through the Databricks SDK, so it needs no running
compute and costs nothing against the serverless quota. Objects that already exist are
left untouched.

    python -m gh_lakehouse.workspace_setup
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING

from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType

from gh_lakehouse.config import Settings, load_settings

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

log = logging.getLogger(__name__)

SCHEMA_COMMENTS = {
    "raw_schema": "GH Archive landing files and streaming checkpoints",
    "bronze_schema": "Raw GH Archive events, payload kept as a JSON string",
    "silver_schema": "Typed, deduplicated events per type, plus quarantine",
    "gold_schema": "Daily aggregates for the dashboard",
}


def ensure_schema(w: WorkspaceClient, catalog: str, name: str, comment: str) -> bool:
    """Create ``catalog.name`` if missing. Returns True if it was created."""
    try:
        w.schemas.get(f"{catalog}.{name}")
        return False
    except NotFound:
        w.schemas.create(name=name, catalog_name=catalog, comment=comment)
        return True


def ensure_volume(w: WorkspaceClient, catalog: str, schema: str, name: str) -> bool:
    """Create managed volume ``catalog.schema.name`` if missing. Returns True if created."""
    try:
        w.volumes.read(f"{catalog}.{schema}.{name}")
        return False
    except NotFound:
        w.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=name,
            volume_type=VolumeType.MANAGED,
        )
        return True


def setup_workspace(w: WorkspaceClient, settings: Settings) -> list[str]:
    """Ensure every schema and volume exists. Returns the names of objects it created."""
    created: list[str] = []
    for field, comment in SCHEMA_COMMENTS.items():
        schema = getattr(settings, field)
        full = f"{settings.catalog}.{schema}"
        if ensure_schema(w, settings.catalog, schema, comment):
            created.append(full)
            log.info("created schema %s", full)
        else:
            log.info("schema %s already exists", full)

    # Managed volumes: Databricks owns the storage location, so there's no cloud bucket or
    # credential to configure, and dropping the volume deletes the files.
    for volume in (settings.landing_volume, settings.checkpoint_volume):
        full = f"{settings.catalog}.{settings.raw_schema}.{volume}"
        if ensure_volume(w, settings.catalog, settings.raw_schema, volume):
            created.append(full)
            log.info("created volume %s", full)
        else:
            log.info("volume %s already exists", full)
    return created


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")

    from databricks.sdk import WorkspaceClient

    settings = load_settings()
    created = setup_workspace(WorkspaceClient(), settings)
    log.info("done: %d object(s) created", len(created))
    return 0


if __name__ == "__main__":
    sys.exit(main())
