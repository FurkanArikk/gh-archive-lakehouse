from types import SimpleNamespace

from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType

from gh_lakehouse.config import Settings
from gh_lakehouse.workspace_setup import setup_workspace

SETTINGS = Settings(
    catalog="workspace",
    raw_schema="gh_raw",
    bronze_schema="gh_bronze",
    silver_schema="gh_silver",
    gold_schema="gh_gold",
    landing_volume="landing",
    checkpoint_volume="checkpoints",
)


class FakeSchemas:
    def __init__(self, existing: set[str]):
        self.existing = existing

    def get(self, full_name: str) -> None:
        if full_name not in self.existing:
            raise NotFound(full_name)

    def create(self, name: str, catalog_name: str, comment: str) -> None:
        assert comment
        self.existing.add(f"{catalog_name}.{name}")


class FakeVolumes:
    def __init__(self, existing: set[str]):
        self.existing = existing

    def read(self, name: str) -> None:
        if name not in self.existing:
            raise NotFound(name)

    def create(self, catalog_name: str, schema_name: str, name: str, volume_type: VolumeType):
        assert volume_type == VolumeType.MANAGED
        self.existing.add(f"{catalog_name}.{schema_name}.{name}")


def fake_client(schemas: set[str] | None = None, volumes: set[str] | None = None):
    return SimpleNamespace(
        schemas=FakeSchemas(schemas or set()), volumes=FakeVolumes(volumes or set())
    )


def test_creates_everything_on_an_empty_catalog():
    w = fake_client()
    created = setup_workspace(w, SETTINGS)
    assert created == [
        "workspace.gh_raw",
        "workspace.gh_bronze",
        "workspace.gh_silver",
        "workspace.gh_gold",
        "workspace.gh_raw.landing",
        "workspace.gh_raw.checkpoints",
    ]


def test_rerun_creates_nothing():
    w = fake_client()
    setup_workspace(w, SETTINGS)
    assert setup_workspace(w, SETTINGS) == []


def test_only_missing_objects_are_created():
    w = fake_client(schemas={"workspace.gh_raw"}, volumes={"workspace.gh_raw.landing"})
    created = setup_workspace(w, SETTINGS)
    assert "workspace.gh_raw" not in created
    assert "workspace.gh_raw.landing" not in created
    assert "workspace.gh_raw.checkpoints" in created
