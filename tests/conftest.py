import os
from datetime import date
from pathlib import Path

import pytest

from complaint_canary import fixture
from complaint_canary.config import Settings, get_settings

DAY3 = date(2026, 10, 2)


@pytest.fixture(scope="session")
def fixture_files(tmp_path_factory) -> dict[str, Path]:
    return fixture.write_fixture(tmp_path_factory.mktemp("fixture"))


@pytest.fixture
def make_settings(tmp_path, fixture_files):
    """Settings for an isolated DuckDB file and landing directory."""

    def factory(cfpb_file: Path | None = None, **overrides) -> Settings:
        values = {
            "env": "local",
            "duckdb_path": tmp_path / "warehouse.duckdb",
            "landing_uri": str(tmp_path / "landing"),
            "cfpb_url": str(cfpb_file or fixture_files["day1"]),
            "fdic_url": str(fixture_files["fdic"]),
            "census_url": str(fixture_files["census"]),
            **overrides,
        }
        return Settings(_env_file=None, **values)

    return factory


def use_environment(settings: Settings) -> None:
    """Point the process-wide settings (used by the Dagster assets) at ``settings``."""
    os.environ.update(
        {
            "CANARY_ENV": settings.env,
            "CANARY_DUCKDB_PATH": str(settings.duckdb_path),
            "CANARY_LANDING_URI": settings.landing_uri,
            "CANARY_CFPB_URL": settings.cfpb_url,
            "CANARY_FDIC_URL": settings.fdic_url,
            "CANARY_CENSUS_URL": settings.census_url,
            "CANARY_GATE_OVERRIDE": str(settings.gate_override).lower(),
        }
    )
    get_settings.cache_clear()
