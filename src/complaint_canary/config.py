"""Runtime settings, read from environment variables (prefix ``CANARY_``) or a ``.env`` file."""

from __future__ import annotations

import os
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = Path(os.environ.get("CANARY_DBT_PROJECT_DIR", REPO_ROOT / "dbt"))

CFPB_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
FDIC_URL = "https://api.fdic.gov/banks/institutions"
CENSUS_URL = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/state/totals/NST-EST2025-ALLDATA.csv"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CANARY_", env_file=".env", extra="ignore")

    env: Literal["local", "dev", "qa", "prod"] = "local"
    warehouse: Literal["duckdb", "bigquery"] = "duckdb"

    duckdb_path: Path = REPO_ROOT / "data" / "warehouse.duckdb"
    gcp_project: str = ""
    bq_location: str = "US"
    dataset_prefix: str = ""

    # A local directory or a gs://bucket/prefix URI.
    landing_uri: str = str(REPO_ROOT / "data" / "landing")

    # Each source accepts an https URL or a local file path (used by the fixture).
    cfpb_url: str = CFPB_URL
    fdic_url: str = FDIC_URL
    census_url: str = CENSUS_URL
    http_user_agent: str = ""

    sample_percent: int = Field(default=100, ge=1, le=100)
    snapshot_date: date | None = None

    dbt_target: str = ""
    dbt_full_refresh: bool = False
    gate_override: bool = False

    fdic_max_age_days: int = 7
    census_max_age_days: int = 365

    event_sink: Literal["stdout", "cloud_logging"] = "stdout"
    enable_schedules: bool = False
    scd2_backup_enabled: bool = False
    zip_policy_tag: str = ""
    git_sha: str = "unknown"

    @property
    def resolved_dbt_target(self) -> str:
        return self.dbt_target or self.env

    def schema(self, layer: str) -> str:
        """Physical schema (BigQuery dataset) for a logical layer such as ``raw`` or ``ops``."""
        return f"{self.dataset_prefix}{layer}"

    def export_env(self) -> None:
        """Expose the settings dbt reads through ``env_var`` to child processes."""
        os.environ["DBT_TARGET"] = self.resolved_dbt_target
        os.environ["CANARY_DUCKDB_PATH"] = str(self.duckdb_path)
        os.environ["CANARY_GCP_PROJECT"] = self.gcp_project
        os.environ["CANARY_BQ_LOCATION"] = self.bq_location
        os.environ["CANARY_DATASET_PREFIX"] = self.dataset_prefix
        os.environ["CANARY_ZIP_POLICY_TAG"] = self.zip_policy_tag


@lru_cache
def get_settings() -> Settings:
    return Settings()
