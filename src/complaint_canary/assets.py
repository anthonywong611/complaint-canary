"""Dagster assets: ingestion, the dbt project, and publication.

Dagster inspects the ``context`` annotation at import time, so this module must not
use ``from __future__ import annotations``.
"""

import json
import os
import subprocess
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, Failure, FreshnessPolicy, MaterializeResult, asset
from dagster_dbt import DbtCliResource, dbt_assets, get_asset_key_for_model

from complaint_canary import pipeline
from complaint_canary.config import DBT_PROJECT_DIR, Settings, get_settings
from complaint_canary.dbt_runner import GATE_SELECTOR, _dbt_executable, dbt_vars, summarize
from complaint_canary.ledger import OutOfOrderSnapshot
from complaint_canary.storage import Landing, get_landing
from complaint_canary.warehouse import Warehouse, get_warehouse

OWNERS = ["team:data-platform"]
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"
_DBT_SOURCES = ("models", "seeds", "snapshots", "macros", "tests", "dbt_project.yml", "profiles.yml")


def ensure_manifest() -> Path:
    """Parse the dbt project when the manifest is missing or older than the project files."""
    newest = max((path.stat().st_mtime for name in _DBT_SOURCES for path in _walk(DBT_PROJECT_DIR / name)), default=0.0)
    if not MANIFEST_PATH.exists() or MANIFEST_PATH.stat().st_mtime < newest:
        get_settings().export_env()
        subprocess.run(
            [
                *_dbt_executable(),
                "parse",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
                "--target-path",
                str(MANIFEST_PATH.parent),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )
    return MANIFEST_PATH


def _walk(path: Path) -> Iterator[Path]:
    if path.is_file():
        yield path
    elif path.is_dir():
        yield from (child for child in path.rglob("*") if child.is_file())


def runtime() -> tuple[Settings, Warehouse, Landing]:
    settings = get_settings()
    settings.export_env()
    return settings, get_warehouse(settings), get_landing(settings.landing_uri)


def _metadata(result: dict[str, Any]) -> dict[str, Any]:
    return {key: str(value) if not isinstance(value, (int, float, bool)) else value for key, value in result.items()}


@asset(
    key=["landing", "cfpb_complaints"],
    group_name="ingestion",
    kinds={"python", "parquet"},
    owners=OWNERS,
    tags={"layer": "landing", "source": "cfpb"},
    description="Today's CFPB bulk file, checked and converted to Parquet in the landing zone.",
)
def cfpb_landing(context: AssetExecutionContext) -> MaterializeResult:
    settings, warehouse, landing = runtime()
    try:
        result = pipeline.ingest_cfpb(settings, warehouse, landing, context.run_id)
    except OutOfOrderSnapshot as exc:
        raise Failure(description=str(exc)) from exc
    return MaterializeResult(metadata=_metadata(result))


@asset(
    key=["raw", "complaints_snapshot"],
    deps=[cfpb_landing],
    group_name="ingestion",
    kinds={"python"},
    owners=OWNERS,
    tags={"layer": "raw", "source": "cfpb"},
    description="The snapshot loaded into the raw table and passed through the integrity gate.",
)
def raw_complaints_snapshot(context: AssetExecutionContext) -> MaterializeResult:
    settings, warehouse, _ = runtime()
    try:
        result = pipeline.load_and_gate(settings, warehouse, context.run_id)
    except pipeline.SnapshotRejected as exc:
        raise Failure(description=str(exc)) from exc
    return MaterializeResult(metadata=_metadata(result))


@asset(
    key=["raw", "fdic_institutions"],
    group_name="ingestion",
    kinds={"python"},
    owners=OWNERS,
    tags={"layer": "raw", "source": "fdic"},
    description="Active FDIC institutions. Refreshed when the last load is more than a week old.",
)
def raw_fdic_institutions(context: AssetExecutionContext) -> MaterializeResult:
    settings, warehouse, landing = runtime()
    today = datetime.now(UTC).date()
    return MaterializeResult(
        metadata=_metadata(pipeline.refresh_fdic(settings, warehouse, landing, context.run_id, today))
    )


@asset(
    key=["raw", "census_state_population"],
    group_name="ingestion",
    kinds={"python"},
    owners=OWNERS,
    tags={"layer": "raw", "source": "census"},
    description="Census state population estimates. Refreshed when the last load is more than a year old.",
)
def raw_census_state_population(context: AssetExecutionContext) -> MaterializeResult:
    settings, warehouse, landing = runtime()
    today = datetime.now(UTC).date()
    return MaterializeResult(
        metadata=_metadata(pipeline.refresh_census(settings, warehouse, landing, context.run_id, today))
    )


@dbt_assets(manifest=ensure_manifest(), exclude=GATE_SELECTOR)
def canary_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Snapshot, staging, intermediate and mart models, each followed by its tests."""
    settings, warehouse, _ = runtime()
    snapshot_date = pipeline.current_snapshot_date(warehouse)
    args = ["build", "--vars", json.dumps(dbt_vars(settings, snapshot_date))]
    if settings.dbt_full_refresh:
        args.append("--full-refresh")
    started = time.monotonic()
    invocation = dbt.cli(args, context=context)
    try:
        yield from invocation.stream()
    finally:
        try:
            run_results = invocation.get_artifact("run_results.json")
        except Exception:  # dbt stopped before writing results
            run_results = None
        summary = summarize(run_results)
        pipeline.after_build(settings, warehouse, context.run_id, snapshot_date, summary, started)


@asset(
    key=["ops", "published_snapshot"],
    deps=[
        get_asset_key_for_model([canary_dbt_assets], name)
        for name in ("alert_digest", "fct_scam_alerts", "fct_complaints", "snapshot_changes", "publication_lag")
    ],
    group_name="publication",
    kinds={"python"},
    owners=OWNERS,
    tags={"layer": "ops"},
    freshness_policy=FreshnessPolicy.time_window(fail_window=timedelta(hours=26)),
    description="Marks the snapshot published, writes the scam-signal digest and emits the success event.",
)
def published_snapshot(context: AssetExecutionContext) -> MaterializeResult:
    settings, warehouse, landing = runtime()
    return MaterializeResult(metadata=_metadata(pipeline.publish(settings, warehouse, landing, context.run_id)))
