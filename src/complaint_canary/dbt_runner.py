"""Run dbt as a subprocess and read its results."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from complaint_canary.config import DBT_PROJECT_DIR, Settings

GATE_SELECTOR = "tag:integrity_gate"
SNAPSHOT_NODE = "snapshot.complaint_canary.snap_complaints"


@dataclass
class DbtSummary:
    """What one dbt invocation did, in the shape the ops tables store."""

    results: list[dict[str, Any]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    warned: list[str] = field(default_factory=list)
    bytes_billed: int = 0
    snapshot_applied: bool = False


@dataclass
class DbtResult:
    success: bool
    output: str
    summary: DbtSummary


def dbt_vars(settings: Settings, snapshot_date: date) -> dict[str, Any]:
    return {"snapshot_date": snapshot_date.isoformat(), "gate_override": settings.gate_override}


def summarize(run_results: dict[str, Any] | None) -> DbtSummary:
    summary = DbtSummary()
    for result in (run_results or {}).get("results", []):
        unique_id = result.get("unique_id", "")
        status = str(result.get("status", ""))
        response = result.get("adapter_response") or {}
        summary.bytes_billed += int(response.get("bytes_billed") or 0)
        if status in ("fail", "error", "runtime error"):
            summary.failed.append(unique_id)
        elif status == "warn":
            summary.warned.append(unique_id)
        if unique_id == SNAPSHOT_NODE and status == "success":
            summary.snapshot_applied = True
        summary.results.append(
            {
                "unique_id": unique_id,
                "resource_type": unique_id.split(".", 1)[0],
                "status": status,
                "failures": result.get("failures"),
                "execution_seconds": float(result.get("execution_time") or 0.0),
                "message": (result.get("message") or "")[:1000] or None,
            }
        )
    return summary


def _dbt_executable() -> list[str]:
    script = Path(sys.executable).with_name("dbt")
    return [str(script)] if script.exists() else [sys.executable, "-m", "dbt.cli.main"]


def run_dbt(settings: Settings, args: list[str], variables: dict[str, Any] | None = None) -> DbtResult:
    """Invoke dbt in its own process, so it never shares a DuckDB handle with this one."""
    settings.export_env()
    with tempfile.TemporaryDirectory(prefix="canary-dbt-") as target_path:
        command = [
            *_dbt_executable(),
            *args,
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
            "--target",
            settings.resolved_dbt_target,
            "--target-path",
            target_path,
        ]
        if variables is not None:
            command += ["--vars", json.dumps(variables)]
        completed = subprocess.run(command, capture_output=True, text=True, env=os.environ.copy(), check=False)
        artifacts = [Path(target_path) / name for name in ("run_results.json", "sources.json")]
        results_file = next((path for path in artifacts if path.exists()), None)
        run_results = json.loads(results_file.read_text()) if results_file else None
    return DbtResult(
        success=completed.returncode == 0,
        output=completed.stdout + completed.stderr,
        summary=summarize(run_results),
    )
