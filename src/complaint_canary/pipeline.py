"""Pipeline steps. The Dagster assets and the CLI are thin wrappers around these."""

from __future__ import annotations

import json
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from complaint_canary import digest
from complaint_canary.config import Settings
from complaint_canary.dbt_runner import GATE_SELECTOR, DbtSummary, dbt_vars, run_dbt
from complaint_canary.events import emit_event
from complaint_canary.ingest import census, cfpb, fdic
from complaint_canary.ingest.http import make_client
from complaint_canary.ledger import APPLIED, PUBLISHED, REJECTED, VALIDATED, Ledger, OutOfOrderSnapshot, utcnow
from complaint_canary.storage import Landing
from complaint_canary.warehouse import Warehouse
from complaint_canary.warehouse.base import Columns

RAW_PARTITION_DAYS = 7
STALE_AFTER_HOURS = 26
BACKUP_INTERVAL_DAYS = 7
BACKUP_RETENTION_DAYS = 28

AUDIT_TABLE = "load_audit"
AUDIT_COLUMNS: Columns = [
    ("run_id", "string"),
    ("snapshot_date", "date"),
    ("env", "string"),
    ("step", "string"),
    ("finished_at", "timestamp"),
    ("duration_seconds", "float"),
    ("row_count", "int"),
    ("bytes_billed", "int"),
    ("git_sha", "string"),
    ("details", "string"),
]
DQ_TABLE = "dq_results"
DQ_COLUMNS: Columns = [
    ("run_id", "string"),
    ("snapshot_date", "date"),
    ("env", "string"),
    ("invocation", "string"),
    ("unique_id", "string"),
    ("resource_type", "string"),
    ("status", "string"),
    ("failures", "int"),
    ("execution_seconds", "float"),
    ("message", "string"),
    ("recorded_at", "timestamp"),
]


class SnapshotRejected(Exception):
    """The integrity gate failed; the snapshot will not change history."""


def ensure_ops_tables(warehouse: Warehouse) -> None:
    Ledger(warehouse).ensure()
    warehouse.ensure_table("ops", AUDIT_TABLE, AUDIT_COLUMNS)
    warehouse.ensure_table("ops", DQ_TABLE, DQ_COLUMNS)


def record_audit(
    settings: Settings,
    warehouse: Warehouse,
    run_id: str,
    step: str,
    snapshot_date: date | None,
    started: float,
    row_count: int | None = None,
    bytes_billed: int | None = None,
    **details: Any,
) -> None:
    warehouse.insert_rows(
        "ops",
        AUDIT_TABLE,
        AUDIT_COLUMNS,
        [
            {
                "run_id": run_id,
                "snapshot_date": snapshot_date,
                "env": settings.env,
                "step": step,
                "finished_at": utcnow(),
                "duration_seconds": round(time.monotonic() - started, 3),
                "row_count": row_count,
                "bytes_billed": bytes_billed,
                "git_sha": settings.git_sha,
                "details": json.dumps(details, default=str, sort_keys=True) if details else None,
            }
        ],
    )


def record_dq_results(
    settings: Settings,
    warehouse: Warehouse,
    run_id: str,
    snapshot_date: date | None,
    invocation: str,
    summary: DbtSummary,
) -> None:
    now = utcnow()
    rows = [
        {
            **result,
            "run_id": run_id,
            "snapshot_date": snapshot_date,
            "env": settings.env,
            "invocation": invocation,
            "recorded_at": now,
        }
        for result in summary.results
        if result["resource_type"] in ("test", "unit_test", "source")
    ]
    warehouse.insert_rows("ops", DQ_TABLE, DQ_COLUMNS, rows)
    for unique_id in summary.warned:
        emit_event(settings, "dq_warning", test=unique_id, snapshot_date=snapshot_date, run_id=run_id)


def ingest_cfpb(settings: Settings, warehouse: Warehouse, landing: Landing, run_id: str) -> dict[str, Any]:
    """Land today's CFPB file as Parquet, unless it is already in hand."""
    started = time.monotonic()
    ensure_ops_tables(warehouse)
    ledger = Ledger(warehouse)
    with make_client(settings.http_user_agent) as client:
        remote = cfpb.stat_source(settings.cfpb_url, client)
        snapshot_date = settings.snapshot_date or remote.last_modified.date()
        try:
            action = ledger.plan(snapshot_date, remote.etag)
        except OutOfOrderSnapshot as exc:
            emit_event(settings, "snapshot_refused", snapshot_date=snapshot_date, reason=str(exc), run_id=run_id)
            raise
        if action == "reuse":
            emit_event(settings, "snapshot_skipped", snapshot_date=snapshot_date, etag=remote.etag, run_id=run_id)
            return {"snapshot_date": snapshot_date, "skipped": True, "etag": remote.etag}

        with tempfile.TemporaryDirectory(prefix="canary-cfpb-") as workdir:
            zip_path = Path(workdir) / "complaints.csv.zip"
            parquet_path = Path(workdir) / "complaints.parquet"
            sha256 = cfpb.download(settings.cfpb_url, zip_path, client, expected_size=remote.size)
            stats = cfpb.convert_zip_to_parquet(zip_path, parquet_path, snapshot_date, run_id, settings.sample_percent)
            landing_uri = landing.put(parquet_path, f"cfpb/dt={snapshot_date.isoformat()}/complaints.parquet")

    ledger.record_landed(
        snapshot_date=snapshot_date,
        etag=remote.etag,
        sha256=sha256,
        source_bytes=remote.size,
        source_last_modified=remote.last_modified.replace(tzinfo=None),
        rows_read=stats.rows_read,
        row_count=stats.rows_written,
        sample_percent=settings.sample_percent,
        landing_uri=landing_uri,
        run_id=run_id,
        gate_override=settings.gate_override,
    )
    record_audit(
        settings,
        warehouse,
        run_id,
        "ingest",
        snapshot_date,
        started,
        row_count=stats.rows_written,
        source_bytes=remote.size,
        etag=remote.etag,
        landing_uri=landing_uri,
    )
    return {
        "snapshot_date": snapshot_date,
        "skipped": False,
        "etag": remote.etag,
        "rows_read": stats.rows_read,
        "rows_written": stats.rows_written,
        "landing_uri": landing_uri,
    }


def load_raw_partition(warehouse: Warehouse, snapshot_date: date, landing_uri: str) -> None:
    """Replace one snapshot's partition in the raw table."""
    warehouse.ensure_table(
        "raw",
        "complaints_snapshot",
        cfpb.RAW_COLUMNS,
        partition_field="snapshot_date",
        partition_expiration_days=RAW_PARTITION_DAYS,
    )
    table = warehouse.qualified("raw", "complaints_snapshot")
    warehouse.execute(f"delete from {table} where snapshot_date = @snapshot_date", {"snapshot_date": snapshot_date})
    warehouse.load_parquet("raw", "complaints_snapshot", landing_uri, cfpb.RAW_COLUMNS, replace=False)
    warehouse.expire_partitions(
        "raw", "complaints_snapshot", "snapshot_date", snapshot_date - timedelta(days=RAW_PARTITION_DAYS)
    )


def run_gate(settings: Settings, warehouse: Warehouse, run_id: str, snapshot_date: date) -> None:
    """Run the integrity gate on a loaded partition and record the verdict in the ledger."""
    started = time.monotonic()
    ledger = Ledger(warehouse)
    result = run_dbt(settings, ["test", "--select", GATE_SELECTOR], dbt_vars(settings, snapshot_date))
    record_dq_results(settings, warehouse, run_id, snapshot_date, "gate", result.summary)
    record_audit(
        settings,
        warehouse,
        run_id,
        "integrity_gate",
        snapshot_date,
        started,
        bytes_billed=result.summary.bytes_billed,
        failed=result.summary.failed,
    )
    if not result.success:
        reason = ", ".join(result.summary.failed) or result.output[-500:]
        ledger.mark(snapshot_date, REJECTED, reject_reason=reason[:1000])
        emit_event(settings, "snapshot_rejected", snapshot_date=snapshot_date, reason=reason, run_id=run_id)
        raise SnapshotRejected(f"snapshot {snapshot_date} failed the integrity gate: {reason}")
    ledger.mark(snapshot_date, VALIDATED)
    check_source_freshness(settings, warehouse, run_id, snapshot_date)


def check_source_freshness(settings: Settings, warehouse: Warehouse, run_id: str, snapshot_date: date) -> None:
    """dbt source freshness: warns when the newest raw partition is stale. Never blocks."""
    if settings.env == "local":
        return  # the fixture's dates are fixed, so it would always look stale
    result = run_dbt(settings, ["source", "freshness", "--select", "source:raw.complaints_snapshot"])
    record_dq_results(settings, warehouse, run_id, snapshot_date, "freshness", result.summary)


def load_and_gate(settings: Settings, warehouse: Warehouse, run_id: str) -> dict[str, Any]:
    """Load the newest landed snapshot into raw and put it through the integrity gate."""
    ensure_ops_tables(warehouse)
    entry = Ledger(warehouse).pending()
    if entry is None:
        return {"skipped": True}
    started = time.monotonic()
    load_raw_partition(warehouse, entry.snapshot_date, entry.landing_uri or "")
    record_audit(settings, warehouse, run_id, "load_raw", entry.snapshot_date, started, row_count=entry.row_count)
    run_gate(settings, warehouse, run_id, entry.snapshot_date)
    return {"skipped": False, "snapshot_date": entry.snapshot_date, "row_count": entry.row_count}


def _last_loaded(warehouse: Warehouse, table: str) -> date | None:
    if not warehouse.table_exists("raw", table):
        return None
    rows = warehouse.fetch(f"select max(as_of_date) as as_of_date from {warehouse.qualified('raw', table)}")
    return rows[0]["as_of_date"] if rows else None


def _is_fresh(warehouse: Warehouse, table: str, max_age_days: int, today: date) -> bool:
    loaded = _last_loaded(warehouse, table)
    return loaded is not None and (today - loaded).days < max_age_days


def refresh_fdic(
    settings: Settings, warehouse: Warehouse, landing: Landing, run_id: str, today: date, force: bool = False
) -> dict[str, Any]:
    if not force and _is_fresh(warehouse, "fdic_institutions", settings.fdic_max_age_days, today):
        return {"skipped": True}
    started = time.monotonic()
    with make_client(settings.http_user_agent) as client:
        records = fdic.fetch_institutions(settings.fdic_url, client)
    with tempfile.TemporaryDirectory(prefix="canary-fdic-") as workdir:
        path = Path(workdir) / "institutions.parquet"
        rows = fdic.write_parquet(records, path, today)
        uri = landing.put(path, f"fdic/dt={today.isoformat()}/institutions.parquet")
    warehouse.ensure_table("raw", "fdic_institutions", fdic.FDIC_COLUMNS)
    warehouse.load_parquet("raw", "fdic_institutions", uri, fdic.FDIC_COLUMNS, replace=True)
    record_audit(settings, warehouse, run_id, "fdic", None, started, row_count=rows, landing_uri=uri)
    return {"skipped": False, "rows": rows, "landing_uri": uri}


def refresh_census(
    settings: Settings, warehouse: Warehouse, landing: Landing, run_id: str, today: date, force: bool = False
) -> dict[str, Any]:
    if not force and _is_fresh(warehouse, "census_state_population", settings.census_max_age_days, today):
        return {"skipped": True}
    started = time.monotonic()
    with make_client(settings.http_user_agent) as client:
        text = census.fetch_csv(settings.census_url, client)
    with tempfile.TemporaryDirectory(prefix="canary-census-") as workdir:
        path = Path(workdir) / "state_population.parquet"
        rows = census.write_parquet(text, path, today)
        uri = landing.put(path, f"census/dt={today.isoformat()}/state_population.parquet")
    warehouse.ensure_table("raw", "census_state_population", census.CENSUS_COLUMNS)
    warehouse.load_parquet("raw", "census_state_population", uri, census.CENSUS_COLUMNS, replace=True)
    record_audit(settings, warehouse, run_id, "census", None, started, row_count=rows, landing_uri=uri)
    return {"skipped": False, "rows": rows, "landing_uri": uri}


def current_snapshot_date(warehouse: Warehouse) -> date:
    entry = Ledger(warehouse).current()
    if entry is None:
        raise RuntimeError("no snapshot has passed the integrity gate yet")
    return entry.snapshot_date


def after_build(
    settings: Settings,
    warehouse: Warehouse,
    run_id: str,
    snapshot_date: date,
    summary: DbtSummary,
    started: float,
) -> None:
    """Record what a dbt build did, whether or not it succeeded."""
    ledger = Ledger(warehouse)
    entry = ledger.get(snapshot_date)
    if summary.snapshot_applied and entry and entry.state == VALIDATED:
        ledger.mark(snapshot_date, APPLIED)
    record_dq_results(settings, warehouse, run_id, snapshot_date, "build", summary)
    record_audit(
        settings,
        warehouse,
        run_id,
        "dbt_build",
        snapshot_date,
        started,
        bytes_billed=summary.bytes_billed,
        nodes=len(summary.results),
        failed=summary.failed,
    )


def backup_history(settings: Settings, warehouse: Warehouse, snapshot_date: date, run_id: str) -> str | None:
    """Weekly BigQuery table snapshot of the SCD2 history, kept four weeks."""
    if not settings.scd2_backup_enabled or settings.warehouse != "bigquery":
        return None
    try:
        dataset = settings.schema("snapshots")
        recent = warehouse.fetch(
            f"select count(*) as recent from `{dataset}`.INFORMATION_SCHEMA.TABLES"
            " where table_name like 'snap_complaints_backup_%'"
            " and creation_time > timestamp_sub(current_timestamp(), interval @days day)",
            {"days": BACKUP_INTERVAL_DAYS},
        )
        if recent and recent[0]["recent"]:
            return None
        name = f"snap_complaints_backup_{snapshot_date:%Y%m%d}"
        warehouse.execute(
            f"create snapshot table if not exists {warehouse.qualified('snapshots', name)}"
            f" clone {warehouse.qualified('snapshots', 'snap_complaints')}"
            " options (expiration_timestamp ="
            f" timestamp_add(current_timestamp(), interval {BACKUP_RETENTION_DAYS} day))"
        )
        return name
    except Exception as exc:  # a failed backup must not block publication
        emit_event(settings, "backup_failed", snapshot_date=snapshot_date, reason=str(exc), run_id=run_id)
        return None


def publish(settings: Settings, warehouse: Warehouse, landing: Landing, run_id: str) -> dict[str, Any]:
    """Mark the snapshot published, write the digest and announce it."""
    started = time.monotonic()
    ledger = Ledger(warehouse)
    snapshot_date = current_snapshot_date(warehouse)
    entry = ledger.get(snapshot_date)
    if entry is None or entry.state not in (APPLIED, PUBLISHED):
        raise RuntimeError(f"snapshot {snapshot_date} is {entry.state if entry else 'missing'}, not applied")

    changes = warehouse.fetch(
        "select new_count, changed_count, deleted_count"
        f" from {warehouse.qualified('ops', 'snapshot_changes')} where snapshot_date = @snapshot_date",
        {"snapshot_date": snapshot_date},
    )
    counts = changes[0] if changes else {"new_count": 0, "changed_count": 0, "deleted_count": 0}
    ledger.mark(snapshot_date, PUBLISHED, **counts)

    alerts = warehouse.fetch(
        f"select * from {warehouse.qualified('marts', 'alert_digest')} order by robust_z_score desc"
    )
    digest_uri = None
    if alerts:
        week = alerts[0]["week_start"]
        digest_uri = landing.put_text(
            digest.render(alerts, snapshot_date), f"digests/week={week}/scam-signal-digest.md"
        )
    backup = backup_history(settings, warehouse, snapshot_date, run_id)
    record_audit(
        settings,
        warehouse,
        run_id,
        "publish",
        snapshot_date,
        started,
        row_count=entry.row_count,
        active_alerts=len(alerts),
        digest_uri=digest_uri,
        backup=backup,
        **counts,
    )
    emit_event(
        settings,
        "snapshot_published",
        snapshot_date=snapshot_date,
        run_id=run_id,
        active_alerts=len(alerts),
        **counts,
    )
    return {"snapshot_date": snapshot_date, "active_alerts": len(alerts), "digest_uri": digest_uri, **counts}


def watchdog(settings: Settings, warehouse: Warehouse, now: datetime) -> dict[str, Any]:
    """Hourly health check: emit a heartbeat, and a stale event when publication has stopped."""
    emit_event(settings, "heartbeat")
    last_published = None
    if warehouse.table_exists("ops", "snapshot_ledger"):
        rows = warehouse.fetch(
            f"select max(published_at) as published_at from {warehouse.qualified('ops', 'snapshot_ledger')}"
        )
        last_published = rows[0]["published_at"] if rows else None
    if last_published is None:
        return {"stale": False, "last_published": None}
    last_published = last_published.replace(tzinfo=None)
    stale = now - last_published > timedelta(hours=STALE_AFTER_HOURS)
    if stale:
        emit_event(settings, "snapshot_stale", last_published=last_published, threshold_hours=STALE_AFTER_HOURS)
    return {"stale": stale, "last_published": last_published}
