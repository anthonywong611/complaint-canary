"""Rebuild SCD2 history by re-applying landed snapshots in date order."""

from __future__ import annotations

import time
from datetime import date

from complaint_canary import pipeline
from complaint_canary.config import Settings
from complaint_canary.dbt_runner import dbt_vars, run_dbt
from complaint_canary.ledger import APPLIED, Ledger
from complaint_canary.warehouse import Warehouse


class ReplayError(Exception):
    pass


def replay(
    settings: Settings, warehouse: Warehouse, run_id: str, from_date: date, drop_history: bool = False
) -> list[date]:
    """Re-apply every landed snapshot from ``from_date`` on.

    Before calling this, history must be as it stood before ``from_date``: restored
    from a table snapshot, or absent. ``drop_history`` removes it, which is only right
    when the landing zone still holds every snapshot since the first.
    """
    ledger = Ledger(warehouse)
    if drop_history:
        warehouse.drop_table("snapshots", "snap_complaints")
    ledger.reset_to_landed(from_date)
    applied: list[date] = []
    for entry in ledger.landed_dates(from_date):
        started = time.monotonic()
        pipeline.load_raw_partition(warehouse, entry.snapshot_date, entry.landing_uri or "")
        pipeline.run_gate(settings, warehouse, run_id, entry.snapshot_date)
        result = run_dbt(settings, ["snapshot"], dbt_vars(settings, entry.snapshot_date))
        if not result.success:
            raise ReplayError(f"dbt snapshot failed for {entry.snapshot_date}:\n{result.output[-2000:]}")
        ledger.mark(entry.snapshot_date, APPLIED)
        pipeline.record_audit(
            settings,
            warehouse,
            run_id,
            "replay",
            entry.snapshot_date,
            started,
            row_count=entry.row_count,
            bytes_billed=result.summary.bytes_billed,
        )
        applied.append(entry.snapshot_date)
    return applied
