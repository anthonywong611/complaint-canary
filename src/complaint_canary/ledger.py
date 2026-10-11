"""Snapshot ledger: one row per snapshot date, tracking it from landing to publication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal

from complaint_canary.warehouse import Warehouse
from complaint_canary.warehouse.base import Columns

LANDED, VALIDATED, APPLIED, PUBLISHED, REJECTED = "landed", "validated", "applied", "published", "rejected"
USABLE = (VALIDATED, APPLIED, PUBLISHED)
APPLIED_STATES = (APPLIED, PUBLISHED)

LEDGER_TABLE = "snapshot_ledger"
LEDGER_COLUMNS: Columns = [
    ("snapshot_date", "date"),
    ("state", "string"),
    ("etag", "string"),
    ("sha256", "string"),
    ("source_bytes", "int"),
    ("source_last_modified", "timestamp"),
    ("rows_read", "int"),
    ("row_count", "int"),
    ("sample_percent", "int"),
    ("landing_uri", "string"),
    ("run_id", "string"),
    ("gate_override", "bool"),
    ("new_count", "int"),
    ("changed_count", "int"),
    ("deleted_count", "int"),
    ("landed_at", "timestamp"),
    ("validated_at", "timestamp"),
    ("applied_at", "timestamp"),
    ("published_at", "timestamp"),
    ("rejected_at", "timestamp"),
    ("reject_reason", "string"),
]
_TIMESTAMP_FOR_STATE = {
    VALIDATED: "validated_at",
    APPLIED: "applied_at",
    PUBLISHED: "published_at",
    REJECTED: "rejected_at",
}


class OutOfOrderSnapshot(Exception):
    """A snapshot at or before the last applied one was offered."""


@dataclass(frozen=True)
class LedgerEntry:
    snapshot_date: date
    state: str
    etag: str | None = None
    row_count: int | None = None
    landing_uri: str | None = None
    run_id: str | None = None


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _in(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


class Ledger:
    def __init__(self, warehouse: Warehouse) -> None:
        self.warehouse = warehouse
        self.table = warehouse.qualified("ops", LEDGER_TABLE)

    def ensure(self) -> None:
        self.warehouse.ensure_table("ops", LEDGER_TABLE, LEDGER_COLUMNS)

    def _one(self, where: str, params: dict[str, Any] | None = None) -> LedgerEntry | None:
        rows = self.warehouse.fetch(
            "select snapshot_date, state, etag, row_count, landing_uri, run_id"
            f" from {self.table} where {where} order by snapshot_date desc limit 1",
            params,
        )
        return LedgerEntry(**rows[0]) if rows else None

    def get(self, snapshot_date: date) -> LedgerEntry | None:
        return self._one("snapshot_date = @snapshot_date", {"snapshot_date": snapshot_date})

    def latest_applied(self) -> LedgerEntry | None:
        return self._one(f"state in ({_in(APPLIED_STATES)})")

    def current(self) -> LedgerEntry | None:
        """The newest snapshot that passed the integrity gate: the one dbt builds from."""
        return self._one(f"state in ({_in(USABLE)})")

    def pending(self) -> LedgerEntry | None:
        """A landed snapshot newer than anything validated, waiting for the gate."""
        entry = self._one(f"state = '{LANDED}'")
        current = self.current()
        if entry and current and entry.snapshot_date <= current.snapshot_date:
            return None
        return entry

    def plan(self, snapshot_date: date, etag: str) -> Literal["ingest", "reuse"]:
        """Decide what to do with the file on offer.

        Snapshots apply strictly in date order: one at or before the last applied
        snapshot is refused unless it is the same file, which is reused.
        """
        latest = self.latest_applied()
        if latest and snapshot_date < latest.snapshot_date:
            raise OutOfOrderSnapshot(
                f"snapshot {snapshot_date} is older than the last applied snapshot {latest.snapshot_date}"
            )
        existing = self.get(snapshot_date)
        if existing and existing.etag == etag and existing.state in USABLE:
            return "reuse"
        if latest and snapshot_date == latest.snapshot_date:
            raise OutOfOrderSnapshot(f"snapshot {snapshot_date} was already applied from a different file")
        return "ingest"

    def record_landed(self, **fields: Any) -> None:
        self.ensure()
        self.warehouse.execute(
            f"delete from {self.table} where snapshot_date = @snapshot_date",
            {"snapshot_date": fields["snapshot_date"]},
        )
        row = {"state": LANDED, "landed_at": utcnow(), **fields}
        self.warehouse.insert_rows("ops", LEDGER_TABLE, LEDGER_COLUMNS, [row])

    def mark(self, snapshot_date: date, state: str, **fields: Any) -> None:
        values = {"state": state, _TIMESTAMP_FOR_STATE[state]: utcnow(), **fields}
        assignments = ", ".join(f"{column} = @{column}" for column in values)
        self.warehouse.execute(
            f"update {self.table} set {assignments} where snapshot_date = @snapshot_date",
            {**values, "snapshot_date": snapshot_date},
        )

    def reset_to_landed(self, from_date: date) -> None:
        """Used by replay: forget that snapshots from ``from_date`` on were applied."""
        self.warehouse.execute(
            f"update {self.table} set state = '{LANDED}' where snapshot_date >= @from_date and state <> '{REJECTED}'",
            {"from_date": from_date},
        )

    def landed_dates(self, from_date: date) -> list[LedgerEntry]:
        rows = self.warehouse.fetch(
            "select snapshot_date, state, etag, row_count, landing_uri, run_id"
            f" from {self.table} where snapshot_date >= @from_date and state = '{LANDED}'"
            " order by snapshot_date",
            {"from_date": from_date},
        )
        return [LedgerEntry(**row) for row in rows]
