"""Warehouse interface. DuckDB serves local runs and CI; BigQuery serves the cloud."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any

from complaint_canary.config import Settings

# Logical column types: string, int, float, bool, date, timestamp.
Columns = list[tuple[str, str]]
Params = dict[str, Any] | None


class Warehouse(ABC):
    """SQL passed to ``execute`` and ``fetch`` uses ``@name`` placeholders."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @abstractmethod
    def qualified(self, layer: str, name: str) -> str:
        """Fully qualified, quoted table name for a logical layer."""

    @abstractmethod
    def execute(self, sql: str, params: Params = None) -> None: ...

    @abstractmethod
    def fetch(self, sql: str, params: Params = None) -> list[dict[str, Any]]: ...

    @abstractmethod
    def table_exists(self, layer: str, name: str) -> bool: ...

    @abstractmethod
    def ensure_table(
        self,
        layer: str,
        name: str,
        columns: Columns,
        partition_field: str | None = None,
        partition_expiration_days: int | None = None,
    ) -> None: ...

    @abstractmethod
    def load_parquet(self, layer: str, name: str, uri: str, columns: Columns, replace: bool) -> None:
        """Load a Parquet file; ``replace`` empties the table first, otherwise rows are appended."""

    @abstractmethod
    def insert_rows(self, layer: str, name: str, columns: Columns, rows: list[dict[str, Any]]) -> None: ...

    @abstractmethod
    def drop_table(self, layer: str, name: str) -> None: ...

    def expire_partitions(self, layer: str, name: str, field: str, cutoff: date) -> None:  # noqa: B027
        """Remove partitions older than ``cutoff``. BigQuery does this natively."""
