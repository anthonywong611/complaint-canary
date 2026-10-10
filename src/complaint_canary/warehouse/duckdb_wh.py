"""DuckDB warehouse for local runs and CI."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from typing import Any

import duckdb

from complaint_canary.warehouse.base import Columns, Params, Warehouse

TYPES = {
    "string": "VARCHAR",
    "int": "BIGINT",
    "float": "DOUBLE",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}
_PLACEHOLDER = re.compile(r"@(\w+)")


class DuckDBWarehouse(Warehouse):
    dialect = "duckdb"

    @contextmanager
    def _connect(self) -> Iterator[duckdb.DuckDBPyConnection]:
        # One short-lived connection per call: dbt runs as a separate process and
        # DuckDB allows a single writer process at a time.
        path = self.settings.duckdb_path
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(path))
        try:
            yield connection
        finally:
            connection.close()

    def qualified(self, layer: str, name: str) -> str:
        return f'"{self.settings.schema(layer)}"."{name}"'

    def execute(self, sql: str, params: Params = None) -> None:
        with self._connect() as connection:
            connection.execute(_PLACEHOLDER.sub(r"$\1", sql), params or {})

    def fetch(self, sql: str, params: Params = None) -> list[dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.execute(_PLACEHOLDER.sub(r"$\1", sql), params or {})
            names = [column[0] for column in cursor.description]
            return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    def table_exists(self, layer: str, name: str) -> bool:
        rows = self.fetch(
            "select 1 as found from information_schema.tables"
            " where table_schema = @table_schema and table_name = @table_name",
            {"table_schema": self.settings.schema(layer), "table_name": name},
        )
        return bool(rows)

    def ensure_table(
        self,
        layer: str,
        name: str,
        columns: Columns,
        partition_field: str | None = None,
        partition_expiration_days: int | None = None,
    ) -> None:
        definition = ", ".join(f'"{column}" {TYPES[kind]}' for column, kind in columns)
        with self._connect() as connection:
            connection.execute(f'create schema if not exists "{self.settings.schema(layer)}"')
            connection.execute(f"create table if not exists {self.qualified(layer, name)} ({definition})")

    def load_parquet(self, layer: str, name: str, uri: str, columns: Columns, replace: bool) -> None:
        if uri.startswith("gs://"):
            raise ValueError("the DuckDB warehouse loads local files only")
        self.ensure_table(layer, name, columns)
        names = ", ".join(f'"{column}"' for column, _ in columns)
        table = self.qualified(layer, name)
        with self._connect() as connection:
            if replace:
                connection.execute(f"delete from {table}")
            connection.execute(f"insert into {table} ({names}) select {names} from read_parquet(?)", [uri])

    def insert_rows(self, layer: str, name: str, columns: Columns, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self.ensure_table(layer, name, columns)
        names = [column for column, _ in columns]
        sql = f"insert into {self.qualified(layer, name)} ({', '.join(names)}) values ({', '.join('?' for _ in names)})"
        with self._connect() as connection:
            connection.executemany(sql, [[row.get(column) for column in names] for row in rows])

    def drop_table(self, layer: str, name: str) -> None:
        with self._connect() as connection:
            connection.execute(f"drop table if exists {self.qualified(layer, name)}")

    def expire_partitions(self, layer: str, name: str, field: str, cutoff: date) -> None:
        self.execute(f'delete from {self.qualified(layer, name)} where "{field}" < @cutoff', {"cutoff": cutoff})
