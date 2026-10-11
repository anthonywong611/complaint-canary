"""BigQuery warehouse for DEV, QA and PROD."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from complaint_canary.warehouse.base import Columns, Params, Warehouse

TYPES = {
    "string": "STRING",
    "int": "INT64",
    "float": "FLOAT64",
    "bool": "BOOL",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}
MAX_BYTES_BILLED = 50 * 1024**3


def parameter_type(value: Any) -> str:
    # bool before int and datetime before date: each is a subclass of the other.
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, datetime):
        return "TIMESTAMP"
    if isinstance(value, date):
        return "DATE"
    return "STRING"


def json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


class BigQueryWarehouse(Warehouse):
    dialect = "bigquery"

    def __init__(self, settings) -> None:
        super().__init__(settings)
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from google.cloud import bigquery

            self._client = bigquery.Client(
                project=self.settings.gcp_project or None, location=self.settings.bq_location
            )
        return self._client

    def _table_id(self, layer: str, name: str) -> str:
        return f"{self.client.project}.{self.settings.schema(layer)}.{name}"

    def qualified(self, layer: str, name: str) -> str:
        return f"`{self._table_id(layer, name)}`"

    def _schema(self, columns: Columns):
        from google.cloud import bigquery

        return [bigquery.SchemaField(column, TYPES[kind]) for column, kind in columns]

    def _query(self, sql: str, params: Params):
        from google.cloud import bigquery

        config = bigquery.QueryJobConfig(
            maximum_bytes_billed=MAX_BYTES_BILLED,
            query_parameters=[
                bigquery.ScalarQueryParameter(name, parameter_type(value), value)
                for name, value in (params or {}).items()
            ],
        )
        return self.client.query(sql, job_config=config).result()

    def execute(self, sql: str, params: Params = None) -> None:
        self._query(sql, params)

    def fetch(self, sql: str, params: Params = None) -> list[dict[str, Any]]:
        return [dict(row.items()) for row in self._query(sql, params)]

    def table_exists(self, layer: str, name: str) -> bool:
        from google.api_core.exceptions import NotFound

        try:
            self.client.get_table(self._table_id(layer, name))
        except NotFound:
            return False
        return True

    def ensure_table(
        self,
        layer: str,
        name: str,
        columns: Columns,
        partition_field: str | None = None,
        partition_expiration_days: int | None = None,
    ) -> None:
        from google.cloud import bigquery

        table = bigquery.Table(self._table_id(layer, name), schema=self._schema(columns))
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
                expiration_ms=(partition_expiration_days * 86_400_000 if partition_expiration_days else None),
            )
        self.client.create_table(table, exists_ok=True)

    def load_parquet(self, layer: str, name: str, uri: str, columns: Columns, replace: bool) -> None:
        from google.cloud import bigquery

        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=self._schema(columns),
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE if replace else bigquery.WriteDisposition.WRITE_APPEND
            ),
        )
        self.client.load_table_from_uri(uri, self._table_id(layer, name), job_config=config).result()

    def insert_rows(self, layer: str, name: str, columns: Columns, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        from google.cloud import bigquery

        # A load job, not a streaming insert, so the rows can be updated straight away.
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=self._schema(columns),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        names = [column for column, _ in columns]
        payload = [{column: json_value(row.get(column)) for column in names} for row in rows]
        self.client.load_table_from_json(payload, self._table_id(layer, name), job_config=config).result()

    def drop_table(self, layer: str, name: str) -> None:
        self.client.delete_table(self._table_id(layer, name), not_found_ok=True)
