"""FDIC BankFind: active institutions, with holding company and asset size."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from complaint_canary.ingest.http import is_remote, retry_transient

FIELDS = "CERT,NAME,NAMEHCR,CITY,STALP,ASSET,DEP,BKCLASS,ACTIVE,REPDTE"
PAGE_SIZE = 10_000

FDIC_SCHEMA = pa.schema(
    [
        pa.field("cert", pa.int64()),
        pa.field("institution_name", pa.string()),
        pa.field("holding_company_name", pa.string()),
        pa.field("city", pa.string()),
        pa.field("state_code", pa.string()),
        pa.field("total_assets_thousands", pa.int64()),
        pa.field("total_deposits_thousands", pa.int64()),
        pa.field("charter_class", pa.string()),
        pa.field("report_date", pa.string()),
        pa.field("as_of_date", pa.date32()),
    ]
)
FDIC_COLUMNS = [
    ("cert", "int"),
    ("institution_name", "string"),
    ("holding_company_name", "string"),
    ("city", "string"),
    ("state_code", "string"),
    ("total_assets_thousands", "int"),
    ("total_deposits_thousands", "int"),
    ("charter_class", "string"),
    ("report_date", "string"),
    ("as_of_date", "date"),
]


@retry_transient
def _fetch_page(client: httpx.Client, url: str, offset: int, page_size: int) -> dict[str, Any]:
    response = client.get(
        url,
        params={
            "filters": "ACTIVE:1",
            "fields": FIELDS,
            "limit": page_size,
            "offset": offset,
            "sort_by": "CERT",
            "sort_order": "ASC",
            "format": "json",
        },
    )
    response.raise_for_status()
    return response.json()


def fetch_institutions(
    url: str, client: httpx.Client | None = None, page_size: int = PAGE_SIZE
) -> list[dict[str, Any]]:
    """Return every active institution, following offset pagination to the reported total."""
    if not is_remote(url):
        return [item["data"] for item in json.loads(Path(url).read_text())["data"]]
    assert client is not None
    records: list[dict[str, Any]] = []
    while True:
        page = _fetch_page(client, url, len(records), page_size)
        items = [item["data"] for item in page.get("data", [])]
        records.extend(items)
        if not items or len(records) >= int(page["meta"]["total"]):
            return records


def to_table(records: list[dict[str, Any]], as_of_date: date) -> pa.Table:
    def text(value: Any) -> str | None:
        value = (value or "").strip() if isinstance(value, str) else value
        return value or None

    rows = [
        {
            "cert": int(r["CERT"]),
            "institution_name": text(r.get("NAME")),
            "holding_company_name": text(r.get("NAMEHCR")),
            "city": text(r.get("CITY")),
            "state_code": text(r.get("STALP")),
            "total_assets_thousands": int(r["ASSET"]) if r.get("ASSET") is not None else None,
            "total_deposits_thousands": int(r["DEP"]) if r.get("DEP") is not None else None,
            "charter_class": text(r.get("BKCLASS")),
            "report_date": text(r.get("REPDTE")),
            "as_of_date": as_of_date,
        }
        for r in records
    ]
    return pa.Table.from_pylist(rows, schema=FDIC_SCHEMA)


def write_parquet(records: list[dict[str, Any]], path: Path, as_of_date: date) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = to_table(records, as_of_date)
    pq.write_table(table, path, compression="zstd")
    return table.num_rows
