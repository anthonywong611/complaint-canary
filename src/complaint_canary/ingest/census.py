"""US Census Bureau state population estimates, unpivoted to one row per state and year."""

from __future__ import annotations

import csv
import io
import re
from datetime import date
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from complaint_canary.ingest.http import is_remote, retry_transient

STATE_SUMMARY_LEVEL = "040"
_YEAR_COLUMN = re.compile(r"^POPESTIMATE(\d{4})$")

CENSUS_SCHEMA = pa.schema(
    [
        pa.field("state_fips", pa.string()),
        pa.field("state_name", pa.string()),
        pa.field("population_year", pa.int64()),
        pa.field("population", pa.int64()),
        pa.field("as_of_date", pa.date32()),
    ]
)
CENSUS_COLUMNS = [
    ("state_fips", "string"),
    ("state_name", "string"),
    ("population_year", "int"),
    ("population", "int"),
    ("as_of_date", "date"),
]


@retry_transient
def fetch_csv(url: str, client: httpx.Client | None = None) -> str:
    if not is_remote(url):
        return Path(url).read_text(encoding="utf-8")
    assert client is not None
    response = client.get(url)
    response.raise_for_status()
    return response.content.decode("latin-1")


def parse(text: str, as_of_date: date) -> pa.Table:
    reader = csv.DictReader(io.StringIO(text))
    year_columns = {name: int(m.group(1)) for name in reader.fieldnames or [] if (m := _YEAR_COLUMN.match(name))}
    if not year_columns:
        raise ValueError("no POPESTIMATE<year> columns in the Census file")
    rows = [
        {
            "state_fips": record["STATE"].zfill(2),
            "state_name": record["NAME"],
            "population_year": year,
            "population": int(record[column]),
            "as_of_date": as_of_date,
        }
        for record in reader
        if record["SUMLEV"].zfill(3) == STATE_SUMMARY_LEVEL
        for column, year in year_columns.items()
    ]
    return pa.Table.from_pylist(rows, schema=CENSUS_SCHEMA)


def write_parquet(text: str, path: Path, as_of_date: date) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = parse(text, as_of_date)
    pq.write_table(table, path, compression="zstd")
    return table.num_rows
