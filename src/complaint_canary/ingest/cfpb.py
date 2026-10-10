"""CFPB Consumer Complaint Database: download the bulk zip and convert it to Parquet."""

from __future__ import annotations

import hashlib
import shutil
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from complaint_canary.ingest.http import is_remote, retry_transient

# The schema contract: the bulk file must have exactly these columns, in this order.
COLUMN_MAP = {
    "Date received": "date_received",
    "Product": "product",
    "Sub-product": "sub_product",
    "Issue": "issue",
    "Sub-issue": "sub_issue",
    "Company public response": "company_public_response",
    "Company": "company",
    "State": "state",
    "ZIP code": "zip_code",
    "Tags": "tags",
    "Submitted via": "submitted_via",
    "Date sent to company": "date_sent_to_company",
    "Company response to consumer": "company_response",
    "Timely response?": "timely_response",
    "Complaint ID": "complaint_id",
}
EXPECTED_HEADER = list(COLUMN_MAP)
SOURCE_COLUMNS = list(COLUMN_MAP.values())
RAW_SCHEMA = pa.schema(
    [pa.field(name, pa.string()) for name in SOURCE_COLUMNS]
    + [pa.field("snapshot_date", pa.date32()), pa.field("run_id", pa.string())]
)
RAW_COLUMNS = [(name, "string") for name in SOURCE_COLUMNS] + [
    ("snapshot_date", "date"),
    ("run_id", "string"),
]

_KNUTH = 2654435761
_CHUNK = 1024 * 1024


class SchemaContractError(Exception):
    """The source file's columns differ from the contract."""


class FileIntegrityError(Exception):
    """The source file is truncated or corrupt."""


@dataclass(frozen=True)
class RemoteFile:
    url: str
    etag: str
    last_modified: datetime
    size: int


@dataclass(frozen=True)
class ConversionStats:
    rows_read: int
    rows_written: int


@retry_transient
def stat_source(url: str, client: httpx.Client | None = None) -> RemoteFile:
    """Describe the source file without downloading it."""
    if not is_remote(url):
        path = Path(url)
        info = path.stat()
        return RemoteFile(
            url=url,
            etag=sha256_file(path),
            last_modified=datetime.fromtimestamp(info.st_mtime, tz=UTC),
            size=info.st_size,
        )
    assert client is not None
    response = client.head(url)
    response.raise_for_status()
    return RemoteFile(
        url=url,
        etag=response.headers.get("etag", "").strip('"'),
        last_modified=parsedate_to_datetime(response.headers["last-modified"]),
        size=int(response.headers.get("content-length", 0)),
    )


@retry_transient
def download(url: str, dest: Path, client: httpx.Client | None = None, expected_size: int = 0) -> str:
    """Fetch the source file to ``dest`` and return its SHA-256."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not is_remote(url):
        shutil.copyfile(url, dest)
    else:
        assert client is not None
        with client.stream("GET", url) as response:
            response.raise_for_status()
            with dest.open("wb") as handle:
                for chunk in response.iter_bytes(_CHUNK):
                    handle.write(chunk)
    size = dest.stat().st_size
    if expected_size and size != expected_size:
        raise FileIntegrityError(f"downloaded {size} bytes, expected {expected_size}")
    return sha256_file(dest)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def sample_mask(complaint_ids: pa.Array, sample_percent: int) -> pa.Array:
    """Stable sample: the same complaint is always in or always out.

    Rows whose ID is not numeric are kept so the integrity gate can see them.
    """
    numeric = pc.fill_null(pc.utf8_is_digit(complaint_ids), False)
    ids = pc.cast(pc.if_else(numeric, complaint_ids, "0"), pa.int64())
    hashed = pc.bit_wise_and(pc.multiply(ids, _KNUTH), 0xFFFFFFFF)
    bucket = pc.subtract(hashed, pc.multiply(pc.divide(hashed, 100), 100))
    return pc.or_(pc.less(bucket, sample_percent), pc.invert(numeric))


def convert_zip_to_parquet(
    zip_path: Path,
    parquet_path: Path,
    snapshot_date: date,
    run_id: str,
    sample_percent: int = 100,
) -> ConversionStats:
    """Stream the zipped CSV into one Parquet file with constant memory.

    Raises ``FileIntegrityError`` for a truncated or corrupt archive (the zip CRC is
    verified as the member is read to its end) and ``SchemaContractError`` when the
    header is not exactly the expected 15 columns.
    """
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    rows_read = rows_written = 0
    try:
        with zipfile.ZipFile(zip_path) as archive:
            members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if len(members) != 1:
                raise FileIntegrityError(f"expected one CSV in the archive, found {members}")
            with archive.open(members[0]) as stream:
                reader = pacsv.open_csv(
                    stream,
                    read_options=pacsv.ReadOptions(block_size=16 * _CHUNK),
                    parse_options=pacsv.ParseOptions(newlines_in_values=True),
                    convert_options=pacsv.ConvertOptions(
                        column_types=dict.fromkeys(EXPECTED_HEADER, pa.string()),
                        strings_can_be_null=True,
                        null_values=[""],
                    ),
                )
                if reader.schema.names != EXPECTED_HEADER:
                    raise SchemaContractError(
                        f"header {reader.schema.names} does not match the contract {EXPECTED_HEADER}"
                    )
                with pq.ParquetWriter(parquet_path, RAW_SCHEMA, compression="zstd") as writer:
                    for batch in reader:
                        rows_read += batch.num_rows
                        table = pa.Table.from_batches([batch]).rename_columns(SOURCE_COLUMNS)
                        if sample_percent < 100:
                            table = table.filter(sample_mask(table["complaint_id"], sample_percent))
                        size = table.num_rows
                        table = table.append_column(
                            "snapshot_date", pa.array([snapshot_date] * size, pa.date32())
                        ).append_column("run_id", pa.array([run_id] * size, pa.string()))
                        writer.write_table(table.cast(RAW_SCHEMA))
                        rows_written += size
    except (zipfile.BadZipFile, EOFError, pa.ArrowInvalid) as exc:
        parquet_path.unlink(missing_ok=True)
        raise FileIntegrityError(f"{zip_path.name} is truncated or corrupt: {exc}") from exc
    return ConversionStats(rows_read=rows_read, rows_written=rows_written)
