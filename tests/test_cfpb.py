import csv
import io
import zipfile
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from complaint_canary import fixture
from complaint_canary.ingest import cfpb

SNAPSHOT = date(2026, 9, 30)


def test_converts_every_row_and_renames_columns(fixture_files, tmp_path):
    out = tmp_path / "complaints.parquet"
    stats = cfpb.convert_zip_to_parquet(fixture_files["day1"], out, SNAPSHOT, "run-1")
    table = pq.read_table(out)

    assert stats.rows_read == stats.rows_written == table.num_rows == len(fixture.build_day1())
    assert table.schema.names == [*cfpb.SOURCE_COLUMNS, "snapshot_date", "run_id"]
    assert table["snapshot_date"].unique().to_pylist() == [SNAPSHOT]
    assert table["run_id"].unique().to_pylist() == ["run-1"]
    # Empty CSV fields become nulls, not empty strings.
    assert table["sub_issue"].null_count > 0


def test_rejects_a_file_whose_header_breaks_the_contract(tmp_path):
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([*cfpb.EXPECTED_HEADER, "Consumer complaint narrative"])
    writer.writerow(["x"] * 16)
    path = tmp_path / "extra-column.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("complaints.csv", buffer.getvalue())

    with pytest.raises(cfpb.SchemaContractError):
        cfpb.convert_zip_to_parquet(path, tmp_path / "out.parquet", SNAPSHOT, "run-1")


def test_rejects_a_truncated_download(fixture_files, tmp_path):
    data = fixture_files["day1"].read_bytes()
    path = tmp_path / "truncated.zip"
    path.write_bytes(data[: len(data) // 2])

    with pytest.raises(cfpb.FileIntegrityError):
        cfpb.convert_zip_to_parquet(path, tmp_path / "out.parquet", SNAPSHOT, "run-1")
    assert not (tmp_path / "out.parquet").exists()


def test_rejects_a_corrupt_archive_by_its_checksum(fixture_files, tmp_path):
    data = bytearray(fixture_files["day1"].read_bytes())
    middle = len(data) // 2
    data[middle : middle + 50] = b"\x00" * 50
    path = tmp_path / "corrupt.zip"
    path.write_bytes(bytes(data))

    with pytest.raises(cfpb.FileIntegrityError):
        cfpb.convert_zip_to_parquet(path, tmp_path / "out.parquet", SNAPSHOT, "run-1")


def test_download_checks_the_expected_size(fixture_files, tmp_path):
    with pytest.raises(cfpb.FileIntegrityError):
        cfpb.download(str(fixture_files["day1"]), tmp_path / "copy.zip", expected_size=1)


def test_sample_is_stable_between_snapshots(fixture_files, tmp_path):
    """A complaint sampled on day 1 is still sampled on day 2, so history stays comparable."""
    ids = {}
    for day in ("day1", "day2"):
        out = tmp_path / f"{day}.parquet"
        stats = cfpb.convert_zip_to_parquet(fixture_files[day], out, SNAPSHOT, "run-1", sample_percent=20)
        ids[day] = set(pq.read_table(out)["complaint_id"].to_pylist())
        assert 0.1 < stats.rows_written / stats.rows_read < 0.3

    full_day2 = {row["Complaint ID"] for row in fixture.build_day2(fixture.build_day1())}
    assert ids["day1"] & full_day2 <= ids["day2"]


def test_sample_keeps_rows_with_a_malformed_id():
    mask = cfpb.sample_mask(pa.array(["12345", "not-a-number", None]), sample_percent=1)
    assert mask.to_pylist()[1:] == [True, True]
