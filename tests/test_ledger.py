from datetime import date

import pytest

from complaint_canary.ledger import APPLIED, REJECTED, VALIDATED, Ledger, OutOfOrderSnapshot
from complaint_canary.warehouse import get_warehouse

D1, D2, D3 = date(2026, 9, 30), date(2026, 10, 1), date(2026, 10, 2)


@pytest.fixture
def ledger(make_settings):
    ledger = Ledger(get_warehouse(make_settings()))
    ledger.ensure()
    return ledger


def land(ledger, snapshot_date, etag):
    ledger.record_landed(snapshot_date=snapshot_date, etag=etag, row_count=100, landing_uri="x", run_id="r")


def test_a_first_snapshot_is_ingested(ledger):
    assert ledger.plan(D1, "etag-1") == "ingest"


def test_an_unchanged_file_is_reused(ledger):
    land(ledger, D1, "etag-1")
    ledger.mark(D1, VALIDATED)
    assert ledger.plan(D1, "etag-1") == "reuse"


def test_an_older_snapshot_is_refused_once_a_newer_one_is_applied(ledger):
    land(ledger, D2, "etag-2")
    ledger.mark(D2, APPLIED)
    with pytest.raises(OutOfOrderSnapshot):
        ledger.plan(D1, "etag-1")


def test_a_different_file_for_an_applied_date_is_refused(ledger):
    land(ledger, D2, "etag-2")
    ledger.mark(D2, APPLIED)
    with pytest.raises(OutOfOrderSnapshot):
        ledger.plan(D2, "etag-2-rebuilt")


def test_a_rejected_snapshot_can_be_replaced(ledger):
    land(ledger, D1, "etag-1")
    ledger.mark(D1, APPLIED)
    land(ledger, D2, "bad")
    ledger.mark(D2, REJECTED, reject_reason="truncated")

    assert ledger.current().snapshot_date == D1
    assert ledger.pending() is None
    assert ledger.plan(D2, "good") == "ingest"

    land(ledger, D2, "good")
    assert ledger.get(D2).state == "landed"
    assert ledger.pending().snapshot_date == D2


def test_the_newest_validated_snapshot_is_current(ledger):
    for day, etag in ((D1, "a"), (D2, "b"), (D3, "c")):
        land(ledger, day, etag)
    ledger.mark(D1, APPLIED)
    ledger.mark(D2, VALIDATED)

    assert ledger.current().snapshot_date == D2
    assert ledger.latest_applied().snapshot_date == D1
    assert ledger.pending().snapshot_date == D3
