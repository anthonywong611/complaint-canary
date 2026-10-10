"""End-to-end runs of the Dagster job on the fixture, against DuckDB."""

from datetime import date

import pytest

from complaint_canary import fixture, pipeline
from complaint_canary.dbt_runner import dbt_vars, run_dbt
from complaint_canary.ingest import cfpb
from complaint_canary.ledger import Ledger, OutOfOrderSnapshot
from complaint_canary.replay import replay
from complaint_canary.storage import get_landing
from complaint_canary.warehouse import get_warehouse
from tests.conftest import DAY3, use_environment

DAY1_ROWS = len(fixture.build_day1())
SCD2 = "select complaint_id, dbt_valid_from, dbt_valid_to, status_hash, attribute_hash from {} order by 1, 2"


def run_job():
    from complaint_canary.definitions import defs

    return defs.resolve_job_def("complaint_canary_daily").execute_in_process(raise_on_error=False)


@pytest.fixture(scope="module")
def two_days(tmp_path_factory, fixture_files):
    """The daily job run for two consecutive snapshots."""
    from complaint_canary.config import Settings

    root = tmp_path_factory.mktemp("pipeline")

    def settings_for(cfpb_file, **overrides):
        return Settings(
            _env_file=None,
            env="local",
            duckdb_path=root / "warehouse.duckdb",
            landing_uri=str(root / "landing"),
            cfpb_url=str(cfpb_file),
            fdic_url=str(fixture_files["fdic"]),
            census_url=str(fixture_files["census"]),
            **overrides,
        )

    results = []
    for day in ("day1", "day2"):
        use_environment(settings_for(fixture_files[day]))
        results.append(run_job())
    settings = settings_for(fixture_files["day2"])
    return {
        "results": results,
        "settings": settings,
        "settings_for": settings_for,
        "root": root,
        "warehouse": get_warehouse(settings),
    }


def scalar(warehouse, sql, **params):
    return next(iter(warehouse.fetch(sql, params or None)[0].values()))


def test_both_runs_succeed_and_publish(two_days):
    warehouse = two_days["warehouse"]
    assert [result.success for result in two_days["results"]] == [True, True]
    rows = warehouse.fetch(
        "select snapshot_date, state, new_count, changed_count, deleted_count"
        f" from {warehouse.qualified('ops', 'snapshot_ledger')} order by snapshot_date"
    )
    assert rows == [
        {
            "snapshot_date": fixture.DAY1,
            "state": "published",
            "new_count": DAY1_ROWS,
            "changed_count": 0,
            "deleted_count": 0,
        },
        {"snapshot_date": fixture.DAY2, "state": "published", "new_count": 30, "changed_count": 15, "deleted_count": 2},
    ]


def test_history_uses_snapshot_dates_not_the_clock(two_days):
    warehouse = two_days["warehouse"]
    history = warehouse.qualified("marts", "fct_complaint_status_history")
    rows = warehouse.fetch(
        "select change_type, is_deleted, count(*) as versions,"
        " min(valid_from) as valid_from, max(valid_to) as valid_to"
        f" from {history} group by 1, 2 order by 1, 2"
    )
    assert rows == [
        {"change_type": "attribute", "is_deleted": False, "versions": 3, "valid_from": fixture.DAY2, "valid_to": None},
        {
            "change_type": "backfill",
            "is_deleted": False,
            "versions": DAY1_ROWS - 2,
            "valid_from": fixture.DAY1,
            "valid_to": fixture.DAY2,
        },
        {
            "change_type": "backfill",
            "is_deleted": True,
            "versions": 2,
            "valid_from": fixture.DAY1,
            "valid_to": fixture.DAY2,
        },
        {"change_type": "new", "is_deleted": False, "versions": 30, "valid_from": fixture.DAY2, "valid_to": None},
        {"change_type": "status", "is_deleted": False, "versions": 12, "valid_from": fixture.DAY2, "valid_to": None},
    ]
    assert scalar(warehouse, f"select count(*) from {history} where previous_snapshot_date = @d", d=fixture.DAY1) == 45


def test_backfilled_complaints_have_no_first_observed_date(two_days):
    warehouse = two_days["warehouse"]
    complaints = warehouse.qualified("marts", "fct_complaints")
    row = warehouse.fetch(
        f"select count(*) as total, count(first_observed_date) as observed,"
        f" sum(case when is_backfill then 1 else 0 end) as backfilled from {complaints}"
    )[0]
    assert row == {"total": DAY1_ROWS + 28, "observed": 30, "backfilled": DAY1_ROWS - 2}


def test_the_planted_spike_is_alerted_and_the_flood_is_not(two_days):
    warehouse = two_days["warehouse"]
    alerts = warehouse.fetch(
        "select companies.company_name, alerts.fraud_category, alerts.week_start, alerts.severity,"
        " alerts.alert_status, alerts.first_raised_date, alerts.last_evaluated_date"
        f" from {warehouse.qualified('marts', 'fct_scam_alerts')} as alerts"
        f" inner join {warehouse.qualified('marts', 'dim_company')} as companies"
        " on alerts.company_key = companies.company_key"
    )
    assert alerts == [
        {
            "company_name": fixture.SPIKE_COMPANY,
            "fraud_category": "scam",
            "week_start": fixture.SPIKE_WEEK,
            "severity": "critical",
            "alert_status": "active",
            # Raised on day 1, re-evaluated and kept on day 2.
            "first_raised_date": fixture.DAY1,
            "last_evaluated_date": fixture.DAY2,
        }
    ]
    flood = warehouse.fetch(
        "select evaluations.robust_z_score, evaluations.fraud_share_lift, evaluations.is_alert"
        f" from {warehouse.qualified('intermediate', 'int_alert_evaluations')} as evaluations"
        f" inner join {warehouse.qualified('marts', 'dim_company')} as companies"
        " on evaluations.entity_key = companies.company_key"
        " where companies.company_name = @company and evaluations.fraud_category = 'scam'"
        " and evaluations.week_start = @week",
        {"company": fixture.FLOOD_COMPANY, "week": fixture.FLOOD_WEEK},
    )[0]
    assert flood["robust_z_score"] > 3.5 and flood["fraud_share_lift"] < 1.2 and not flood["is_alert"]


def test_the_digest_is_written_to_the_landing_zone(two_days):
    digest_file = two_days["root"] / "landing" / "digests" / f"week={fixture.SPIKE_WEEK}" / "scam-signal-digest.md"
    assert fixture.SPIKE_COMPANY in digest_file.read_text()


def test_companies_resolve_by_override_name_match_or_not_at_all(two_days):
    warehouse = two_days["warehouse"]
    rows = warehouse.fetch(
        f"select company_name, match_method, entity_type, fdic_cert from {warehouse.qualified('marts', 'dim_company')}"
    )
    resolved = {row["company_name"]: (row["match_method"], row["entity_type"], row["fdic_cert"]) for row in rows}
    assert resolved[fixture.SPIKE_COMPANY] == ("override", "bank", 628)
    assert resolved["Sutton Bank"] == ("fdic_institution", "bank", 5962)
    assert resolved["Coinbase, Inc."] == ("override", "non_bank", None)
    assert resolved["Example Unreviewed Lender LLC"] == ("unmatched", "unreviewed", None)
    # The rename of three complaints did not split the company in two.
    assert sum(1 for name in resolved if name.lower().startswith("paypal")) == 1


def test_quality_results_and_audit_rows_are_recorded(two_days):
    warehouse = two_days["warehouse"]
    statuses = warehouse.fetch(
        f"select invocation, status, count(*) as tests from {warehouse.qualified('ops', 'dq_results')}"
        " group by 1, 2 order by 1, 2"
    )
    assert {(row["invocation"], row["status"]) for row in statuses} == {("build", "pass"), ("gate", "pass")}
    steps = warehouse.fetch(
        f"select step, count(*) as runs from {warehouse.qualified('ops', 'load_audit')} group by 1 order by 1"
    )
    assert {row["step"]: row["runs"] for row in steps} == {
        "census": 1,
        "dbt_build": 2,
        "fdic": 1,
        "ingest": 2,
        "integrity_gate": 2,
        "load_raw": 2,
        "publish": 2,
    }


def test_rerunning_the_same_snapshot_changes_nothing(two_days):
    warehouse = two_days["warehouse"]
    snapshot_table = warehouse.qualified("snapshots", "snap_complaints")
    before = warehouse.fetch(SCD2.format(snapshot_table))

    use_environment(two_days["settings"])
    assert run_job().success

    assert warehouse.fetch(SCD2.format(snapshot_table)) == before
    assert scalar(warehouse, f"select count(*) from {warehouse.qualified('ops', 'snapshot_ledger')}") == 2


def test_an_older_snapshot_is_refused(two_days, fixture_files):
    settings = two_days["settings_for"](fixture_files["day1"])
    warehouse = get_warehouse(settings)
    with pytest.raises(OutOfOrderSnapshot):
        pipeline.ingest_cfpb(settings, warehouse, get_landing(settings.landing_uri), "out-of-order")


def test_a_truncated_snapshot_is_rejected_and_leaves_everything_unchanged(two_days, tmp_path):
    warehouse = two_days["warehouse"]
    snapshot_table = warehouse.qualified("snapshots", "snap_complaints")
    complaints = warehouse.qualified("marts", "fct_complaints")
    history_before = warehouse.fetch(SCD2.format(snapshot_table))
    complaints_before = scalar(warehouse, f"select count(*) from {complaints}")

    # A well-formed file holding only the first 60% of the rows.
    rows = fixture.build_day2(fixture.build_day1())
    short_file = fixture.write_zip(rows[: int(len(rows) * 0.6)], tmp_path / "short.zip", DAY3)
    use_environment(two_days["settings_for"](short_file))
    result = run_job()

    assert not result.success
    entry = Ledger(warehouse).get(DAY3)
    assert entry.state == "rejected"
    reason = scalar(
        warehouse,
        f"select reject_reason from {warehouse.qualified('ops', 'snapshot_ledger')} where snapshot_date = @d",
        d=DAY3,
    )
    assert "gate_row_count_within_bounds" in reason and "gate_delete_guard" in reason
    assert warehouse.fetch(SCD2.format(snapshot_table)) == history_before
    assert scalar(warehouse, f"select count(*) from {complaints}") == complaints_before
    assert Ledger(warehouse).current().snapshot_date == fixture.DAY2


def test_a_corrupt_download_never_reaches_the_landing_zone(two_days, fixture_files, tmp_path):
    data = fixture_files["day2"].read_bytes()
    broken = tmp_path / "broken.zip"
    broken.write_bytes(data[: len(data) // 2])
    settings = two_days["settings_for"](broken, snapshot_date=date(2026, 10, 5))
    warehouse = get_warehouse(settings)

    with pytest.raises(cfpb.FileIntegrityError):
        pipeline.ingest_cfpb(settings, warehouse, get_landing(settings.landing_uri), "corrupt")
    assert Ledger(warehouse).get(date(2026, 10, 5)) is None
    assert not (two_days["root"] / "landing" / "cfpb" / "dt=2026-10-05").exists()


def test_the_watchdog_reports_stale_publication(two_days, caplog):
    import logging
    from datetime import timedelta

    from complaint_canary.ledger import utcnow

    warehouse, settings = two_days["warehouse"], two_days["settings"]
    with caplog.at_level(logging.INFO, logger="complaint-canary"):
        fresh = pipeline.watchdog(settings, warehouse, utcnow())
        stale = pipeline.watchdog(settings, warehouse, utcnow() + timedelta(hours=27))

    assert not fresh["stale"] and stale["stale"]
    events = [record.getMessage() for record in caplog.records]
    assert sum('"event": "heartbeat"' in event for event in events) == 2
    assert sum('"event": "snapshot_stale"' in event for event in events) == 1


def test_replay_rebuilds_identical_history(two_days):
    """Runs last: it drops the SCD2 table and re-applies both landed snapshots."""
    warehouse, settings = two_days["warehouse"], two_days["settings"]
    snapshot_table = warehouse.qualified("snapshots", "snap_complaints")
    before = warehouse.fetch(SCD2.format(snapshot_table))

    applied = replay(settings, warehouse, "replay-test", fixture.DAY1, drop_history=True)

    assert applied == [fixture.DAY1, fixture.DAY2]
    assert warehouse.fetch(SCD2.format(snapshot_table)) == before
    result = run_dbt(settings, ["build", "--exclude", "tag:integrity_gate"], dbt_vars(settings, fixture.DAY2))
    assert result.success, result.output[-2000:]
