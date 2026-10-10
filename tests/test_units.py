import json
import logging
from datetime import date, datetime
from types import SimpleNamespace

from complaint_canary import digest
from complaint_canary.dbt_runner import SNAPSHOT_NODE, summarize
from complaint_canary.events import emit_event
from complaint_canary.storage import LocalLanding, get_landing
from complaint_canary.warehouse.bigquery_wh import BigQueryWarehouse, json_value, parameter_type


def test_local_landing_lists_snapshot_dates(tmp_path):
    landing = get_landing(str(tmp_path))
    assert isinstance(landing, LocalLanding)
    source = tmp_path / "file.parquet"
    source.write_text("x")
    for day in ("2026-10-01", "2026-09-30"):
        landing.put(source, f"cfpb/dt={day}/complaints.parquet")
    landing.put_text("# digest", "digests/week=2026-09-07/scam-signal-digest.md")

    assert landing.list_dates("cfpb") == [date(2026, 9, 30), date(2026, 10, 1)]
    assert landing.list_dates("digests") == []


def test_events_are_structured(make_settings, caplog):
    with caplog.at_level(logging.INFO, logger="complaint-canary"):
        payload = emit_event(make_settings(), "snapshot_rejected", snapshot_date=date(2026, 10, 2), reason="short")

    assert payload == {"event": "snapshot_rejected", "env": "local", "snapshot_date": "2026-10-02", "reason": "short"}
    record = caplog.records[0]
    assert record.levelname == "ERROR"
    assert json.loads(record.getMessage().removeprefix("CANARY_EVENT "))["event"] == "snapshot_rejected"


def test_bigquery_parameter_types_respect_subclassing():
    assert parameter_type(True) == "BOOL"
    assert parameter_type(7) == "INT64"
    assert parameter_type(datetime(2026, 10, 1, 9)) == "TIMESTAMP"
    assert parameter_type(date(2026, 10, 1)) == "DATE"
    assert parameter_type("x") == "STRING"
    assert json_value(date(2026, 10, 1)) == "2026-10-01"


def test_bigquery_names_use_the_dataset_prefix(make_settings):
    warehouse = BigQueryWarehouse(make_settings(warehouse="bigquery", dataset_prefix="canary_dev_"))
    warehouse._client = SimpleNamespace(project="my-project")

    assert warehouse.qualified("ops", "snapshot_ledger") == "`my-project.canary_dev_ops.snapshot_ledger`"


def test_dbt_results_are_summarised():
    summary = summarize(
        {
            "results": [
                {
                    "unique_id": SNAPSHOT_NODE,
                    "status": "success",
                    "execution_time": 1.5,
                    "adapter_response": {"bytes_billed": 1000},
                },
                {"unique_id": "test.complaint_canary.a", "status": "fail", "failures": 3, "message": "Got 3 results"},
                {"unique_id": "test.complaint_canary.b", "status": "warn", "failures": 1},
                {
                    "unique_id": "model.complaint_canary.c",
                    "status": "success",
                    "adapter_response": {"bytes_billed": 500},
                },
            ]
        }
    )

    assert summary.snapshot_applied
    assert summary.bytes_billed == 1500
    assert summary.failed == ["test.complaint_canary.a"]
    assert summary.warned == ["test.complaint_canary.b"]
    assert [r["resource_type"] for r in summary.results] == ["snapshot", "test", "test", "model"]
    assert not summarize(None).results


def test_digest_lists_every_signal_with_the_caveat():
    alerts = [
        {
            "week_start": date(2026, 9, 7),
            "severity": "critical",
            "scope": "company",
            "entity_name": "A Bank",
            "fraud_category": "scam",
            "complaint_count": 45,
            "baseline_median": 9.0,
            "lift": 5.0,
            "robust_z_score": 12.0,
        },
    ]
    text = digest.render(alerts, date(2026, 9, 30))

    assert "week of 2026-09-07" in text
    assert "| critical | company | A Bank | scam | 45 | 9.0 | 5.0x | 12.0 |" in text
    assert "not confirmed fraud" in text
