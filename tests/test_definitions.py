from dagster import AssetKey


def test_the_code_location_loads_with_its_job_schedule_and_assets(make_settings):
    from complaint_canary.definitions import defs

    job = defs.resolve_job_def("complaint_canary_daily")
    keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}

    assert job.name == "complaint_canary_daily"
    assert defs.resolve_job_def("replay_scd2")
    assert {sensor.name for sensor in defs.sensors} == {"emit_run_failure_event", "freshness_watchdog"}
    assert defs.resolve_schedule_def("daily_after_cfpb_rebuild").cron_schedule == "0 11 * * *"
    assert {
        "landing/cfpb_complaints",
        "raw/complaints_snapshot",
        "raw/fdic_institutions",
        "raw/census_state_population",
        "ops/published_snapshot",
    } <= keys
    assert any(key.endswith("fct_scam_alerts") for key in keys)
    # dbt sources resolve to the Python assets that load them.
    graph = defs.resolve_asset_graph()
    snapshot_key = next(key for key in graph.get_all_asset_keys() if key.path[-1] == "snap_complaints")
    assert AssetKey(["raw", "complaints_snapshot"]) in graph.get(snapshot_key).parent_keys
