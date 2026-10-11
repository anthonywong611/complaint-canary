"""Dagster code location."""

from datetime import date

from dagster import (
    AssetSelection,
    Config,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    OpExecutionContext,
    RunFailureSensorContext,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    define_asset_job,
    in_process_executor,
    job,
    op,
    run_failure_sensor,
    sensor,
)
from dagster_dbt import DbtCliResource

from complaint_canary import assets
from complaint_canary.config import DBT_PROJECT_DIR, get_settings
from complaint_canary.events import emit_event
from complaint_canary.ledger import utcnow
from complaint_canary.pipeline import watchdog
from complaint_canary.replay import replay

settings = get_settings()
settings.export_env()

# One step at a time: DuckDB allows a single writer, and the steps are sequential anyway.
daily_job = define_asset_job(
    name="complaint_canary_daily",
    selection=AssetSelection.all(),
    executor_def=in_process_executor,
    tags={"dagster/max_runtime": 7200},
    description="Ingest today's snapshot, gate it, rebuild the warehouse and publish.",
)

daily_schedule = ScheduleDefinition(
    name="daily_after_cfpb_rebuild",
    job=daily_job,
    cron_schedule="0 11 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING if settings.enable_schedules else DefaultScheduleStatus.STOPPED,
)


class ReplayConfig(Config):
    from_date: str
    drop_history: bool = False


@op
def replay_history(context: OpExecutionContext, config: ReplayConfig) -> None:
    runtime_settings, warehouse, _ = assets.runtime()
    applied = replay(
        runtime_settings, warehouse, context.run_id, date.fromisoformat(config.from_date), config.drop_history
    )
    context.log.info("re-applied snapshots: %s", ", ".join(map(str, applied)) or "none")


@job(executor_def=in_process_executor, description="Rebuild SCD2 history from landed snapshots. See docs/runbook.md.")
def replay_scd2() -> None:
    replay_history()


@run_failure_sensor(
    name="emit_run_failure_event",
    default_status=DefaultSensorStatus.RUNNING if settings.enable_schedules else DefaultSensorStatus.STOPPED,
)
def emit_run_failure_event(context: RunFailureSensorContext) -> None:
    emit_event(
        get_settings(),
        "run_failure",
        run_id=context.dagster_run.run_id,
        job=context.dagster_run.job_name,
        error=context.failure_event.message,
    )


@sensor(
    name="freshness_watchdog",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.RUNNING if settings.enable_schedules else DefaultSensorStatus.STOPPED,
    description="Hourly heartbeat, plus an event when no snapshot has been published for 26 hours.",
)
def freshness_watchdog(_context: SensorEvaluationContext) -> SkipReason:
    runtime_settings, warehouse, _ = assets.runtime()
    status = watchdog(runtime_settings, warehouse, utcnow())
    return SkipReason(f"heartbeat sent; last published {status['last_published']}; stale={status['stale']}")


defs = Definitions(
    assets=[
        assets.cfpb_landing,
        assets.raw_complaints_snapshot,
        assets.raw_fdic_institutions,
        assets.raw_census_state_population,
        assets.canary_dbt_assets,
        assets.published_snapshot,
    ],
    jobs=[daily_job, replay_scd2],
    schedules=[daily_schedule],
    sensors=[emit_run_failure_event, freshness_watchdog],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            target=settings.resolved_dbt_target,
        )
    },
)
