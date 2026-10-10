"""Command line: ``canary fixture | run | replay | status``."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

from complaint_canary.config import REPO_ROOT, get_settings


def _fixture(args: argparse.Namespace) -> int:
    from complaint_canary.fixture import write_fixture

    for name, path in write_fixture(Path(args.out)).items():
        print(f"{name}: {path}")
    return 0


def _run(args: argparse.Namespace) -> int:
    from dagster import DagsterInstance

    from complaint_canary.definitions import defs

    instance = DagsterInstance.get() if os.environ.get("DAGSTER_HOME") else DagsterInstance.ephemeral()
    result = defs.resolve_job_def(args.job).execute_in_process(instance=instance, raise_on_error=False)
    print(f"run {result.run_id}: {'succeeded' if result.success else 'FAILED'}")
    return 0 if result.success else 1


def _replay(args: argparse.Namespace) -> int:
    from complaint_canary.assets import runtime
    from complaint_canary.replay import replay

    settings, warehouse, _ = runtime()
    applied = replay(settings, warehouse, "manual-replay", date.fromisoformat(args.from_date), args.drop_history)
    print("re-applied:", ", ".join(map(str, applied)) or "none")
    return 0


def _status(_: argparse.Namespace) -> int:
    from complaint_canary.warehouse import get_warehouse

    warehouse = get_warehouse(get_settings())
    if not warehouse.table_exists("ops", "snapshot_ledger"):
        print("no snapshots yet")
        return 0
    rows = warehouse.fetch(
        "select snapshot_date, state, row_count, new_count, changed_count, deleted_count"
        f" from {warehouse.qualified('ops', 'snapshot_ledger')} order by snapshot_date desc limit 14"
    )
    for row in rows:
        print("  ".join(f"{key}={value}" for key, value in row.items()))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="canary", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    fixture = commands.add_parser("fixture", help="write the synthetic fixture files")
    fixture.add_argument("--out", default=str(REPO_ROOT / "data" / "fixture"))
    fixture.set_defaults(handler=_fixture)

    run = commands.add_parser("run", help="execute a Dagster job in this process")
    run.add_argument("--job", default="complaint_canary_daily")
    run.set_defaults(handler=_run)

    replay = commands.add_parser("replay", help="re-apply landed snapshots to rebuild SCD2 history")
    replay.add_argument("--from", dest="from_date", required=True, metavar="YYYY-MM-DD")
    replay.add_argument("--drop-history", action="store_true")
    replay.set_defaults(handler=_replay)

    status = commands.add_parser("status", help="show the latest snapshot ledger rows")
    status.set_defaults(handler=_status)

    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
