# Runbook

Commands assume the cloud. `ENV` is `dev`, `qa` or `prod`; datasets are `canary_<env>_<layer>`.

Open a shell on the VM:

```bash
gcloud compute ssh canary-prod --tunnel-through-iap
```

On the VM, `sudo /opt/canary/compose.sh` is `docker compose` with the right environment:

```bash
sudo /opt/canary/compose.sh ps
sudo /opt/canary/compose.sh logs --tail 100 daemon
sudo /opt/canary/compose.sh run --rm pipeline canary status
sudo /opt/canary/compose.sh run --rm pipeline canary run
```

Dagster UI: `make ui-tunnel ENV=prod`, then http://localhost:3000.

## Where to look first

| Question | Query |
|---|---|
| What happened to each snapshot? | `select * from canary_prod_ops.snapshot_ledger order by snapshot_date desc` |
| Which tests failed or warned? | `select * from canary_prod_ops.dq_results where status not in ('pass') order by recorded_at desc` |
| Which rows failed a test? | The table named after the test in `canary_prod_dq_failures` |
| How long did each step take, and what did it scan? | `select * from canary_prod_ops.load_audit order by finished_at desc` |
| What events were emitted? | Logs Explorer: `logName:"complaint-canary"` |

## Alert: run failed or snapshot rejected

Read the event's `event` field.

### `snapshot_rejected`

The integrity gate failed. History and marts are unchanged; the ledger row is `rejected` with the failing tests in `reject_reason`.

1. Compare row counts: `select snapshot_date, state, rows_read, row_count, reject_reason from canary_prod_ops.snapshot_ledger order by 1 desc limit 5`.
2. If the file was truncated or malformed, do nothing. The next scheduled run takes the next file.
3. If CFPB really did remove or add that many complaints (check their release notes), let it through once:
   ```bash
   sudo /opt/canary/compose.sh run --rm -e CANARY_GATE_OVERRIDE=true pipeline canary run
   ```
   The override skips the row-count and delete-guard checks only, and is recorded in `snapshot_ledger.gate_override`.

### `snapshot_refused`

A snapshot dated at or before the last applied one was offered. This happens if CFPB rebuilds its file twice in one day. No action: the next day's file applies normally.

### `run_failure`

1. Open the run in the Dagster UI and read the failed step.
2. Download or schema failure (`FileIntegrityError`, `SchemaContractError`): see "Source changed" below.
3. A dbt test failed after the snapshot was applied: marts may be partly rebuilt and nothing was published. Fix the model or the data rule, deploy, and re-run. Re-running the same snapshot is safe.

## Alert: no snapshot published in 26 hours

The orchestrator is alive but nothing was published.

1. `canary status`: is the latest snapshot `rejected`, `validated` or `applied`?
2. `rejected`: see above. `validated` or `applied`: the dbt build failed; see `run_failure`.
3. No new ledger row at all: CFPB has not rebuilt its file. Check `curl -sI https://files.consumerfinance.gov/ccdb/complaints.csv.zip | grep -i last-modified`.

## Alert: orchestrator heartbeat missing

The VM or its containers are down.

1. `gcloud compute instances describe canary-prod --format='value(status)'`. If stopped: `gcloud compute instances start canary-prod`.
2. On the VM: `sudo /opt/canary/compose.sh ps`. Restart with `sudo /opt/canary/compose.sh up -d`.
3. If the VM is beyond repair, rebuild it (below).

## VM rebuild

All pipeline state is in BigQuery and GCS. The VM holds only Dagster's run history, on its own disk.

```bash
terraform -chdir=terraform apply -replace=google_compute_instance.canary \
  -var-file=envs/prod.tfvars -var-file=envs/prod.local.tfvars
```

Then redeploy the current release: run the `main` workflow by hand with the source hash from the last successful run's summary. Target: 15 minutes.

To restore Dagster's run history, create a disk from a `canary-prod-data` snapshot and attach it in place of the data disk. Losing it does not affect the data.

## Roll back a release

Run the `main` workflow by hand (Actions → main → Run workflow) with the `source_hash` of an earlier release, shown in that run's summary. Only images that passed QA can be deployed. There is no way to skip QA.

## Replay history from the landing zone

Use when the SCD2 table `snapshots.snap_complaints` is damaged. Landed files are kept 90 days in PROD.

History must first be put back to how it stood before the first date to replay.

**Damage is recent (within 7 days):** restore by time travel, then replay from the next day.

```sql
create or replace table canary_prod_snapshots.snap_complaints as
select * from canary_prod_snapshots.snap_complaints
for system_time as of timestamp '2026-10-08 10:00:00+00';
```

**Older:** restore the weekly table snapshot, then replay from the day after it.

```sql
create or replace table canary_prod_snapshots.snap_complaints
clone canary_prod_snapshots.snap_complaints_backup_20261004;
```

Then:

```bash
sudo /opt/canary/compose.sh run --rm pipeline canary replay --from 2026-10-05
sudo /opt/canary/compose.sh run --rm pipeline canary run
```

`--drop-history` rebuilds from nothing. Use it only if the landing zone still holds every snapshot since the first one; otherwise older history is lost.

## Warning: fraud labels are not reviewed

CFPB added an issue or sub-issue that reads like fraud. The test `assert_fraud_labels_are_reviewed` lists them in `dq_failures`. Add each to `dbt/seeds/fraud_taxonomy.csv` with a category, or `not_fraud` and a note. More than 10 unreviewed labels fails the build.

## Warning: unreviewed companies

Among the 200 highest-volume companies of the last 90 days, more than 5% of volume belongs to companies nobody has classified. List them:

```sql
select company_name from canary_prod_marts.dim_company where entity_type = 'unreviewed';
```

Add the large ones to `dbt/seeds/company_overrides.csv` as `bank` (with the FDIC certificate), `credit_union` or `non_bank`.

## Warning: publication lag

More than 5% of newly observed complaints were over 15 days old: CFPB is publishing late. Recent weeks are less complete than the alert rule assumes. Treat the latest alerts as provisional; they are re-evaluated for 8 weeks. If it persists, raise `maturity_days`.

## Warning: scale

The monthly BigQuery scan is nearing the 1 TiB free tier, or the row count is nearing 150 million. Check `load_audit.bytes_billed` by step. The first lever is partitioning the snapshot comparison by received year.

## Source changed

- `SchemaContractError`: CFPB changed the file's columns. Update `COLUMN_MAP` in `src/complaint_canary/ingest/cfpb.py`, the raw source and `snap_complaints`, then decide how the change affects history.
- HTTP 403 from CFPB: its CDN rejects some User-Agent strings. Leave `CANARY_HTTP_USER_AGENT` unset to use the client default, which works.
- Census URL returns 404: a new vintage was published. Set `CANARY_CENSUS_URL` to the new file.

## Stop the spend

```bash
gcloud compute instances stop canary-prod --zone us-central1-a
```

The VM is most of the bill. Storage continues at about a dollar a month. To remove everything, see the README.
