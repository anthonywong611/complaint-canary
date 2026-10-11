# Complaint Canary

Fraud and scam early-warning signals from public consumer complaints.

Banks and regulators usually notice a new scam, or a company whose customers are being hit, weeks late. The US Consumer Financial Protection Bureau (CFPB) publishes every complaint it receives, 18.3 million so far and about 27,000 more each day, but only as one large file with no change history.

Complaint Canary ingests that file daily, tracks how each complaint changes, classifies the fraud-related ones, and flags companies and states whose fraud complaints rise sharply against their own baseline. It serves fraud-strategy analysts, compliance teams and regulators.

Complaints are self-reported and lag the events they describe. The alerts are triage signals for an analyst, not detected fraud.

# Architecture

![Architecture](diagrams/architecture.png)

Source: [diagrams/architecture.drawio](diagrams/architecture.drawio) (open in [draw.io](https://app.diagrams.net)).

| Source Data | Description |
|---|---|
| CFPB (CC0) | The [Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/): every complaint the bureau has published since 2011, as one zipped CSV (353 MB, 18.3 million rows, 15 columns) rebuilt daily. Each row has the product, issue, company, state, ZIP code, dates and the company's response; there are no names or narratives. The file carries no change history, so the pipeline downloads it in full each day and works out what changed. It is the core of the project: every complaint, alert and history table derives from it. |
| FDIC | The [BankFind Suite API](https://api.fdic.gov/banks/docs/), `/institutions` endpoint: about 4,200 active FDIC-insured banks with their certificate number, holding company, charter class and total assets. A paginated JSON API with no key, refreshed weekly. It is used to tell banks from non-banks among the complaint companies and to size them, so alerts can be read per billion dollars of assets. |
| US Census Bureau (public domain) | [State population estimates](https://www.census.gov/programs-surveys/popest.html): one CSV with a yearly population figure per state, the District of Columbia and Puerto Rico, from 2020 on. Refreshed yearly. It is used to express state-level alerts per 100,000 residents, so large states do not dominate. |

| Component | Role |
|---|---|
| Compute Engine VM | Runs Dagster (schedule, sensors, UI) and the pipeline in Docker Compose. Disposable: all pipeline state is in Cloud Storage and BigQuery. |
| Cloud Storage | Landing zone. One immutable Parquet file per daily snapshot, kept 90 days in PROD so history can be replayed. |
| BigQuery | Warehouse, one dataset per layer: `raw`, `snapshots` (SCD2), `staging`, `intermediate`, `marts`, `ops`. dbt transforms in place (ELT). |
| Artifact Registry | One image per source version, with immutable tags. |
| Cloud Logging and Monitoring | The pipeline emits structured events; alert policies on them send email. |
| Identity-Aware Proxy | The only way into the VM. No public ports. |
| GitHub Actions | Builds, tests and deploys with short-lived tokens (Workload Identity Federation). |

# Pipeline Flow

1. **Ingest**: download the CFPB file, verify its size, checksum and 15-column header, and stream it to Parquet in the landing zone.
2. **Gate**: load the snapshot to `raw` and run five dbt tests on it (IDs, dates, row count against the last snapshot, share of complaints that would be deleted). A snapshot that fails is rejected and changes nothing.
3. **History**: a dbt snapshot records new, changed and removed complaints as SCD2 rows, stamped with the snapshot date so a re-run or replay gives the same result.
4. **Model**: dbt builds staging, intermediate and mart models, running each model's tests before anything downstream.
5. **Publish**: the ledger row is marked published, the digest is written, and an event is emitted for monitoring.

```mermaid
flowchart TD
  a[Check the CFPB file: ETag, size, last-modified] --> g{Compared with the last applied snapshot}
  g -- older, or same date but a different file --> q2[Refuse, emit snapshot_refused]
  g -- same file --> j
  g -- newer --> b[Download, verify size, zip CRC and the 15-column header]
  b -- corrupt or wrong columns --> q0[Fail before landing, emit run_failure]
  b --> c[Stream to Parquet with a fixed schema, DEV keeps a stable 5 percent sample]
  c --> d[Store in landing zone: cfpb/dt=YYYY-MM-DD]
  d --> e[Load the raw.complaints_snapshot partition]
  e --> f{Integrity gate: 5 dbt tests on the partition}
  f -- fail --> q[Ledger: rejected. History and marts unchanged, emit snapshot_rejected]
  f -- pass --> h[dbt snapshot: SCD2 by row hash, stamped with the snapshot date]
  h --> j[dbt build: staging, intermediate, marts, each followed by its tests]
  j -- a test fails --> q3[Nothing published, emit run_failure]
  j -- all pass --> l[Ledger: published. Write digest, emit snapshot_published]
```
Source: [diagrams/pipeline.mmd](diagrams/architecture.mmd)

The same code runs locally on DuckDB and in the cloud on BigQuery.

# Data Model

A star schema in the `marts` dataset. Keys only are shown here; every column is in [diagrams/data-model.mmd](diagrams/data-model.mmd) and in the dbt docs.

```mermaid
erDiagram
  fct_complaints }o--|| dim_date : received_date_key
  fct_complaints }o--|| dim_company : company_key
  fct_complaints }o--|| dim_product : product_key
  fct_complaints }o--|| dim_issue : issue_key
  fct_complaints }o--|| dim_geography : geography_key
  fct_complaint_status_history }o--|| fct_complaints : complaint_id
  fct_scam_alerts }o--|| dim_date : week_start_date_key
  fct_scam_alerts }o--o| dim_company : company_key
  fct_scam_alerts }o--o| dim_geography : geography_key
  fct_alert_backtest }o--|| dim_date : week_start_date_key
  alert_digest }o--|| fct_scam_alerts : latest_week_active_alerts

  fct_complaints {
    int complaint_id PK
    date received_date
    string fraud_category
    bool is_fraud_related
    date first_observed_date
  }
  fct_complaint_status_history {
    int complaint_id FK
    date valid_from
    date valid_to
    string change_type
    bool is_deleted
  }
  fct_scam_alerts {
    string alert_key PK
    date week_start
    string scope
    string fraud_category
    int complaint_count
    float robust_z_score
    string severity
    string alert_status
  }
  fct_alert_backtest {
    date week_start PK
    int series_evaluated
    int alerts_raised
  }
  alert_digest {
    date week_start
    string entity_name
    string severity
  }
  dim_company {
    string company_key PK
    string company_name
    string entity_type
    int fdic_cert
    string asset_size_band
  }
  dim_product {
    string product_key PK
    string product
    string sub_product
  }
  dim_issue {
    string issue_key PK
    string issue
    string sub_issue
    string fraud_category
  }
  dim_geography {
    string geography_key PK
    string state_name
    int population
  }
  dim_date {
    int date_key PK
    date date_day
    date week_start
  }
```

| Table | What it serves |
|---|---|
| `fct_scam_alerts` | The product: one row per weekly signal, for a fraud category and a company or state, with its z-score, lift and severity. An alert that stops holding is marked `withdrawn`, never deleted. |
| `fct_complaints` | One row per complaint currently in the CFPB database, with its fraud category. |
| `fct_complaint_status_history` | Every observed version of every complaint: how company responses change and which complaints CFPB removes. |
| `fct_alert_backtest` | The alert rule replayed over every week since 2018, to judge how noisy it is. |
| `alert_digest` | Active signals for the latest evaluated week; the content of the weekly digest. |
| `dim_company` | Complaint companies matched to FDIC institutions, with asset size for rates. |
| `dim_issue` | CFPB issue labels mapped to a stable fraud category across both of CFPB's taxonomy eras. |
| `ops.snapshot_ledger`, `ops.load_audit`, `ops.dq_results`, `ops.publication_lag` | Operations: what happened to each snapshot, step timings and bytes scanned, test results, CFPB's publishing delay. |

How an alert is decided, and why each threshold has its value: [docs/alert-rule.md](docs/alert-rule.md).

# Quick Start

Prerequisites: [uv](https://docs.astral.sh/uv/) 0.12 or later. Python 3.14 is installed by uv. No cloud account is needed.

```bash
make setup      # uv sync, and .env from .env.example
make fixture    # synthetic CFPB, FDIC and Census files in data/fixture
make run        # the daily job, on DuckDB
make status
```

`make status` prints the snapshot ledger:

```text
snapshot_date=2026-09-30  state=published  row_count=5119  new_count=5119  changed_count=0  deleted_count=0
```

The fixture plants one scam spike. The digest it produces is in `data/landing/digests/`.

# Usage

| Task | Command |
|---|---|
| Apply the next day's snapshot (30 new complaints, 15 changed, 2 removed) | `make day2` |
| Browse assets, lineage, checks and runs | `make ui`, then http://localhost:3000 |
| Same, in Docker with Postgres-backed run history | `make up` |
| Show the ledger | `uv run canary status` |
| Rebuild history from landed snapshots | `uv run canary replay --from 2026-09-30 --drop-history` |
| Generate dbt docs into `site/` | `make docs` |
| Run against the real sources | Remove the three `CANARY_*_URL` lines from `.env`, then `make run` (downloads 353 MB) |

After `make day2`:

```text
snapshot_date=2026-10-01  state=published  row_count=5147  new_count=30  changed_count=15  deleted_count=2
snapshot_date=2026-09-30  state=published  row_count=5119  new_count=5119  changed_count=0  deleted_count=0
```

The digest for the planted spike:

```text
| Severity | Scope | Entity | Fraud category | Complaints | Baseline median | Lift | z-score |
| critical | company | JPMORGAN CHASE & CO. | scam | 45 | 9.0 | 5.0x | 12.0 |
| high | state | Texas | scam | 29 | 11.5 | 2.5x | 5.2 |
```

Query the local warehouse with any DuckDB client:

```sql
select week_start, scope, fraud_category, complaint_count, robust_z_score, severity, alert_status
from marts.fct_scam_alerts;
```

# Configuration

Settings are environment variables, read from `.env` locally. [.env.example](.env.example) lists them all with safe placeholders. Never commit `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `CANARY_ENV` | `local` | `local`, `dev`, `qa` or `prod` |
| `CANARY_WAREHOUSE` | `duckdb` | `duckdb` or `bigquery` |
| `CANARY_CFPB_URL`, `CANARY_FDIC_URL`, `CANARY_CENSUS_URL` | The public sources | An https URL or a local file |
| `CANARY_LANDING_URI` | `data/landing` | A directory or `gs://bucket/prefix` |
| `CANARY_SAMPLE_PERCENT` | `100` | Stable sample of complaints to keep (DEV uses 5) |
| `CANARY_GATE_OVERRIDE` | `false` | Skip the row-count and delete-guard checks for one run |
| `CANARY_DBT_FULL_REFRESH` | `false` | Rebuild incremental models in full (DEV and QA) |
| `CANARY_GCP_PROJECT`, `CANARY_BQ_LOCATION`, `CANARY_DATASET_PREFIX` | empty | BigQuery target |
| `CANARY_ENABLE_SCHEDULES` | `false` | Turn on the daily schedule and sensors (PROD) |
| `CANARY_EVENT_SINK` | `stdout` | `cloud_logging` in the cloud |
| `CANARY_ZIP_POLICY_TAG` | empty | BigQuery policy tag applied to `fct_complaints.zip_code` |
| `DAGSTER_PG_PASSWORD` | none | Local Docker Compose only |

In the cloud these are written to the VM by Terraform. The only secret, the Dagster database password, is generated by Terraform, kept in Secret Manager and read at container start.

Alert thresholds and gate limits are dbt variables in [dbt/dbt_project.yml](dbt/dbt_project.yml).

# Development Guide

```bash
make setup
make format     # ruff format and autofix
make lint       # ruff, and sqlfluff in the BigQuery dialect
make test
make docker     # build the image
make tf-check   # terraform fmt -check and validate
```

Layout:

| Path | Content |
|---|---|
| `src/complaint_canary/` | Ingestion, warehouse adapters, ledger, pipeline steps, Dagster definitions, CLI |
| `dbt/` | Models, the SCD2 snapshot, seeds, macros, tests |
| `tests/` | pytest suite |
| `terraform/` | Infrastructure; `envs/` holds one variable file per environment |
| `deploy/` | Dagster instance config and the Compose file used on the cloud VMs |
| `.github/` | Workflows and composite actions |

Conventions:

- SQL is written in the subset BigQuery and DuckDB share. The few differences are isolated in `dbt/macros/cross_db.sql`.
- Curated knowledge lives in seeds: `fraud_taxonomy.csv` and `company_overrides.csv`.
- Python that Dagster inspects (`assets.py`, `definitions.py`) must not use `from __future__ import annotations`.

# Testing

```bash
make test
```

| Layer | What it covers |
|---|---|
| pytest, unit | Zip-to-Parquet conversion, header contract, truncated and corrupt files, stable sampling, FDIC pagination and retry, Census parsing, ledger ordering rules, event format, digest |
| pytest, end to end | The Dagster job for two consecutive snapshots on DuckDB; history and counts; the planted spike alerts and the planted flood does not; a re-run changes nothing; an older snapshot is refused; a truncated snapshot is rejected and leaves history and marts untouched; replay rebuilds identical history; the freshness watchdog |
| dbt data tests | Keys, relationships, accepted values, uniqueness of grains, history starts at the environment's first snapshot, taxonomy drift |
| dbt unit tests | Every condition of the alert rule, week maturity, fraud classification across both CFPB taxonomy eras, name normalisation |
| Integrity gate | Five dbt tests on each incoming snapshot, run before history can change |

CI runs all of it on every push, on DuckDB. BigQuery is first exercised in DEV.

# CI/CD Workflows

The image is built once, on the feature branch, and the same image is promoted through the environments. Nothing is rebuilt for PROD.

| | DEV | QA | PROD |
|---|---|---|---|
| Deployed by | Push to a feature branch | The same push, after DEV passes | Merge to `main` |
| Data | Stable 5% sample | Full | Full |
| VM | Started for the deploy, then stopped | Started for the deploy, then stopped | Always on |
| Purpose | First run on BigQuery | Rehearsal at real volume | Scheduled service, daily at 11:00 UTC |

```mermaid
flowchart TD
  subgraph BR[branch.yml: push to a feature branch]
    P[Push] --> CI[Lint and test: ruff, sqlfluff, pytest with dbt build on DuckDB, Terraform validate, Docker build]
    CI --> Q{Source hash already marked qa-passed?}
    Q -- yes --> SKIP[Nothing to deploy]
    Q -- no --> E{Image for this source hash in the DEV registry?}
    E -- no --> B[Build and push once: tag = source hash, label = commit SHA]
    E -- yes --> D1
    B --> D1[DEV: start VM, deploy by digest, run pipeline on a 5 percent sample, stop VM]
    D1 --> C2[Carbon-copy the digest to the QA registry, verify it is unchanged]
    C2 --> D2[QA: start VM, deploy by digest, run pipeline on full data, stop VM]
    D2 --> T[Tag that digest qa-passed-SOURCEHASH]
  end
  subgraph MN[main.yml: merge to main]
    M[Merge] --> V{Tag qa-passed-SOURCEHASH exists in the QA registry?}
    V -- no --> X[Fail closed: re-run the branch]
    V -- yes --> C3[Carbon-copy that digest to the PROD registry, verify it is unchanged]
    C3 --> D3[PROD: deploy by digest]
    D3 --> DOC[Publish dbt docs to GitHub Pages]
  end
  T -.-> M
  RB[Run main.yml by hand with an earlier source hash] -. rollback .-> V
```

- **Image identity**: the tag is a hash of the Git tree entries that go into the image (`src`, `dbt`, `deploy`, `Dockerfile`, lock file). It is the same for a branch head and for its merge commit, which a commit SHA is not. Changes to docs, Terraform or workflows leave it unchanged, so they reuse the tested image.
- **QA is the gate**: QA tags the exact digest it tested. `main` deploys only a digest carrying that tag, and fails if there is none.
- **Unchanged in transit**: images are copied between the environments' registries as carbon copies and the digest is checked after each copy. Registry tags are immutable.
- **Rollback**: run the `main` workflow by hand with an earlier source hash. Only QA-passed images can be deployed; there is no way to skip QA.
- **Identity**: each environment has its own deploy identity. PROD's accepts tokens from the `main` branch only.
- Terraform is applied by hand, not by the workflows.

Workflow files: [branch.yml](.github/workflows/branch.yml), [main.yml](.github/workflows/main.yml).

# Deployment and Operations

## Prerequisites

- A GCP account with billing, and three empty projects (or one shared project).
- On your user: Owner on each project, and Billing Account Costs Manager for the budget alert.
- Terraform 1.9 or later, the gcloud CLI, and `gcloud auth login` plus `gcloud auth application-default login`.
- A GitHub repository. Environment branch rules and branch protection need a public repository or a paid plan.

## Estimated cost

About $46 to $57 a month for all three environments in `us-central1`: PROD about $35 (the always-on `e2-medium` VM is $24.50), QA about $6, DEV about $5, plus up to $10 of BigQuery queries beyond the free 1 TiB. Prices were checked against secondary sources, not the official calculator; confirm before relying on them. Each environment has a budget alert (DEV 15, QA 15, PROD 70, in the billing account's currency; change `budget_amount` in `terraform/envs/<env>.tfvars` if that is not US dollars).

## Deploy

1. Create a bucket for Terraform state:
   ```bash
   gcloud storage buckets create gs://<state-bucket> --project <project> --location us-central1 --uniform-bucket-level-access
   ```
2. For `dev`, then `qa`, then `prod`: copy `terraform/envs/local.tfvars.example` to `terraform/envs/<env>.local.tfvars`, fill it in, and apply.
   ```bash
   make tf-apply ENV=dev TF_STATE_BUCKET=<state-bucket>
   terraform -chdir=terraform output github_environment_variables
   ```
3. In GitHub, under Settings:
   - **Environments**: create `dev`, `qa` and `prod`, and add the eight variables from that output to each. On `prod`, set "Deployment branches and tags" to `main` only.
   - **Pages**: set the source to "GitHub Actions".
   - **Branches**: protect `main`. Require a pull request and require branches to be up to date.
4. Push a feature branch. The `branch` workflow deploys DEV, then QA. The first QA run loads the full history; it took about 8 minutes.
5. Add the five jobs of the `branch` workflow as required status checks on `main`. GitHub lists a check only after it has run once, and treats a skipped job as passing, so require all five.
6. Open a pull request and merge it. The `main` workflow deploys PROD, which then runs daily at 11:00 UTC. To run it at once, use "Materialize all" in the Dagster UI.

## Monitor

- **Dagster UI**: `make ui-tunnel ENV=prod`, then http://localhost:3000.
- **Email alerts** (Cloud Monitoring): run failed or snapshot rejected; no snapshot published in 26 hours; orchestrator heartbeat missing; data quality warning; VM disk or CPU; budget at 50, 80 and 100%.
- **Dashboard**: "Complaint Canary (<env>)" in Cloud Monitoring.
- **Tables**: `ops.snapshot_ledger`, `ops.load_audit`, `ops.dq_results`; failing rows in the `dq_failures` dataset.

## Back up and restore

- SCD2 history is the only state that cannot be rebuilt from the source. It is protected by 90 days of landed snapshots (replayable), a weekly BigQuery table snapshot kept 4 weeks, and 7 days of time travel.
- Dagster's run history sits on a separate disk with daily snapshots kept 7 days. Losing it does not affect the data.
- Procedures: [docs/runbook.md](docs/runbook.md).

## Troubleshoot

[docs/runbook.md](docs/runbook.md) has a section per alert. Common first-deploy problems:

| Symptom | Cause and fix |
|---|---|
| `terraform apply` fails on the budget | With a permission error: you lack Billing Account Costs Manager. Set `enable_budget = false`. |
| `terraform apply` fails on the taxonomy or policy tag | Set `enable_policy_tags = false`. |
| Workflow cannot authenticate to GCP | A GitHub Environment variable is missing or `github_repository` in the tfvars does not match the repository. |
| "The VM did not become ready" | Read `/var/log/canary-startup.log` on the VM. The first boot installs Docker and takes a few minutes. |
| `main` fails with "No QA-passed image" | The merged branch was not up to date with `main`, or its `branch` run did not finish. Re-run the branch. |
| Re-applying after a destroy fails on the workload identity pool | Deleted pools are kept for 30 days. Undelete it, or wait. |

## Stop the spend

The PROD VM is most of the bill. Stopping it pauses the service and leaves everything else in place:

```bash
gcloud compute instances stop canary-prod --zone us-central1-a
```

Storage continues at about a dollar a month. To remove everything, see [Clean Up](#clean-up).

## Security and data handling

- **Reporting a vulnerability**: use GitHub's private vulnerability reporting (Security → Report a vulnerability). Please do not open a public issue.
- **Data**: all three sources are public. Complaints carry no names or narratives. `zip_code` is restricted by a BigQuery policy tag; analysts are granted the marts without that tag's reader role.
- **Access**: no inbound access to the VMs except SSH through Identity-Aware Proxy. Each environment has its own runtime and deploy identities.
- **Secrets**: none in the repository or the image. `.env` and `*.local.tfvars` are git-ignored. Terraform state holds the generated database password, so keep the state bucket private.
- **Retention**: landed snapshots 90 days in PROD (14 in QA, 7 in DEV); raw snapshot partitions 7 days; failing test rows 30 days; SCD2 history and marts until deleted.

# Clean Up

This removes every deployed resource. It deletes all data, including the SCD2 history, which cannot be rebuilt once the landed snapshots are gone.

1. Destroy the environments in reverse order. Each holds a permission on the one before it, so `dev` must go last.
   ```bash
   make tf-destroy ENV=prod TF_STATE_BUCKET=<state-bucket>
   make tf-destroy ENV=qa TF_STATE_BUCKET=<state-bucket>
   make tf-destroy ENV=dev TF_STATE_BUCKET=<state-bucket>
   ```
   Read each plan before confirming: it should list only `canary-<env>` resources.
2. Delete the Terraform state bucket, once all three destroys have succeeded:
   ```bash
   gcloud storage rm -r gs://<state-bucket>
   ```
3. Check nothing billable is left in each project:
   ```bash
   gcloud compute instances list --project <project>
   gcloud compute disks list --project <project>
   gcloud compute snapshots list --project <project>
   gcloud storage buckets list --project <project>
   bq ls --project_id <project>
   ```
   Disk snapshots taken by the PROD backup schedule are kept by design when the disk is deleted; delete any that remain.
4. If the projects were created only for this, delete them. That is the surest way to stop all charges: `gcloud projects delete <project>`.
5. In GitHub: delete the `dev`, `qa`, `prod` and `github-pages` environments, and unpublish the Pages site if you no longer want it.
6. Locally: `make down`, then remove `data/`, `.env`, `site/` and `terraform/envs/*.local.tfvars`.

Two things to know before re-deploying after a clean-up: deleted workload identity pools and deleted projects are kept for 30 days, and their names cannot be reused until then.
