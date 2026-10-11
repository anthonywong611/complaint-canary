# Data Engineering Project Plan Review: Iteration 3 (Scam Radar)

**Overall Rating:** 9.1 / 10

This is an exceptionally high-quality, production-ready data engineering project plan. It demonstrates deep operational maturity, strict adherence to modern software engineering practices (IaC, immutability, asset-based orchestration, testing gates), and pragmatic domain awareness of the 2026 CFPB data landscape.

---

## 1. Score Breakdown

| Metric | Score | Key Rationale |
| :--- | :---: | :--- |
| **Completeness** | **9.5 / 10** | Exhaustive coverage from upstream source constraints and ERD modeling to Terraform IaC, multi-env deployment, runbooks, and line-item GCP cost estimates. |
| **Reliability** | **9.0 / 10** | Defensive, gate-driven pipeline architecture with strict snapshot reconciliation, delete guards, replay capability, and state ledgers. Minor risk in single-VM metadata storage. |
| **Maintainability** | **9.2 / 10** | Exemplary CI/CD with immutable image digests, asset-centric lineage in Dagster, dbt contracts/tests, and local DuckDB execution. Minor maintenance debt in manual entity-resolution seeds. |
| **Scalability** | **8.5 / 10** | Compute/storage decoupling in BigQuery handles scale well, but daily full-snapshot row-hash diffing creates a linear scan-cost trajectory as historical volume grows. |
| **Relevancy** | **9.5 / 10** | Accurately accounts for 2026 CFPB portal shifts (narrative removals, export caps). Alerting statistics (MAD z-score, share lift) fit zero-inflated complaint distributions. |

---

## 2. Detailed Metric Evaluation

### Completeness — 9.5 / 10
* **Strengths**:
  * **End-to-End Scope**: Covers every phase of the lifecycle—ingestion, validation, SCD2 tracking, entity resolution, statistical alerting, IaC, CI/CD, and monitoring.
  * **Operational Context**: Includes explicit retention schedules per environment, RTO targets (15 min), runbook triggers, and line-item cost projections (~$46–$52/mo).
  * **Upstream Realities**: Explicitly addresses source constraints (100k row export caps, 403 blocks on custom User-Agents, missing `updated_at`, 2017 taxonomy reorganizations, and 2026 narrative removals).
* **Gaps**:
  * **Alert Routing Detail**: Specifies "Email alerts" via Cloud Monitoring, but lacks exact delivery mechanisms (e.g., SendGrid/SMTP vs. PagerDuty/PubSub sinks for operational alerts vs. daily signal digests).
  * **Data Governance Granularity**: Mentions policy tags on `zip_code` and read-only BigQuery roles, but omits explicit column masking syntax or fine-grained service account RBAC definitions.

### Reliability — 9.0 / 10
* **Strengths**:
  * **Defensive Integrity Gates**: The `ops.snapshot_ledger` combined with strict pre-SCD2 dbt source tests (row count floor, max daily growth ceiling, header verification) prevents upstream file truncations from polluting warehouse history.
  * **Delete Guard**: The 0.5% active row missing threshold stops mass-deletions or partial source drops from closing active SCD2 records prematurely.
  * **Deterministic State & Replayability**: Using the snapshot date (rather than the run clock) for SCD2 validity guarantees deterministic replays. The 90-day immutable GCS Parquet landing allows full re-hydration of SCD2 tables if state is corrupted.
* **Risks**:
  * **Single Point of Failure (SPOF)**: Hosting Dagster webserver, daemon, and Postgres metadata on a single GCE VM creates an availability risk. Although backed up by persistent disk snapshots, a mid-run node failure requires manual or scripted terraform/re-attach intervention.
  * **Lag Vulnerability**: The 15-day maturity window for alert evaluation assumes CFPB publishing delays remain static. A temporary systemic delay from CFPB would artificially drop complaint counts and trigger false baseline dips.

### Maintainability — 9.2 / 10
* **Strengths**:
  * **Build Once, Promote Anywhere**: Packaging code via Docker using git commit SHAs and promoting the identical digest across DEV -> QA -> PROD eliminates "works in QA, fails in Prod" drift.
  * **Asset-Centric Lineage**: Native mapping between dbt models, asset checks, and Dagster nodes provides clear root-cause analysis when tests fail.
  * **Developer Velocity**: Local DuckDB target with sample fixtures allows sub-second CI and offline local development without burning GCP cloud credits.
* **Risks**:
  * **SQL Dialect Divergence**: Running dbt against DuckDB locally and BigQuery in cloud requires careful macro abstraction. Complex window functions or date-arithmetic edge cases in DuckDB may pass CI but fail during QA BigQuery execution.
  * **Manual Entity Resolution Drift**: Relying on a curated static override seed for FDIC institution matching creates ongoing manual maintenance as bank M&A activity occurs.

### Scalability — 8.5 / 10
* **Strengths**:
  * **Memory-Bound Ingestion**: Streaming zip decompression directly to Parquet keeps VM memory usage constant ($O(1)$ RAM), fitting comfortably inside an `e2-medium` (4 GB RAM).
  * **Warehouse Partitioning**: Partitioning `fct_complaints` by `received_month` and clustering by `company_key, fraud_category` ensures efficient BigQuery slot utilization and scan-pruning for time-series alert runs.
* **Bottlenecks**:
  * **Full-Snapshot Diffing Cost Ceiling**: Because CFPB does not provide incremental deltas, computing SCD2 changes via full-table row hashing against 18.3M+ records requires reading the full historical dataset daily. As the dataset doubles over the next 3–5 years, BigQuery daily scan costs ($/TB scanned) and join/hash processing times will scale linearly.

### Relevancy — 9.5 / 10
* **Strengths**:
  * **Domain-Specific Statistics**: The alert engine addresses count-data sparsity by using Median Absolute Deviation (MAD) with a square-root scale floor ($\max(1.4826 \times \text{MAD}, \sqrt{\text{median}}, 1)$) to prevent division-by-zero on zero-baseline series.
  * **Share-Lift Noise Reduction**: Requiring both a Z-score spike *and* an increased share of company complaint volume effectively filters out broad, company-wide public relations events from true fraud-specific surges.
  * **2026 Currency**: Correctly reflects current 2026 CFPB system attributes (removal of dispute/consent fields and consumer narratives).

---

## 3. Critical Recommendations for Final Approval

1. **Optimize Full-Snapshot Hash Diffing**:
   * Instead of hashing the entire BigQuery table against the incoming raw snapshot daily, restrict the change-detection join to active SCD2 records (`valid_to IS NULL`) plus a rolling 30-day lookback window for late-arriving updates. This caps daily BigQuery scan costs regardless of how large the historical archive grows.

2. **Harden SQL Dialect Parity in CI**:
   * Ensure sqlfluff rules are strictly configured for BigQuery syntax in CI, and run `dbt compile --target qa` during PR checks to validate BigQuery-specific SQL before merging to `main`.

3. **Automate Entity-Resolution Exception Metrics**:
   * Add a dbt test/Dagster asset check that alerts if the percentage of unmatched bank complaint volume exceeds a set threshold (e.g., >5%), ensuring missing M&A mapping seeds are caught immediately.