variable "environment" {
  description = "Environment name. Every resource name carries it, so all three can share one project."
  type        = string

  validation {
    condition     = contains(["dev", "qa", "prod"], var.environment)
    error_message = "environment must be dev, qa or prod."
  }
}

variable "project_id" {
  description = "GCP project that hosts this environment."
  type        = string
}

variable "region" {
  description = "Region for the VM, bucket and registry."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Zone for the VM."
  type        = string
  default     = "us-central1-a"
}

variable "bq_location" {
  description = "BigQuery dataset location."
  type        = string
  default     = "US"
}

variable "github_repository" {
  description = "GitHub repository allowed to deploy, as owner/name."
  type        = string
}

variable "alert_email" {
  description = "Address that receives operational and budget alerts."
  type        = string
}

variable "analyst_group_email" {
  description = "Google group granted read access to the marts. Empty to skip."
  type        = string
  default     = ""
}

# --- Promotion ---------------------------------------------------------------

variable "upstream_project_id" {
  description = "Project of the environment this one promotes images from (dev for qa, qa for prod). Empty for dev."
  type        = string
  default     = ""
}

variable "upstream_environment" {
  description = "Environment this one promotes images from. Empty for dev."
  type        = string
  default     = ""
}

# --- Environment shape -------------------------------------------------------

variable "machine_type" {
  description = "VM machine type."
  type        = string
  default     = "e2-medium"
}

variable "vm_running" {
  description = "Whether the VM stays on. DEV and QA are off and are started by deploys."
  type        = bool
  default     = false
}

variable "enable_schedules" {
  description = "Run the daily schedule and sensors. PROD only."
  type        = bool
  default     = false
}

variable "sample_percent" {
  description = "Percentage of complaints kept by ingestion (stable sample)."
  type        = number
  default     = 100
}

variable "dbt_full_refresh" {
  description = "Rebuild incremental models in full on every run. DEV and QA."
  type        = bool
  default     = false
}

variable "landing_retention_days" {
  description = "Days to keep landed snapshot files. This is how far back history can be replayed."
  type        = number
  default     = 7
}

variable "enable_backups" {
  description = "Daily snapshots of the Dagster metadata disk and weekly table snapshots of SCD2 history. PROD only."
  type        = bool
  default     = false
}

variable "force_destroy" {
  description = "Let terraform destroy remove buckets and datasets that still hold data."
  type        = bool
  default     = true
}

# --- Optional features -------------------------------------------------------

variable "enable_policy_tags" {
  description = "Restrict fct_complaints.zip_code with a BigQuery policy tag. Needs Data Catalog admin rights to create."
  type        = bool
  default     = true
}

variable "enable_budget" {
  description = "Create a billing budget with email alerts. Needs Billing Account Costs Manager."
  type        = bool
  default     = true
}

variable "billing_account_id" {
  description = "Billing account for the budget, as XXXXXX-XXXXXX-XXXXXX."
  type        = string
  default     = ""
}

variable "budget_amount" {
  description = "Monthly budget for this environment, in the billing account's currency."
  type        = number
  default     = 15
}
