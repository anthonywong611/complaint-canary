# DEV: first cloud run of a branch, on a stable 5% sample. The VM is off between deploys.
environment            = "dev"
vm_running             = false
enable_schedules       = false
sample_percent         = 5
dbt_full_refresh       = true
landing_retention_days = 7
enable_backups         = false
budget_amount          = 15
