# QA: branch rehearsal on full data. The VM is off between deploys.
environment            = "qa"
upstream_environment   = "dev"
vm_running             = false
enable_schedules       = false
sample_percent         = 100
dbt_full_refresh       = true
landing_retention_days = 14
enable_backups         = false
budget_amount          = 15
