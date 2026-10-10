# PROD: the scheduled service.
environment            = "prod"
upstream_environment   = "qa"
vm_running             = true
enable_schedules       = true
sample_percent         = 100
dbt_full_refresh       = false
landing_retention_days = 90
enable_backups         = true
budget_amount          = 70
