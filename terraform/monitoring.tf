# The pipeline writes structured events to the "complaint-canary" log. Each alert
# below is a log-based metric over those events, or a VM metric.

locals {
  event_filter = join(" AND ", [
    "logName=\"projects/${var.project_id}/logs/complaint-canary\"",
    "jsonPayload.env=\"${local.env}\"",
  ])

  event_metrics = {
    failures = {
      description = "Runs that failed, snapshots rejected by the integrity gate, and snapshots refused as out of order."
      filter      = "jsonPayload.event=(\"run_failure\" OR \"snapshot_rejected\" OR \"snapshot_refused\")"
    }
    stale = {
      description = "The freshness watchdog found no snapshot published in the last 26 hours."
      filter      = "jsonPayload.event=\"snapshot_stale\""
    }
    warnings = {
      description = "Data quality warnings: publication lag, unreviewed companies, taxonomy drift, scale, failed backups."
      filter      = "jsonPayload.event=(\"dq_warning\" OR \"backup_failed\")"
    }
    published = {
      description = "Snapshots published."
      filter      = "jsonPayload.event=\"snapshot_published\""
    }
    heartbeat = {
      description = "Hourly heartbeat from the orchestrator."
      filter      = "jsonPayload.event=\"heartbeat\""
    }
  }
}

resource "google_logging_metric" "events" {
  for_each = local.event_metrics

  project     = var.project_id
  name        = "canary_${local.env}_${each.key}"
  description = each.value.description
  filter      = "${local.event_filter} AND ${each.value.filter}"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }

  depends_on = [google_project_service.apis]
}

resource "google_monitoring_notification_channel" "email" {
  project      = var.project_id
  display_name = "Complaint Canary ${local.env} alerts"
  type         = "email"

  labels = {
    email_address = var.alert_email
  }

  depends_on = [google_project_service.apis]
}

locals {
  count_alerts = {
    failures = {
      display_name = "Run failed or snapshot rejected"
      severity     = "ERROR"
      runbook      = "A pipeline run failed, or the integrity gate rejected a snapshot. History and marts are unchanged. See docs/runbook.md."
    }
    stale = {
      display_name = "No snapshot published in 26 hours"
      severity     = "ERROR"
      runbook      = "The orchestrator is up but nothing has been published for more than a day. See docs/runbook.md."
    }
    warnings = {
      display_name = "Data quality warning"
      severity     = "WARNING"
      runbook      = "A warn-level check fired. Find the test name in the complaint-canary log. See docs/runbook.md."
    }
  }
}

resource "google_monitoring_alert_policy" "event_count" {
  for_each = local.count_alerts

  project      = var.project_id
  display_name = "[canary ${local.env}] ${each.value.display_name}"
  combiner     = "OR"
  severity     = each.value.severity

  conditions {
    display_name = each.value.display_name

    condition_threshold {
      filter = join(" AND ", [
        "metric.type=\"logging.googleapis.com/user/${google_logging_metric.events[each.key].name}\"",
        "resource.type=\"global\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "86400s"
  }

  documentation {
    content   = each.value.runbook
    mime_type = "text/markdown"
  }

  notification_channels = [google_monitoring_notification_channel.email.id]
}

# The watchdog cannot report its own death, so its heartbeat going quiet is an alert too.
resource "google_monitoring_alert_policy" "orchestrator_down" {
  count = var.enable_schedules ? 1 : 0

  project      = var.project_id
  display_name = "[canary ${local.env}] Orchestrator heartbeat missing"
  combiner     = "OR"
  severity     = "ERROR"

  conditions {
    display_name = "No heartbeat for 3 hours"

    condition_absent {
      filter = join(" AND ", [
        "metric.type=\"logging.googleapis.com/user/${google_logging_metric.events["heartbeat"].name}\"",
        "resource.type=\"global\"",
      ])
      duration = "10800s"

      aggregations {
        alignment_period     = "3600s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  documentation {
    content   = "The Dagster daemon has stopped reporting: the VM or its containers are down. See docs/runbook.md, 'VM rebuild'."
    mime_type = "text/markdown"
  }

  notification_channels = [google_monitoring_notification_channel.email.id]
}

resource "google_monitoring_alert_policy" "vm_resources" {
  count = var.vm_running ? 1 : 0

  project      = var.project_id
  display_name = "[canary ${local.env}] VM disk or CPU"
  combiner     = "OR"
  severity     = "WARNING"

  conditions {
    display_name = "Disk more than 80% full"

    condition_threshold {
      filter = join(" AND ", [
        "metric.type=\"agent.googleapis.com/disk/percent_used\"",
        "resource.type=\"gce_instance\"",
        "resource.labels.instance_id=\"${google_compute_instance.canary.instance_id}\"",
        "metric.labels.state=\"used\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 80
      duration        = "900s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  conditions {
    display_name = "CPU above 90% for 30 minutes"

    condition_threshold {
      filter = join(" AND ", [
        "metric.type=\"compute.googleapis.com/instance/cpu/utilization\"",
        "resource.type=\"gce_instance\"",
        "resource.labels.instance_id=\"${google_compute_instance.canary.instance_id}\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0.9
      duration        = "1800s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  documentation {
    content   = "The VM is short of disk or CPU. See docs/runbook.md."
    mime_type = "text/markdown"
  }

  notification_channels = [google_monitoring_notification_channel.email.id]
}

resource "google_monitoring_dashboard" "overview" {
  project = var.project_id

  dashboard_json = jsonencode({
    displayName = "Complaint Canary (${local.env})"
    gridLayout = {
      columns = 2
      widgets = concat(
        [
          for key in ["published", "failures", "warnings", "heartbeat"] : {
            title = "Events: ${key}"
            xyChart = {
              dataSets = [{
                plotType = "STACKED_BAR"
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.events[key].name}\" resource.type=\"global\""
                    aggregation = {
                      alignmentPeriod    = "3600s"
                      perSeriesAligner   = "ALIGN_SUM"
                      crossSeriesReducer = "REDUCE_SUM"
                    }
                  }
                }
              }]
            }
          }
        ],
        [
          {
            title = "VM CPU utilisation"
            xyChart = {
              dataSets = [{
                plotType = "LINE"
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"compute.googleapis.com/instance/cpu/utilization\" resource.type=\"gce_instance\" resource.labels.instance_id=\"${google_compute_instance.canary.instance_id}\""
                    aggregation = {
                      alignmentPeriod  = "300s"
                      perSeriesAligner = "ALIGN_MEAN"
                    }
                  }
                }
              }]
            }
          },
          {
            title = "VM disk used (%)"
            xyChart = {
              dataSets = [{
                plotType = "LINE"
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"agent.googleapis.com/disk/percent_used\" resource.type=\"gce_instance\" resource.labels.instance_id=\"${google_compute_instance.canary.instance_id}\" metric.labels.state=\"used\""
                    aggregation = {
                      alignmentPeriod  = "300s"
                      perSeriesAligner = "ALIGN_MEAN"
                    }
                  }
                }
              }]
            }
          },
        ]
      )
    }
  })

  depends_on = [google_project_service.apis]
}
