# Budget alert. It notifies; it does not stop spending.
resource "google_billing_budget" "environment" {
  count = var.enable_budget ? 1 : 0

  billing_account = var.billing_account_id
  display_name    = "Complaint Canary ${local.env}"

  budget_filter {
    # Scoped to the project. If all three environments share one project, enable the
    # budget in one of them only and give it the whole amount.
    projects = ["projects/${data.google_project.this.number}"]
  }

  # No currency code: the API then uses the billing account's own currency. Naming a
  # different one is rejected with "Request contains an invalid argument".
  amount {
    specified_amount {
      units = tostring(var.budget_amount)
    }
  }

  dynamic "threshold_rules" {
    for_each = [0.5, 0.8, 1.0]

    content {
      threshold_percent = threshold_rules.value
      spend_basis       = "CURRENT_SPEND"
    }
  }

  all_updates_rule {
    monitoring_notification_channels = [google_monitoring_notification_channel.email.id]
    disable_default_iam_recipients   = false
  }

  depends_on = [google_project_service.apis]
}
