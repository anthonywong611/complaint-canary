output "github_environment_variables" {
  description = "Set these as variables on the GitHub Environment named after this environment."
  value = {
    GCP_PROJECT_ID     = var.project_id
    GCP_REGION         = var.region
    GCP_WIF_PROVIDER   = google_iam_workload_identity_pool_provider.github.name
    GCP_DEPLOY_SA      = google_service_account.deploy.email
    GCP_VM_NAME        = google_compute_instance.canary.name
    GCP_VM_ZONE        = google_compute_instance.canary.zone
    GCP_IMAGE          = local.image_name
    GCP_UPSTREAM_IMAGE = local.upstream_image_name
  }
}

output "landing_bucket" {
  description = "Landing-zone bucket."
  value       = google_storage_bucket.landing.name
}

output "datasets" {
  description = "BigQuery datasets by layer."
  value       = { for layer, dataset in google_bigquery_dataset.layer : layer => dataset.dataset_id }
}

output "dagster_ui_tunnel" {
  description = "Command that forwards the Dagster UI to http://localhost:3000."
  value = join(" ", [
    "gcloud compute ssh ${google_compute_instance.canary.name}",
    "--project ${var.project_id} --zone ${google_compute_instance.canary.zone}",
    "--tunnel-through-iap -- -N -L 3000:localhost:3000",
  ])
}

output "monitoring_dashboard" {
  description = "Cloud Monitoring dashboard resource."
  value       = google_monitoring_dashboard.overview.id
}
