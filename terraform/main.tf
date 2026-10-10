locals {
  env            = var.environment
  name           = "canary-${local.env}"
  dataset_prefix = "canary_${local.env}_"
  labels = {
    app         = "complaint-canary"
    environment = local.env
    managed_by  = "terraform"
  }

  layers = ["raw", "snapshots", "staging", "intermediate", "marts", "ops", "dq_failures"]

  registry_host = "${var.region}-docker.pkg.dev"
  image_name    = "${local.registry_host}/${var.project_id}/${local.name}/complaint-canary"
  upstream_image_name = var.upstream_environment == "" ? "" : (
    "${local.registry_host}/${var.upstream_project_id}/canary-${var.upstream_environment}/complaint-canary"
  )

  apis = [
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "billingbudgets.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "compute.googleapis.com",
    "datacatalog.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "iap.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "osconfig.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
    "sts.googleapis.com",
  ]
}

data "google_project" "this" {
  project_id = var.project_id
}

resource "google_project_service" "apis" {
  for_each = toset(local.apis)

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
