# --- Runtime identity: the VM ----------------------------------------------------

resource "google_service_account" "pipeline" {
  project      = var.project_id
  account_id   = "data-pipeline-${local.env}"
  display_name = "Complaint Canary pipeline (${local.env})"

  depends_on = [google_project_service.apis]
}

resource "google_project_iam_member" "pipeline" {
  for_each = toset([
    "roles/bigquery.jobUser",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ])

  project = var.project_id
  role    = each.value
  member  = google_service_account.pipeline.member
}

# --- Deploy identity: GitHub Actions through Workload Identity Federation --------

resource "google_service_account" "deploy" {
  project      = var.project_id
  account_id   = "github-deploy-${local.env}"
  display_name = "Complaint Canary GitHub deploys (${local.env})"

  depends_on = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool" "github" {
  project                   = var.project_id
  workload_identity_pool_id = "${local.name}-github"
  display_name              = "GitHub Actions (${local.env})"

  depends_on = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  # DEV and QA accept any branch of this repository. PROD accepts main only.
  attribute_condition = local.env == "prod" ? (
    "assertion.repository == \"${var.github_repository}\" && assertion.ref == \"refs/heads/main\""
    ) : (
    "assertion.repository == \"${var.github_repository}\""
  )

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "deploy_from_github" {
  service_account_id = google_service_account.deploy.name
  role               = "roles/iam.workloadIdentityUser"
  member = join("", [
    "principalSet://iam.googleapis.com/",
    google_iam_workload_identity_pool.github.name,
    "/attribute.repository/",
    var.github_repository,
  ])
}

# SSH to the VM over IAP, as an administrator, to run the deploy script.
resource "google_project_iam_member" "deploy_compute_viewer" {
  project = var.project_id
  role    = "roles/compute.viewer"
  member  = google_service_account.deploy.member
}

resource "google_compute_instance_iam_member" "deploy_instance" {
  for_each = toset([
    "roles/compute.instanceAdmin.v1",
    "roles/compute.osAdminLogin",
  ])

  project       = var.project_id
  zone          = google_compute_instance.canary.zone
  instance_name = google_compute_instance.canary.name
  role          = each.value
  member        = google_service_account.deploy.member
}

resource "google_iap_tunnel_instance_iam_member" "deploy_tunnel" {
  project  = var.project_id
  zone     = google_compute_instance.canary.zone
  instance = google_compute_instance.canary.name
  role     = "roles/iap.tunnelResourceAccessor"
  member   = google_service_account.deploy.member
}

resource "google_service_account_iam_member" "deploy_uses_pipeline" {
  service_account_id = google_service_account.pipeline.name
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.deploy.member
}
