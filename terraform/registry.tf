resource "google_artifact_registry_repository" "images" {
  project       = var.project_id
  location      = var.region
  repository_id = local.name
  description   = "Complaint Canary images (${local.env})."
  format        = "DOCKER"
  labels        = local.labels

  # A tag can never move to a different image, so a tested tag stays the tested image.
  docker_config {
    immutable_tags = true
  }

  depends_on = [google_project_service.apis]
}

resource "google_artifact_registry_repository_iam_member" "deploy_writer" {
  project    = var.project_id
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.writer"
  member     = google_service_account.deploy.member
}

resource "google_artifact_registry_repository_iam_member" "pipeline_reader" {
  project    = var.project_id
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.reader"
  member     = google_service_account.pipeline.member
}

# Promotion: this environment's deployer reads images from the one before it.
resource "google_artifact_registry_repository_iam_member" "deploy_reads_upstream" {
  count = var.upstream_environment == "" ? 0 : 1

  project    = var.upstream_project_id
  location   = var.region
  repository = "canary-${var.upstream_environment}"
  role       = "roles/artifactregistry.reader"
  member     = google_service_account.deploy.member
}
