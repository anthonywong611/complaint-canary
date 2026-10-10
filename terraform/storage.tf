# Landing zone: immutable Parquet snapshots, reference files and digests.
resource "google_storage_bucket" "landing" {
  name     = "${local.name}-landing-${var.project_id}"
  project  = var.project_id
  location = var.region
  labels   = local.labels

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy

  versioning {
    enabled = true
  }

  # Snapshot files older than the retention window can no longer be replayed.
  lifecycle_rule {
    condition {
      age            = var.landing_retention_days
      matches_prefix = ["cfpb/"]
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 7
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket_iam_member" "pipeline_landing" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.pipeline.member
}
