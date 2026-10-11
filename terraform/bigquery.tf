resource "google_bigquery_dataset" "layer" {
  for_each = toset(local.layers)

  project       = var.project_id
  dataset_id    = "${local.dataset_prefix}${each.value}"
  friendly_name = "Complaint Canary ${local.env}: ${each.value}"
  description   = "Complaint Canary ${each.value} layer (${local.env})."
  location      = var.bq_location
  labels        = merge(local.labels, { layer = each.value })

  # Rows that failed a data test are kept for a month.
  default_table_expiration_ms = each.value == "dq_failures" ? 30 * 24 * 60 * 60 * 1000 : null

  delete_contents_on_destroy = var.force_destroy

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset_iam_member" "pipeline_editor" {
  for_each = google_bigquery_dataset.layer

  project    = var.project_id
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.pipeline.member
}

resource "google_bigquery_dataset_iam_member" "analyst_marts" {
  count = var.analyst_group_email == "" ? 0 : 1

  project    = var.project_id
  dataset_id = google_bigquery_dataset.layer["marts"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "group:${var.analyst_group_email}"
}

resource "google_project_iam_member" "analyst_jobs" {
  count = var.analyst_group_email == "" ? 0 : 1

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "group:${var.analyst_group_email}"
}

# --- Column-level security for ZIP codes ---------------------------------------
# Analysts get the marts but not the policy tag's Fine-Grained Reader role, so
# fct_complaints.zip_code is hidden from them.

resource "google_data_catalog_taxonomy" "sensitivity" {
  count = var.enable_policy_tags ? 1 : 0

  project                = var.project_id
  region                 = lower(var.bq_location)
  display_name           = "${local.name}-sensitivity"
  description            = "Column sensitivity for Complaint Canary (${local.env})."
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]

  depends_on = [google_project_service.apis]
}

resource "google_data_catalog_policy_tag" "consumer_location" {
  count = var.enable_policy_tags ? 1 : 0

  taxonomy     = google_data_catalog_taxonomy.sensitivity[0].id
  display_name = "consumer_location"
  description  = "Consumer ZIP code: a quasi-identifier when combined with other fields."
}

resource "google_data_catalog_policy_tag_iam_member" "pipeline_reader" {
  count = var.enable_policy_tags ? 1 : 0

  policy_tag = google_data_catalog_policy_tag.consumer_location[0].name
  role       = "roles/datacatalog.categoryFineGrainedReader"
  member     = google_service_account.pipeline.member
}

# Attaching a policy tag to a column needs to read the taxonomy and to set the
# column's category, which only table owners may do.
resource "google_project_iam_member" "pipeline_taxonomy_viewer" {
  count = var.enable_policy_tags ? 1 : 0

  project = var.project_id
  role    = "roles/datacatalog.viewer"
  member  = google_service_account.pipeline.member
}

resource "google_bigquery_dataset_iam_member" "pipeline_marts_owner" {
  count = var.enable_policy_tags ? 1 : 0

  project    = var.project_id
  dataset_id = google_bigquery_dataset.layer["marts"].dataset_id
  role       = "roles/bigquery.dataOwner"
  member     = google_service_account.pipeline.member
}
