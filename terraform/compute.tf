# --- Dagster metadata database password -----------------------------------------

resource "random_password" "dagster_pg" {
  length  = 32
  special = false
}

resource "google_secret_manager_secret" "dagster_pg" {
  project   = var.project_id
  secret_id = "${local.name}-dagster-pg-password"
  labels    = local.labels

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "dagster_pg" {
  secret      = google_secret_manager_secret.dagster_pg.id
  secret_data = random_password.dagster_pg.result
}

resource "google_secret_manager_secret_iam_member" "pipeline" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.dagster_pg.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.pipeline.member
}

# --- VM --------------------------------------------------------------------------

resource "google_compute_disk" "data" {
  project = var.project_id
  name    = "${local.name}-data"
  zone    = var.zone
  type    = "pd-balanced"
  size    = 10
  labels  = local.labels

  depends_on = [google_project_service.apis]
}

resource "google_compute_instance" "canary" {
  project      = var.project_id
  name         = local.name
  zone         = var.zone
  machine_type = var.machine_type
  tags         = [local.name]
  labels       = local.labels

  desired_status            = var.vm_running ? "RUNNING" : "TERMINATED"
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 30
      type  = "pd-balanced"
    }
  }

  attached_disk {
    source      = google_compute_disk.data.id
    device_name = "canary-data"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.canary.id

    # An ephemeral public address for outbound calls to CFPB, FDIC and Docker Hub.
    # It costs less than Cloud NAT; inbound traffic is still limited to IAP by the firewall.
    access_config {}
  }

  service_account {
    email  = google_service_account.pipeline.email
    scopes = ["cloud-platform"]
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
  }

  metadata = {
    enable-oslogin         = "TRUE"
    enable-osconfig        = "TRUE"
    block-project-ssh-keys = "TRUE"
    startup-script = templatefile("${path.module}/templates/startup.sh.tftpl", {
      environment      = local.env
      project_id       = var.project_id
      bq_location      = var.bq_location
      dataset_prefix   = local.dataset_prefix
      landing_bucket   = google_storage_bucket.landing.name
      sample_percent   = var.sample_percent
      dbt_full_refresh = var.dbt_full_refresh
      enable_schedules = var.enable_schedules
      enable_backups   = var.enable_backups
      zip_policy_tag   = var.enable_policy_tags ? google_data_catalog_policy_tag.consumer_location[0].name : ""
      registry_host    = local.registry_host
      pg_secret_id     = google_secret_manager_secret.dagster_pg.secret_id
    })
  }

  depends_on = [
    google_secret_manager_secret_version.dagster_pg,
    google_secret_manager_secret_iam_member.pipeline,
  ]
}

# --- Daily snapshots of the metadata disk, kept 7 days ---------------------------

resource "google_compute_resource_policy" "data_snapshots" {
  count = var.enable_backups ? 1 : 0

  project = var.project_id
  name    = "${local.name}-data-daily"
  region  = var.region

  snapshot_schedule_policy {
    schedule {
      daily_schedule {
        days_in_cycle = 1
        start_time    = "06:00"
      }
    }
    retention_policy {
      max_retention_days    = 7
      on_source_disk_delete = "APPLY_RETENTION_POLICY"
    }
    snapshot_properties {
      labels = local.labels
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_compute_disk_resource_policy_attachment" "data_snapshots" {
  count = var.enable_backups ? 1 : 0

  project = var.project_id
  name    = google_compute_resource_policy.data_snapshots[0].name
  disk    = google_compute_disk.data.name
  zone    = var.zone
}

# --- Weekly OS patching ----------------------------------------------------------

resource "google_os_config_patch_deployment" "weekly" {
  project             = var.project_id
  patch_deployment_id = "${local.name}-weekly"

  instance_filter {
    instances = [google_compute_instance.canary.id]
  }

  patch_config {
    reboot_config = "DEFAULT"

    apt {
      type = "UPGRADE"
    }
  }

  recurring_schedule {
    time_zone {
      id = "Etc/UTC"
    }

    # Sunday 04:00 UTC, well clear of the 11:00 UTC daily run.
    time_of_day {
      hours = 4
    }

    weekly {
      day_of_week = "SUNDAY"
    }
  }

  depends_on = [google_project_service.apis]
}
