resource "google_compute_network" "canary" {
  project                 = var.project_id
  name                    = local.name
  auto_create_subnetworks = false

  depends_on = [google_project_service.apis]
}

resource "google_compute_subnetwork" "canary" {
  project       = var.project_id
  name          = local.name
  region        = var.region
  network       = google_compute_network.canary.id
  ip_cidr_range = "10.10.0.0/24"

  private_ip_google_access = true
}

# The only way in: SSH from Google's Identity-Aware Proxy range. The Dagster UI is
# reached through an SSH tunnel; nothing else listens on the network interface.
resource "google_compute_firewall" "iap_ssh" {
  project   = var.project_id
  name      = "${local.name}-allow-iap-ssh"
  network   = google_compute_network.canary.id
  direction = "INGRESS"

  source_ranges = ["35.235.240.0/20"]
  target_tags   = [local.name]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}
