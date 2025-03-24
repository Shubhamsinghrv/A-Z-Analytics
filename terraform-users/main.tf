# Declare the project_id variable
variable "project_id" {}

# Read clients from JSON
locals {
  client_data = jsondecode(file("${path.module}/clients.json"))
}

# Create buckets dynamically
resource "google_storage_bucket" "client_buckets" {
  for_each      = toset(local.client_data.clients)
  name          = "${each.key}-data"
  location      = "asia-south1"  # Change if needed
  storage_class = "STANDARD"
  project       = var.project_id
}
