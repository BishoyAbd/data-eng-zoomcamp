terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }

  required_version = ">= 0.14.0"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Create Google Cloud Storage Bucket
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  storage_class = var.storage_class
  force_destroy = true  # Forces deletion of bucket and all its contents

  versioning {
    enabled = var.versioning
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age = var.lifecycle_age
    }
  }
}

# Create BigQuery Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.dataset_id
  project                     = var.project_id
  location                    = var.region
  description                 = var.dataset_description
  default_table_expiration_ms = var.table_expiration_ms
  delete_contents_on_destroy = true
  labels = {
    environment = "prod"
  }
}
