#terrafrom version
terraform {
  required_version = ">=1.0"
  backend "local" {}
  required_providers {
    google = {
        source = "hashicorp/google"
    }
  }
}

#provider information
provider "google" {
  region = var.region
  project = var.project
  credentials = file(var.credentials)
}

#Create cloud stroage (Data Lake)
resource "google_storage_bucket" "data-lake-bucket" {
  location = var.region
  name = "${local.data_lake_bucket}_${var.project}"

  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
  force_destroy = true
}

#Create Big Query Dataset ( Data Warehouse)
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  location = var.region
  project = var.project
  
}