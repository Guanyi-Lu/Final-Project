terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.10.0"  # Use the latest stable version
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# ========== STORAGE ==========
resource "google_storage_bucket" "raw_bucket_pk" {
  name          = var.gcs_bucket_raw #the name in the cosole is raw_bucket_guanyi
  location      = var.location
  force_destroy = true
  lifecycle_rule {
    condition { age = 30 } # Auto-delete raw files after 30 days
    action { type = "Delete" }
  }
}

resource "google_storage_bucket" "processed_bucket_pk" {
  name          = var.gcs_bucket_parquet #the name in the cosole is parquet_data_bucket
  location      = var.location
  force_destroy = true
}

# ========== BIGQUERY ==========
resource "google_bigquery_dataset" "mortality_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_bigquery_table" "mortality_aggregated" {
  dataset_id = google_bigquery_dataset.mortality_dataset.dataset_id
  table_id   = var.bq_table_name
  deletion_protection = false

}



# ========== CLOUD COMPOSER ==========
resource "google_composer_environment" "mortality_pipeline" {
  name   = "mortality-pipeline-composer"
  region = var.region

  config {
    software_config {
      image_version = "composer-3-airflow-2.10.2-build.11"  # Ensure compatibility with Composer 3
      pypi_packages = {
        "beautifulsoup4" = ">=4.11.0"
        "pyspark"       = ""
        "pandas"        = ">=2.0.0"
        "zipfile36"     = ""
      }
      env_variables = {
        "GCP_GUANYI_PROJECT"  = var.project
        "GCS_RAW_BUCKET"       = google_storage_bucket.raw_bucket_guanyi.name
        "GCS_PROCESSED_BUCKET" = google_storage_bucket.processed_bucket_pk.name
        "BQ_DATASET"           = google_bigquery_dataset.mortality_dataset.dataset_id
      }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 10
        min_count  = 1
        max_count  = 3
      }
    }

    node_config {
      service_account = "820191181720-compute@developer.gserviceaccount.com	"
      network = "default"  
      subnetwork = "default" 
    }
  }
}



# ========== VARIABLES ==========
variable "credentials" {
  description = "Guanyi Credentials"
  default     = "./my-keys.json"

}

variable "project" {
  description = "Project"
  default     = "oval-sunset-455016-n0"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "us-central1"
}


variable "gcs_bucket_raw" {
  description = "My Storage Bucket Name for raw data"
  #Update the below to a unique bucket name
  default = "raw_bucket_guanyi"
}

variable "gcs_bucket_parquet" {
  description = "My Storage Bucket Name for cleanned non-aggregated data"
  #Update the below to a unique bucket name
  default = "parquet_data_bucket"
}

variable "bq_dataset_name" {
  description = "My Final Project BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "Australia_Mortality_Dataset"
}

variable "bq_table_name" {
  description = "My Final Project BigQuery Table Name"
  #Update the below to what you want your table to be called
  default = "Australia_Mortality_Aggregated_Table"
}

# ========== OUTPUTS ==========
output "composer_airflow_uri" {
  value = google_composer_environment.mortality_pipeline.config.0.airflow_uri
}

output "raw_bucket_name" {
  value = google_storage_bucket.raw_bucket_guanyi.name
}

output "processed_bucket_name" {
  value = google_storage_bucket.processed_bucket_pk.name
}