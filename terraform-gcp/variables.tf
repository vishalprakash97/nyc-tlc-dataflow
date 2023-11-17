locals {
  data_lake_bucket="dtc_data_lake"
}

variable "project" {
  description = "GCP Project ID"
  #enter project ID at runtime
}

variable "region" {
  description = "region for resource allocation"
  default = "us-east1"
  type = string
}

variable "credentials" {
  description = "json file contatining gcp credentialst"
}

variable "storage_class" {
  description = "Storage class type for bucket"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "Big Query Dataset that raw data will be written to"
  type = string
  default = "yellow_taxi_trips_all"
}