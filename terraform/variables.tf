variable "project_id" {
  description = "The ID of the project in which to create resources."
  type        = string
  default     = "de-zoom-camp-course"
}

variable "region" {
  description = "The region to create resources in."
  type        = string
  default     = "europe-west3"
}

# Variables for Storage Bucket
variable "bucket_name" {
  description = "The name of the storage bucket."
  type        = string
  default     = "my-default-data-lake-bucket"
}

variable "storage_class" {
  description = "The storage class of the bucket."
  type        = string
  default     = "STANDARD"
}

variable "versioning" {
  description = "Enable versioning for the bucket."
  type        = bool
  default     = true
}

variable "lifecycle_age" {
  description = "The age in days after which to delete objects."
  type        = number
  default     = 365
}

# Variables for BigQuery Dataset
variable "dataset_id" {
  description = "The ID of the BigQuery dataset."
  type        = string
  default     = "my_default_dataset"
}

variable "dataset_description" {
  description = "The description of the BigQuery dataset."
  type        = string
  default     = "This is my default BigQuery dataset."
}

variable "table_expiration_ms" {
  description = "Default expiration time for tables in milliseconds."
  type        = number
  default     = null
}
