variable "databricks_host" {
  description = "Databricks workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token for the target workspace"
  type        = string
  sensitive   = true
}

variable "workspace_root" {
  description = "Workspace folder where notebooks will be deployed"
  type        = string
  default     = "/Shared/databricks-aws-demos"
}

variable "project_root" {
  description = "Absolute path to the local project root"
  type        = string
  default     = "../.."
}

variable "include_dirs" {
  description = "Project directories to deploy into the Databricks workspace"
  type        = list(string)
  default = [
    "_resources",
    "01. Ingestion",
    "02. Processing",
    "03. Serving",
  ]
}


