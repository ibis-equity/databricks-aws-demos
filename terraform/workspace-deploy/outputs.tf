output "workspace_root" {
  description = "Root workspace path where notebooks were deployed"
  value       = var.workspace_root
}

output "deployed_notebook_count" {
  description = "Total number of notebooks deployed"
  value       = length(databricks_notebook.project_notebooks)
}

output "sample_notebook_paths" {
  description = "Sample deployed notebook paths"
  value       = slice(sort([for n in databricks_notebook.project_notebooks : n.path]), 0, min(10, length(databricks_notebook.project_notebooks)))
}

