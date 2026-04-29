locals {
  notebook_extensions = ["py", "sql"]

  notebook_files = flatten([
    for d in var.include_dirs : [
      for ext in local.notebook_extensions : [
        for f in fileset("${var.project_root}/${d}", "**/*.${ext}") : "${d}/${f}"
      ]
    ]
  ])

  notebook_map = {
    for rel in flatten(local.notebook_files) : rel => {
      source_path    = "${var.project_root}/${rel}"
      workspace_path = "${var.workspace_root}/${rel}"
      extension      = lower(element(reverse(split(".", rel)), 0))
    }
  }
}

resource "databricks_directory" "project_root" {
  path = var.workspace_root
}

resource "databricks_notebook" "project_notebooks" {
  for_each = local.notebook_map

  path     = each.value.workspace_path
  source   = each.value.source_path
  language = each.value.extension == "sql" ? "SQL" : "PYTHON"

  depends_on = [databricks_directory.project_root]
}

