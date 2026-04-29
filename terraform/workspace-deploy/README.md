# Terraform: Deploy Databricks AWS Demos to a Workspace

This Terraform config deploys the project notebooks from this repository into a target Databricks workspace.

## What it deploys

- Workspace root directory (default: `/Shared/databricks-aws-demos`)
- All `.py` and `.sql` notebooks under:
  - `_resources`
  - `01. Ingestion`
  - `02. Processing`
  - `03. Serving`

The original folder structure is preserved so `%run` references continue to work.

## Prerequisites

- Terraform `>= 1.5`
- Databricks PAT for the target workspace
- Access to target workspace URL (for example `https://dbc-xxxx.cloud.databricks.com`)

## Quick start

1. Open this folder:

```powershell
cd terraform/workspace-deploy
```

2. Create variables file:

```powershell
Copy-Item terraform.tfvars.example terraform.tfvars
```

3. Edit `terraform.tfvars` and set your host/token.

4. Initialize and deploy:

```powershell
terraform init
terraform plan
terraform apply
```

5. Open Databricks workspace path from output `workspace_root` and run notebooks.

## Notes

- This deployment uploads notebooks directly from your local checkout.
- If you rename folders or move notebooks, run `terraform apply` again.
- To remove deployed assets from workspace:

```powershell
terraform destroy
```

