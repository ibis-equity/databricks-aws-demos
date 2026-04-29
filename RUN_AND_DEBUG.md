# Run and Debug Guide

Use this guide to run tests locally, debug in PyCharm, and run notebooks in Databricks.

## 1) Prerequisites

- Windows with PowerShell (or adapt commands for your shell)
- Python virtual environment at `.venv`
- Dev dependencies from `requirements-dev.txt`
- Terraform `>= 1.5.0`
- Databricks workspace URL and Personal Access Token (PAT)

## 2) Open the project

```powershell
Set-Location "C:\Users\desha\PycharmProjects\databricks-aws-demos"
```

## 3) Install local dependencies

```powershell
.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

## 4) Run tests locally

Run all unit tests:

```powershell
.venv\Scripts\python.exe -m pytest -v
```

Run one test file:

```powershell
.venv\Scripts\python.exe -m pytest tests\unit\test_01_setup_kinesis.py -v
```

Run one test function:

```powershell
.venv\Scripts\python.exe -m pytest tests\unit\test_01_setup_kinesis.py::test_generate_writes_300_records_to_kinesis_client -v -s
```

## 5) Debug tests in PyCharm

1. Open the Run/Debug configuration dropdown in PyCharm.
2. Select one of these saved configs:
   - `All Tests`
   - `test_00_basedata`
   - `test_01_config`
   - `test_01_setup_kinesis`
3. Set breakpoints in test files or helper code.
4. Click **Debug** (bug icon).
5. Use Step Over / Step Into and inspect values in Variables.

Config files are stored under `.idea/runConfigurations/`.

## 6) Deploy notebooks to Databricks with Terraform

### 6.1 Go to the Terraform folder

```powershell
Set-Location "C:\Users\desha\PycharmProjects\databricks-aws-demos\terraform\workspace-deploy"
```

### 6.2 Create `terraform.tfvars`

```powershell
Copy-Item terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set:

```hcl
databricks_host  = "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"
databricks_token = "<your_pat_here>"
workspace_root   = "/Shared/databricks-aws-demos"
```

### 6.3 Initialize and deploy

```powershell
terraform init
terraform plan
terraform apply
```

### 6.4 Open deployed notebooks

In the Databricks workspace UI, open:

- `/Shared/databricks-aws-demos/_resources`
- `/Shared/databricks-aws-demos/01. Ingestion`
- `/Shared/databricks-aws-demos/02. Processing`
- `/Shared/databricks-aws-demos/03. Serving`

## 7) Suggested notebook run order in Databricks

1. Run `_resources/00-setup`
2. Run `_resources/01-config`
3. Run `_resources/01-setup`
4. Run notebooks in `01. Ingestion`
5. Run `02. Processing/01. DLT-CDC-Pipeline`
6. Run `03. Serving/03.1 DynamoDB`

## 8) Common troubleshooting

- `Unresolved reference` in local IDE for `%run` symbols: expected in local static analysis; runtime in Databricks resolves `%run` dependencies.
- Terraform auth errors: verify `databricks_host` and `databricks_token` in `terraform.tfvars`.
- `terraform plan` shows path issues: run Terraform from `terraform/workspace-deploy` so `project_root = "../.."` resolves correctly.
- Tests fail due to missing packages: reinstall with `pip install -r requirements-dev.txt` inside `.venv`.

## 9) Cleanup

Remove deployed workspace assets:

```powershell
Set-Location "C:\Users\desha\PycharmProjects\databricks-aws-demos\terraform\workspace-deploy"
terraform destroy
```

