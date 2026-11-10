# Databricks Log Processor

A log processing platform on Azure Databricks. Logs from three applications (web, api, orders)
are read from Azure SQL Database, loaded into Unity Catalog with Auto Loader, and checked
by a configurable data quality framework.

## Pipeline

1. **Ingestion** (`src/ingestion/ingestion.py`) - incremental load from Azure SQL (watermark) to Parquet files in the `raw` container.
2. **Auto Loader** (`src/autoloader/autoloader.py`) - new Parquet files go to `bronze` tables.
3. **Quality checks** (`src/quality/checks.py`) - rules from `src/config.yaml`; good rows go to `silver`,
   bad rows go to `quarantine`, results are saved in `monitoring.quality_check_runs`.
   The job fails when more than 20% of the rows of a source fail.

The three steps run as one Databricks job defined in `databricks.yml` (Databricks Asset Bundle).

## Repository structure

| Path | Content |
|---|---|
| `src/` | pipeline code and `config.yaml` |
| `tests/` | unit tests (pytest) |
| `sql/` | scripts that create and fill the source database |
| `docs/` | data defects, deviations and known limitations |
| `databricks.yml` | job definition and the `dev` / `prod` targets |
| `.github/workflows/` | CI and CD workflows |
| `Create-AzureResources.ps1` | creates the Azure resources |

## Local setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
databricks auth login --host <workspace-url>
pytest
```

Create a `.env` file with `SQL_PASSWORD=<password>` to run the ingestion locally.
The file is in `.gitignore` and must never be committed.

## CI/CD (GitHub Actions)

| Workflow | Trigger | Steps |
|---|---|---|
| `ci.yml` | pull request to `main` | unit tests, `databricks bundle validate -t dev` |
| `deploy.yml` | push to `main` | `databricks bundle deploy -t prod` (needs approval) |

### Authentication

The workflows log in with **OIDC (workload identity federation)**, not with a personal access token
or a client secret. GitHub creates a short-lived token for every run, and Databricks accepts it
for the service principal `github-actions-dbx-logs` because of two federation policies:

| Policy subject ends with | Used by |
|---|---|
| `:environment:prod` | `deploy.yml` (job with `environment: prod`) |
| `:pull_request` | `ci.yml` |

### Required variables

Set them in **Settings > Secrets and variables > Actions > Variables**:

| Name | Value |
|---|---|
| `DATABRICKS_HOST` | workspace URL, for example `https://adb-xxxx.azuredatabricks.net` |
| `DATABRICKS_CLIENT_ID` | application ID (UUID) of the service principal |

### Required secrets

None. `DATABRICKS_CLIENT_SECRET` is not needed because OIDC replaces the client secret.

### Environment

The `prod` environment (**Settings > Environments**) has a required reviewer,
so every deployment waits for a manual approval.

### Prod schedule

The job schedule is paused in the `prod` target to save costs.
See `docs/deviations_and_limitations.md`.