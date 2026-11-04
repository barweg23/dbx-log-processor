# Deviations and known limitations

Running notes collected during the challenge; they feed the limitations section of `docs/validation.md` (task 12).

## Deviations from the handbook

- **Task 01 – SQL region:** West Europe rejected new SQL servers (`RegionDoesNotAllowProvisioning`), so the
  provisioning script fell back to Sweden Central. Databricks and storage stay in West Europe.
- **Task 03 – Metastore:** Azure auto-provisioned the metastore `metastore_azure_westeurope` and assigned it to the
  workspace, so it was reused instead of being created in the account console.
- **Task 03 – Catalog location:** the auto-provisioned metastore has no root storage, so
  `CREATE CATALOG IF NOT EXISTS logs;` fails. The catalog was created with
  `MANAGED LOCATION 'abfss://processed@<storage>.dfs.core.windows.net/unity/logs'`.
- **Task 03 – Extra external location:** besides `ext_raw`, an `ext_processed` location was created; it backs the
  catalog managed location and the Auto Loader checkpoints (task 06).
- **Task 03 – Extra schema:** `logs.monitoring` was created up front for the tracking table in task 08.
- **Task 03 – Cluster node type:** the subscription has no vCPU quota for the Ddsv5 family in West Europe, so the
  cluster uses a node type from a family with available quota (see the cluster configuration).
  

## Known limitations

- **Cross-region traffic:** the SQL source (Sweden Central) and Databricks (West Europe) are in different regions;
  in production they should share a region to avoid latency and egress cost.
- **`AllowAzureServices` firewall rule:** allows any Azure service, including other tenants', to reach the SQL
  server; production should use a private endpoint.
- **File events:** the "File Events" checks on the external locations fail because the access connector has no
  Event Grid / storage queue roles. Not needed: Auto Loader runs in directory listing mode, which suits this volume.
- **Row 2003 (`app_logs_api`):** `event_time` is later than `ingestion_date`; no rule covers this.
- **At-least-once ingestion:** the raw file is written before the watermark is saved. If a run stops between the two
  steps (observed once: a run interrupted with Ctrl+C left `raw/web/20261104_181351.parquet` without a watermark),
  the next run loads the same rows again. Data is never lost; duplicates are caught by the `unique` rule (task 07).
  The duplicate folder was deleted by hand.
  - **Timestamp precision:** SQL Server `DATETIME2` keeps 7 fractional digits, Spark `TIMESTAMP` keeps 6, so the stored
  watermark could be slightly lower than the real value and the newest row was ingested again on every run
  (found with row 3006: `18:30:14.6187073` in SQL vs `18:30:14.618707` in Spark). Fix: the query reads rows
  `>= watermark + 1 microsecond`. Remaining risk: a row written within the same microsecond as the last loaded row,
  after the run, would be skipped. The two duplicate output folders in `raw/orders/` were deleted by hand.
