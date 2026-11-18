# Validation (Task 12)

End-to-end validation of the log processor against the deterministic dataset from task 02.
All runs were made on 17 and 18 November 2026 in the `dev` target.

- Job: [`[dev] log-processor`](https://adb-7405607547860282.2.azuredatabricks.net/jobs/826100013534574)
- Screenshots: [`docs/screenshots/`](screenshots/)

## Summary

| # | Acceptance criterion | Result | Evidence |
|---|---|---|---|
| 1 | A full pipeline run from an empty state loads all three sources | Passed | [Scenario 1](#scenario-1---full-run-from-an-empty-state) |
| 2 | A second run without new source rows ingests zero rows | Passed | [Scenario 2](#scenario-2---second-run-without-new-rows) |
| 3 | Adding a new row in Azure SQL results in exactly one new row downstream | Passed | [Scenario 3](#scenario-3---one-new-row) |
| 4 | Every intentionally corrupted record appears in a quarantine table with the correct rule names | Passed | [Scenario 1](#quarantine) |
| 5 | A row for the failing run appears in `logs.monitoring.quality_check_runs` with `threshold_breached = true` | Passed | [Scenario 1](#monitoring) |
| 6 | Raising the threshold makes the job succeed, lowering it makes it fail | Passed | [Scenario 4](#scenario-4---failure-threshold) |
| 7 | A new log source can be onboarded through configuration only | Passed | [Scenario 5](#scenario-5---onboarding-a-fourth-source) |
| 8 | Unit tests run locally and in CI | Passed | [Unit tests](#unit-tests) |

## Scenario 0 - Reset to a clean state

The pipeline keeps its state in several places, so all of them were cleared before the first run.

1. Dropped all 11 pipeline tables (listed first from `logs.information_schema.tables`):
   `bronze._ingestion_state`, `bronze.logs_*`, `silver.logs_*`, `quarantine.logs_*`,
   `monitoring.quality_check_runs`. The schemas were kept.
2. Deleted the raw files and the Auto Loader state with `dbutils.fs.rm(path, True)`:
   `raw/api/`, `raw/orders/`, `raw/web/`, `processed/checkpoints/`, `processed/schemas/`.
   The folder `processed/unity/` (managed location of the catalog) was kept.
3. Reloaded the source data with `sql/02_insert_deterministic_test_data.sql`
   (it truncates the tables first): 6 + 6 + 6 = 18 rows.

Evidence: `00_reset_tables_empty.png`, `00_reset_files_empty.png`.

The first attempt of the reset was overwritten by a scheduled run, see [Incident](#incident---hourly-schedule-active-in-dev).

## Scenario 1 - Full run from an empty state

Run: [844331485767484](https://adb-7405607547860282.2.azuredatabricks.net/jobs/826100013534574/runs/844331485767484)

| Step | Expected | Actual |
|---|---|---|
| Ingestion (web / api / orders) | 6 / 6 / 6 new rows | 6 / 6 / 6 |
| Bronze (web / api / orders) | 6 / 6 / 6 rows | 6 / 6 / 6 |
| Quality web (passed / failed / ratio) | 1 / 5 / 83% | 1 / 5 / 83% |
| Quality api | 2 / 4 / 67% | 2 / 4 / 67% |
| Quality orders | 3 / 3 / 50% | 3 / 3 / 50% |
| Job result | failed: ratio above 20% | `RuntimeError: Failure ratio above 20% for: web, api, orders` |

The `quality_checks` task fails on purpose. It writes silver, quarantine and monitoring first and raises the error
at the end, so a failing run still leaves a full trail.

Evidence: `01_full_run_graph.png`, `01_full_run_ingestion.png`, `01_full_run_autoloader.png`, `01_full_run_quality.png`.

### Quarantine

12 rows (5 + 4 + 3), one for every failed row. Every corrupted record from task 02 is there with the rule it breaks.

| Source | log_id | Defect in the data | `failed_rules` |
|---|---|---|---|
| web | 1001 (2nd) | duplicate log_id | `unique:log_id` |
| web | 1002 | missing user_id, invalid e-mail | `not_null:log_id,user_id`, `regex:user_email` |
| web | 1003 | impossible month | `timestamp:event_time` |
| web | 1004 | missing user_id | `not_null:log_id,user_id` |
| web | 1005 | negative duration | `range:duration_ms` |
| api | 2002 | invalid e-mail | `regex:client_email` |
| api | 2002 (2nd) | duplicate log_id, missing client | `not_null:log_id,api_client`, `unique:log_id` |
| api | 2004 | invalid timestamp | `timestamp:event_time` |
| api | 2005 | HTTP method `FETCH` | `allowed_values:http_method` |
| orders | 3001 (2nd) | duplicate log_id | `unique:log_id` |
| orders | 3002 | missing ids, invalid e-mail | `not_null:log_id,order_id,customer_id`, `regex:customer_email` |
| orders | 3005 | negative amount | `range:amount` |

Rows with only a different timestamp format (api 2003, orders 3003 and 3004) are parsed correctly and are not
quarantined. Orders 3004 has a NULL `amount`; `amount` is not a business-critical column, so the row passes.

Evidence: `01_quarantine.png`.

### Monitoring

The run wrote 16 rows to `logs.monitoring.quality_check_runs`, one per rule and source (5 + 6 + 5),
all with `threshold_breached = true`.

Note: `threshold_breached` is the decision for the whole source, so it is also `true` for rules with zero failures.

Evidence: `01_monitoring.png`.

## Scenario 2 - Second run without new rows

Run: [535284575040660](https://adb-7405607547860282.2.azuredatabricks.net/jobs/826100013534574/runs/535284575040660)

| Step | Expected | Actual |
|---|---|---|
| Ingestion (web / api / orders) | 0 / 0 / 0 new rows | 0 / 0 / 0 |
| Bronze (web / api / orders) | 6 / 6 / 6 rows (unchanged) | 6 / 6 / 6 |
| Quality | same as scenario 1 | same as scenario 1 |

The watermarks stop the ingestion from loading the same rows again, and the Auto Loader checkpoints stop
the same files from being loaded again.

Evidence: `02_second_run_graph.png`, `02_second_run_ingestion.png`, `02_second_run_autoloader.png`, `02_second_run_quality.png`.

## Scenario 3 - One new row

One valid order was inserted in Azure SQL:

```sql
INSERT INTO dbo.app_logs_orders
    (log_id, event_time, order_id, customer_id, customer_email, order_status, amount, service_name, ingestion_date)
VALUES
    (3006, '2026-11-17 10:30:00', 9006, 7006, 'customer6@example.com', 'completed', 99.90, 'orders-service', SYSUTCDATETIME());
```

Run: [1103936042342973](https://adb-7405607547860282.2.azuredatabricks.net/jobs/826100013534574/runs/1103936042342973)

| Layer (orders) | Before | Expected after | Actual after |
|---|---|---|---|
| Ingestion | - | 1 new row (web and api 0) | 1 (web 0, api 0) |
| Bronze | 6 | 7 | 7 |
| Silver | 3 | 4 | 4 |
| Quarantine | 3 | 3 | 3 |
| Quality ratio | 50% | 43% | 43% |

Silver contains row 3006, loaded from its own raw file (`raw/orders/20261117_093632.parquet`).

`SYSUTCDATETIME()` stores 7 fractional digits. The next runs ingested 0 orders rows, which confirms the
timestamp precision fix (see the deviations file).

Evidence: `03_new_row_graph.png`, `03_new_row_ingestion.png`, `03_new_row_autoloader.png`, `03_new_row_quality.png`,
`03_counts.png`, `03_silver_orders.png`.

## Scenario 4 - Failure threshold

`quality.failure_threshold` in `src/config.yaml` was changed, deployed with `databricks bundle deploy -t dev` and the job
was run. The ratios were web 83%, api 67%, orders 43%.

| Threshold | Run | web | api | orders | Job |
|---|---|---|---|---|---|
| 0.2 (default) | [1103936042342973](https://adb-7405607547860282.2.azuredatabricks.net/jobs/826100013534574/runs/1103936042342973) | breached | breached | breached | failed |
| 0.9 (raised) | [563005966030285](https://adb-7405607547860282.2.azuredatabricks.net/jobs/826100013534574/runs/563005966030285) | ok | ok | ok | **succeeded** |
| 0.5 (lowered) | [224555068909920](https://adb-7405607547860282.2.azuredatabricks.net/jobs/826100013534574/runs/224555068909920) | breached | breached | ok | failed: `Failure ratio above 50% for: web, api` |

The threshold works per source. The monitoring table stores the threshold of every run, so each result can be
explained later. After the test the threshold was set back to 0.2.

Evidence: `04_threshold_090_*.png`, `04_threshold_050_*.png`, `04_threshold_monitoring.png`.

## Scenario 5 - Onboarding a fourth source

A `payments` source was added without changing any Python module:

1. `sql/03_add_payments_source.sql` creates `dbo.app_logs_payments` with 5 rows (3 of them corrupted on purpose).
2. A new entry in the `sources` list of `src/config.yaml` with six rules
   (`not_null`, `unique`, `regex`, `range`, `allowed_values`, `timestamp`).

`git status` before the run shows only `databricks.yml`, `src/config.yaml`, the new SQL script and the screenshots:
no `.py` file.

Run: [946521894734354](https://adb-7405607547860282.2.azuredatabricks.net/jobs/826100013534574/runs/946521894734354)

| Step | Expected | Actual |
|---|---|---|
| Ingestion payments (other sources) | 5 (0) | 5 (0) |
| Bronze payments | 5 rows | 5 |
| Quality payments | 2 / 3 / 60% | 2 / 3 / 60% |
| New tables | `bronze`, `silver`, `quarantine` `.logs_payments` (14 tables in total) | 14 tables |

Quarantine for payments: 4003 `allowed_values:payment_method` (`CRYPTO`), 4004 `range:amount`,
4005 `regex:payer_email`.

Evidence: `05_git_status_no_python.png`, `05_payments_graph.png`, `05_payments_ingestion.png`, `05_payments_autoloader.png`,
`05_payments_quality.png`, `05_payments_tables.png`, `05_payments_quarantine.png`.

## Unit tests

| Where | Result | Evidence |
|---|---|---|
| Local (Python 3.11.9, pytest 9.2.0) | 9 passed | `06_pytest_local.png` |
| CI (`ci.yml` on the pull request of this task) | 9 passed | [CI run](https://github.com/barweg23/dbx-log-processor/actions/runs/44349550969/job/134961094169?pr=2) |

The test `test_every_source_has_the_required_keys` also checks the new `payments` entry.

## CI/CD runs (Task 10)

| Workflow | Result | Link |
|---|---|---|
| `ci.yml` (pull request) | succeeded | [CI run](https://github.com/barweg23/dbx-log-processor/actions/runs/42784622612) |
| `deploy.yml` (push to main, approved in the `prod` environment) | succeeded | [Deploy run](https://github.com/barweg23/dbx-log-processor/actions/runs/42784931385) |

## Incident - hourly schedule active in dev

**What happened.** The job ran every hour in `dev` ("By scheduler") since the first dev deployment in task 09.
It was found because the monitoring table had 5 quality runs while only 3 runs had been started by hand.

**Root cause.** `databricks.yml` set `pause_status: UNPAUSED` explicitly. `mode: development` pauses schedules only when
the value is not set, so the explicit value kept the schedule running.

**Impact.**
- Extra cost: a job cluster every hour, and the serverless SQL database woke up every hour.
- The first reset attempt was overwritten: the 09:00 scheduled run started after the tables were dropped but before the
  source data was reloaded, so it loaded the old data again. The reset was repeated.
- The monitoring table contains these extra runs.
- Every failed run sent an e-mail notification that was not noticed.

**Fix.** The schedule is now `PAUSED` for all targets in `databricks.yml` (one place), deployed to `dev`
and checked in the job settings.

**Lesson.** Check deployed settings in the UI instead of trusting the documented default, and set a budget alert in
Azure Cost Management from the first day.

Evidence: `incident_scheduled_runs.png`, `incident_monitoring_runs.png`, `incident_schedule_paused.png`.

## Deviations and known limitations

The full list is in [`deviations_and_limitations.md`](deviations_and_limitations.md). The ones that matter most for this
validation:

- `dev` and `prod` share the same catalog and storage, so the validation was done only in `dev`.
  The `prod` job is deployed by the CD workflow but not run.
- When duplicates come in the same file, the row kept by the `unique` rule is not deterministic
  (same `_ingested_at` and `_source_file`). It did not change the results here, because both api 2002 rows are quarantined.
- An ingestion task can take more than 10 minutes when the serverless SQL database has to wake up (auto-pause).
- Running `sql/03_add_payments_source.sql` again loads the payments rows a second time (new `ingestion_date`).
