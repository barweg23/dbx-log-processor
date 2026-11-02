# Known data defects in the source dataset

Found while inspecting the seeded data in Azure SQL (task 02).

| Table | log_id | Column | Defect | Rule |
|---|---|---|---|---|
| app_logs_web | 1001 | log_id | duplicate (identical rows) | `unique` |
| app_logs_api | 2002 | log_id | duplicate (different row contents) | `unique` |
| app_logs_orders | 3001 | log_id | duplicate (identical rows) | `unique` |
| app_logs_web | 1002 | user_id | NULL in business-critical column | `not_null` |
| app_logs_web | 1004 | user_id | NULL in business-critical column | `not_null` |
| app_logs_api | 2002 | api_client | NULL in business-critical column (second of the duplicate rows) | `not_null` |
| app_logs_orders | 3002 | order_id, customer_id | NULL in business-critical columns | `not_null` |
| app_logs_web | 1002 | user_email | malformed e-mail (`bad-email`) | `regex` |
| app_logs_api | 2002 | client_email | malformed e-mail (`client-b@`, first of the duplicate rows) | `regex` |
| app_logs_orders | 3002 | customer_email | malformed e-mail (`cust2_at_example.com`) | `regex` |
| app_logs_web | 1003 | event_time | impossible date (`2026-13-01`, month 13) | `timestamp` |
| app_logs_api | 2004 | event_time | not a timestamp (`invalid-timestamp`) | `timestamp` |
| app_logs_web | 1005 | duration_ms | negative value (`-12`) | `range` |
| app_logs_orders | 3005 | amount | negative value (`-49.99`) | `range` |
| app_logs_api | 2005 | http_method | invalid HTTP method (`FETCH`) | `allowed_values` (or `regex`) |

## Business-critical columns

Columns that are nullable in the source schema but must not be NULL for a log record to be usable.
Columns declared `NOT NULL` in SQL are already enforced by the database, so they are not listed here.

| Table | Business-critical columns | Reason |
|---|---|---|
| app_logs_web | `log_id`, `user_id` | `log_id` is the record key (needed for duplicate detection); `user_id` identifies who made the request |
| app_logs_api | `log_id`, `api_client` | `log_id` is the record key; `api_client` identifies who called the API |
| app_logs_orders | `log_id`, `order_id`, `customer_id` | `log_id` is the record key; without `order_id` and `customer_id` the order cannot be traced |

Nullable but not critical: e-mail columns (the entity is already identified by its id),
`session_id`, `duration_ms`, `payload_size` (e.g. a `GET` request may send no payload)
and `amount` (e.g. a `failed` order may have no amount). Negative values are covered by separate range rules.

## Timestamp formats in `event_time`

All of these are valid and must be parsed; a value is a defect only if none of them matches.

| Format | Example | log_id |
|---|---|---|
| `yyyy-MM-dd HH:mm:ss` | `2026-08-01 09:00:00` | most rows |
| `MM/dd/yyyy hh:mm:ss a` | `08/01/2026 09:04:00 AM` | 1004 |
| `yyyy/MM/dd hh:mm:ss a` | `2026/08/01 09:02:30 PM` | 2003 |
| `yyyy-MM-dd'T'HH:mm:ss'Z'` (ISO 8601, UTC) | `2026-08-01T09:02:50Z` | 3003 |
| `dd-MMM-yyyy HH:mm:ss` | `01-Aug-2026 09:04:00` | 3004 |

Observation: row 2003 has `event_time` 21:02:30 but `ingestion_date` 09:05:00 on the same day,
so the event appears to happen after it was ingested. Not covered by the required rules; noted as a known limitation.

