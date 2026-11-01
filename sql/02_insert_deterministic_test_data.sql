-- Deterministic training data for the "Databricks - Log processor" challenge.
--
-- Run this after sql/01_create_log_schema.sql, in the Azure Portal Query editor, in Azure Cloud
-- Shell, or from any SQL client that can reach the server. There are deliberately no GO batch
-- separators, so the whole file can be pasted and executed in one go.
--
-- The dataset intentionally contains duplicates, NULLs, malformed e-mails and mixed timestamp
-- formats so the data quality checks built in task 07 have something to catch.
-- The script is rerunnable: it truncates the tables before inserting.

TRUNCATE TABLE dbo.app_logs_web;
TRUNCATE TABLE dbo.app_logs_api;
TRUNCATE TABLE dbo.app_logs_orders;

INSERT INTO dbo.app_logs_web
    (log_id, event_time, user_id, user_email, session_id, request_path, response_status, duration_ms, service_name, ingestion_date)
VALUES
    (1001, '2026-08-01 09:00:00', 501, 'alice@example.com', 'session-001', '/home', 200, 85, 'web-service', '2026-08-01 09:05:00'),
    (1001, '2026-08-01 09:00:00', 501, 'alice@example.com', 'session-001', '/home', 200, 85, 'web-service', '2026-08-01 09:05:00'),      -- duplicate log_id
    (1002, '2026-08-01 09:02:10', NULL, 'bad-email', 'session-002', '/checkout', 500, 350, 'web-service', '2026-08-01 09:05:00'),        -- invalid e-mail + missing user_id
    (1003, '2026-13-01 09:03:00', 503, 'charlie@example.com', 'session-003', '/products', 200, 55, 'web-service', '2026-08-01 09:05:00'),-- impossible month
    (1004, '08/01/2026 09:04:00 AM', NULL, NULL, 'session-004', '/login', 302, 120, 'web-service', '2026-08-01 09:05:00'),               -- alternate timestamp format
    (1005, '2026-08-01 09:06:00', 505, 'eve@example.com', 'session-005', '/cart', 200, -12, 'web-service', '2026-08-01 09:10:00');       -- negative duration

INSERT INTO dbo.app_logs_api
    (log_id, event_time, api_client, client_email, endpoint, http_method, response_code, payload_size, service_name, ingestion_date)
VALUES
    (2001, '2026-08-01 09:00:10', 'client-a', 'client-a@example.com', '/api/v1/items', 'GET', 200, 512, 'api-service', '2026-08-01 09:05:00'),
    (2002, '2026-08-01 09:01:20', 'client-b', 'client-b@', '/api/v1/orders', 'POST', 201, 1024, 'api-service', '2026-08-01 09:05:00'),   -- invalid e-mail
    (2002, '2026-08-01 09:01:20', NULL, NULL, '/api/v1/orders', 'POST', 201, 1024, 'api-service', '2026-08-01 09:05:00'),                -- duplicate log_id + missing client
    (2003, '2026/08/01 09:02:30 PM', 'client-c', 'client-c@example.com', '/api/v1/users', 'GET', 404, NULL, 'api-service', '2026-08-01 09:05:00'),
    (2004, 'invalid-timestamp', 'client-d', 'client-d@example.com', '/api/v1/items', 'PUT', 500, 2048, 'api-service', '2026-08-01 09:05:00'),
    (2005, '2026-08-01 09:07:45', 'client-e', 'client-e@example.com', '/api/v1/items', 'FETCH', 200, 256, 'api-service', '2026-08-01 09:10:00'); -- invalid HTTP method

INSERT INTO dbo.app_logs_orders
    (log_id, event_time, order_id, customer_id, customer_email, order_status, amount, service_name, ingestion_date)
VALUES
    (3001, '2026-08-01 09:00:20', 9001, 7001, 'customer1@example.com', 'completed', 250.00, 'orders-service', '2026-08-01 09:05:00'),
    (3001, '2026-08-01 09:00:20', 9001, 7001, 'customer1@example.com', 'completed', 250.00, 'orders-service', '2026-08-01 09:05:00'),    -- duplicate log_id
    (3002, '2026-08-01 09:01:40', NULL, NULL, 'cust2_at_example.com', 'cancelled', 0.00, 'orders-service', '2026-08-01 09:05:00'),       -- missing ids + invalid e-mail
    (3003, '2026-08-01T09:02:50Z', 9003, 7003, 'customer3@example.com', 'processing', 125.50, 'orders-service', '2026-08-01 09:05:00'),  -- ISO 8601 with timezone
    (3004, '01-Aug-2026 09:04:00', 9004, 7004, 'customer4@example.com', 'failed', NULL, 'orders-service', '2026-08-01 09:05:00'),        -- alternate format + NULL amount
    (3005, '2026-08-01 09:08:15', 9005, 7005, 'customer5@example.com', 'delivered', -49.99, 'orders-service', '2026-08-01 09:10:00');    -- negative amount

