-- Adds a fourth log source (payments) for the onboarding test in task 12.
-- Run it in the Azure Portal Query editor after 01 and 02.
-- Warning: running it again truncates the table and inserts the rows with a new
-- ingestion_date, so the pipeline would load them a second time.

IF OBJECT_ID('dbo.app_logs_payments', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.app_logs_payments (
        log_id          INT             NULL,
        event_time      VARCHAR(128)    NOT NULL,
        payment_id      INT             NULL,
        order_id        INT             NULL,
        payer_email     VARCHAR(256)    NULL,
        payment_method  VARCHAR(32)     NOT NULL,
        amount          DECIMAL(12, 2)  NULL,
        service_name    VARCHAR(64)     NOT NULL,
        ingestion_date  DATETIME2       NOT NULL
    );
END;

TRUNCATE TABLE dbo.app_logs_payments;

INSERT INTO dbo.app_logs_payments
    (log_id, event_time, payment_id, order_id, payer_email, payment_method, amount, service_name, ingestion_date)
VALUES
    (4001, '2026-11-17 12:00:00', 8001, 9001, 'customer1@example.com', 'CARD', 250.00, 'payments-service', SYSUTCDATETIME()),
    (4002, '2026-11-17T12:01:00Z', 8002, 9003, 'customer3@example.com', 'BLIK', 125.50, 'payments-service', SYSUTCDATETIME()),
    (4003, '2026-11-17 12:02:00', 8003, 9004, 'customer4@example.com', 'CRYPTO', 80.00, 'payments-service', SYSUTCDATETIME()),     -- payment method not allowed
    (4004, '2026-11-17 12:03:00', 8004, 9005, 'customer5@example.com', 'TRANSFER', -49.99, 'payments-service', SYSUTCDATETIME()), -- negative amount
    (4005, '2026-11-17 12:04:00', 8005, 9006, 'customer6_at_example.com', 'CARD', 99.90, 'payments-service', SYSUTCDATETIME());   -- invalid e-mail