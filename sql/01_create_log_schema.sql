-- Creates the deterministic application log schema for the "Databricks - Log processor" challenge.
--
-- Run this in the Azure Portal Query editor, in Azure Cloud Shell, or from any SQL client that
-- can reach the server. There are deliberately no GO batch separators, so the whole file can be
-- pasted and executed in one go.
--
-- Azure SQL Database does not support "CREATE TABLE IF NOT EXISTS", so the objects are guarded
-- with OBJECT_ID checks to keep the script rerunnable.

IF OBJECT_ID('dbo.app_logs_web', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.app_logs_web (
        log_id          INT             NULL,
        event_time      VARCHAR(128)    NOT NULL,
        user_id         INT             NULL,
        user_email      VARCHAR(256)    NULL,
        session_id      VARCHAR(64)     NULL,
        request_path    VARCHAR(256)    NOT NULL,
        response_status INT             NOT NULL,
        duration_ms     INT             NULL,
        service_name    VARCHAR(64)     NOT NULL,
        ingestion_date  DATETIME2       NOT NULL
    );
END;

IF OBJECT_ID('dbo.app_logs_api', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.app_logs_api (
        log_id          INT             NULL,
        event_time      VARCHAR(128)    NOT NULL,
        api_client      VARCHAR(128)    NULL,
        client_email    VARCHAR(256)    NULL,
        endpoint        VARCHAR(256)    NOT NULL,
        http_method     VARCHAR(16)     NOT NULL,
        response_code   INT             NOT NULL,
        payload_size    INT             NULL,
        service_name    VARCHAR(64)     NOT NULL,
        ingestion_date  DATETIME2       NOT NULL
    );
END;

IF OBJECT_ID('dbo.app_logs_orders', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.app_logs_orders (
        log_id          INT             NULL,
        event_time      VARCHAR(128)    NOT NULL,
        order_id        INT             NULL,
        customer_id     INT             NULL,
        customer_email  VARCHAR(256)    NULL,
        order_status    VARCHAR(64)     NOT NULL,
        amount          DECIMAL(12, 2)  NULL,
        service_name    VARCHAR(64)     NOT NULL,
        ingestion_date  DATETIME2       NOT NULL
    );
END;

-- Indexes on ingestion_date support the incremental watermark queries built in task 05.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_app_logs_web_ingestion_date')
    CREATE INDEX ix_app_logs_web_ingestion_date ON dbo.app_logs_web (ingestion_date);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_app_logs_api_ingestion_date')
    CREATE INDEX ix_app_logs_api_ingestion_date ON dbo.app_logs_api (ingestion_date);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_app_logs_orders_ingestion_date')
    CREATE INDEX ix_app_logs_orders_ingestion_date ON dbo.app_logs_orders (ingestion_date);

