-- ============================================================
-- 03_stage_and_file_formats.sql
-- Run as: INGEST_ROLE
-- Run when ready to connect S3
-- ============================================================

USE ROLE INGEST_ROLE;
USE DATABASE CUSTOMER_PLATFORM;
USE SCHEMA RAW;
USE WAREHOUSE WH_INGEST;

-- File formats
CREATE FILE FORMAT IF NOT EXISTS FF_CSV_HEADER
    TYPE             = CSV
    FIELD_DELIMITER  = ','
    SKIP_HEADER      = 1
    NULL_IF          = ('NULL', 'null', 'N/A', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE       = TRUE
    DATE_FORMAT      = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS';

CREATE FILE FORMAT IF NOT EXISTS FF_JSON
    TYPE             = JSON
    STRIP_OUTER_ARRAY = TRUE
    NULL_IF          = ('null', '');

CREATE FILE FORMAT IF NOT EXISTS FF_PARQUET
    TYPE             = PARQUET
    SNAPPY_COMPRESSION = TRUE;

-- External stages
CREATE STAGE IF NOT EXISTS STG_S3_CUSTOMERS
    STORAGE_INTEGRATION = S3_CUSTOMER_PLATFORM_INTEGRATION
    URL                 = 's3://<YOUR_BUCKET>/customer-platform/customers/'
    FILE_FORMAT         = FF_CSV_HEADER;

CREATE STAGE IF NOT EXISTS STG_S3_ORDERS
    STORAGE_INTEGRATION = S3_CUSTOMER_PLATFORM_INTEGRATION
    URL                 = 's3://<YOUR_BUCKET>/customer-platform/orders/'
    FILE_FORMAT         = FF_CSV_HEADER;

CREATE STAGE IF NOT EXISTS STG_S3_TRANSACTIONS
    STORAGE_INTEGRATION = S3_CUSTOMER_PLATFORM_INTEGRATION
    URL                 = 's3://<YOUR_BUCKET>/customer-platform/transactions/'
    FILE_FORMAT         = FF_CSV_HEADER;

CREATE STAGE IF NOT EXISTS STG_S3_CLICKSTREAM
    STORAGE_INTEGRATION = S3_CUSTOMER_PLATFORM_INTEGRATION
    URL                 = 's3://<YOUR_BUCKET>/customer-platform/clickstream/'
    FILE_FORMAT         = FF_JSON;

CREATE STAGE IF NOT EXISTS STG_S3_CAMPAIGNS
    STORAGE_INTEGRATION = S3_CUSTOMER_PLATFORM_INTEGRATION
    URL                 = 's3://<YOUR_BUCKET>/customer-platform/campaigns/'
    FILE_FORMAT         = FF_CSV_HEADER;

CREATE STAGE IF NOT EXISTS STG_S3_PRODUCTS
    STORAGE_INTEGRATION = S3_CUSTOMER_PLATFORM_INTEGRATION
    URL                 = 's3://<YOUR_BUCKET>/customer-platform/products/'
    FILE_FORMAT         = FF_CSV_HEADER;

CREATE STAGE IF NOT EXISTS STG_S3_ORDER_ITEMS
    STORAGE_INTEGRATION = S3_CUSTOMER_PLATFORM_INTEGRATION
    URL                 = 's3://<YOUR_BUCKET>/customer-platform/order_items/'
    FILE_FORMAT         = FF_CSV_HEADER;