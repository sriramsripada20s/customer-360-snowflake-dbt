-- ============================================================
-- 02_storage_integration.sql
-- Run as: ACCOUNTADMIN
-- Run when ready to connect S3
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_CUSTOMER_PLATFORM_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/snowflake-customer-platform-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_BUCKET_NAME>/customer-platform/')
    COMMENT                   = 'Secure S3 integration for Customer Platform';

-- Run this and copy values to AWS IAM trust policy
DESC INTEGRATION S3_CUSTOMER_PLATFORM_INTEGRATION;

GRANT USAGE ON INTEGRATION S3_CUSTOMER_PLATFORM_INTEGRATION
    TO ROLE INGEST_ROLE;