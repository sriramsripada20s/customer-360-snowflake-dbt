-- dq_helpers.sql
-- Purpose : Reusable data quality macros used across
--           models, tests and monitoring

-- -------------------------------------------------------
-- safe_divide
-- Null-safe division — returns 0 on division by zero
-- Usage: {{ safe_divide('numerator_col', 'denominator_col') }}
-- -------------------------------------------------------
{% macro safe_divide(numerator, denominator) %}
    CASE
        WHEN {{ denominator }} = 0
          OR {{ denominator }} IS NULL
        THEN 0
        ELSE {{ numerator }}::FLOAT / {{ denominator }}
    END
{% endmacro %}


-- -------------------------------------------------------
-- min_max_norm
-- Min-max normalization expression
-- Usage: {{ min_max_norm('value_col', 'min_val', 'max_val') }}
-- Usage inverted: {{ min_max_norm('value_col', 'min_val', 'max_val', invert=true) }}
-- -------------------------------------------------------
{% macro min_max_norm(value_col, min_val, max_val, invert=false) %}
    {% if invert %}
        CASE
            WHEN ({{ max_val }} - {{ min_val }}) > 0
            THEN 1.0 - (
                ({{ value_col }} - {{ min_val }})::FLOAT
                / ({{ max_val }} - {{ min_val }})
            )
            ELSE 0
        END
    {% else %}
        CASE
            WHEN ({{ max_val }} - {{ min_val }}) > 0
            THEN ({{ value_col }} - {{ min_val }})::FLOAT
                 / ({{ max_val }} - {{ min_val }})
            ELSE 0
        END
    {% endif %}
{% endmacro %}


-- -------------------------------------------------------
-- rolling_window_filter
-- Standard WHERE clause for rolling time windows
-- Usage: WHERE {{ rolling_window_filter('transaction_date', 30) }}
-- -------------------------------------------------------
{% macro rolling_window_filter(date_col, days) %}
    {{ date_col }} >= DATEADD('day', -{{ days }}, CURRENT_TIMESTAMP())
{% endmacro %}


-- -------------------------------------------------------
-- log_pipeline_run
-- Writes a pipeline run record to MONITORING.PIPELINE_AUDIT
-- Usage: {{ log_pipeline_run('STAGING', 'stg_customers', 'SUCCESS', 50000) }}
-- -------------------------------------------------------
{% macro log_pipeline_run(layer, step, status, rows_loaded=0) %}
    INSERT INTO CUSTOMER_PLATFORM.MONITORING.PIPELINE_AUDIT (
        PIPELINE_NAME,
        LAYER,
        STEP,
        STATUS,
        ROWS_LOADED,
        STARTED_AT,
        COMPLETED_AT,
        CREATED_AT
    )
    VALUES (
        'CUSTOMER_PLATFORM_DBT',
        '{{ layer }}',
        '{{ step }}',
        '{{ status }}',
        {{ rows_loaded }},
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    )
{% endmacro %}


-- -------------------------------------------------------
-- log_dq_failure
-- Writes a DQ failure record to MONITORING.PIPELINE_AUDIT
-- Called when a custom test fails
-- Usage: {{ log_dq_failure('dim_customers', 'unique_customer_id', 5) }}
-- -------------------------------------------------------
{% macro log_dq_failure(model_name, test_name, failure_count) %}
    INSERT INTO CUSTOMER_PLATFORM.MONITORING.PIPELINE_AUDIT (
        PIPELINE_NAME,
        LAYER,
        STEP,
        STATUS,
        ROWS_FAILED,
        ERROR_MESSAGE,
        CREATED_AT
    )
    VALUES (
        '{{ model_name }}',
        'DQ',
        '{{ test_name }}',
        'FAILED',
        {{ failure_count }},
        '{{ test_name }} failed with {{ failure_count }} rows',
        CURRENT_TIMESTAMP()
    )
{% endmacro %}


-- -------------------------------------------------------
-- is_incremental_filter
-- Standardized incremental WHERE clause
-- Usage: {{ is_incremental_filter('_loaded_at') }}
-- -------------------------------------------------------
{% macro is_incremental_filter(date_col) %}
    {% if is_incremental() %}
        AND {{ date_col }} > (
            SELECT MAX({{ date_col }})
            FROM {{ this }}
        )
    {% endif %}
{% endmacro %}


-- -------------------------------------------------------
-- cents_to_dollars
-- Converts integer cents to decimal dollars
-- Usage: {{ cents_to_dollars('amount_cents') }}
-- -------------------------------------------------------
{% macro cents_to_dollars(col) %}
    ROUND({{ col }}::FLOAT / 100, 2)
{% endmacro %}