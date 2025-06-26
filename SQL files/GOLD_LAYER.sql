USE SCHEMA CUSTOMER360.GOLD_LAYER;


-- 1.1 Date Dimension
CREATE OR REPLACE TABLE dim_date (
    date_id_sk     NUMBER AUTOINCREMENT PRIMARY KEY,
    dates          DATE,
    months         INTEGER,
    month_name     STRING,
    quarters       INTEGER,
    years          INTEGER,
    days           STRING
);

-- 1.2 Customer Dimension
CREATE OR REPLACE TABLE dim_customers (
    customer_sk        NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id        STRING,
    first_name         STRING,
    last_name          STRING,
    full_name          STRING,
    email              STRING,
    phone              STRING,
    city               STRING,
    state_code         STRING,
    state_name         STRING,
    signup_date_sk     NUMBER REFERENCES dim_date(date_id_sk)
);

-- 1.3 Support Dimension
CREATE OR REPLACE TABLE dim_support (
    ticket_sk              NUMBER PRIMARY KEY,
    ticket_id              STRING,
    customer_id_sk         NUMBER,
    created_date_sk        NUMBER REFERENCES dim_date(date_id_sk),
    issue_type             STRING,
    resolution_time_days   NUMBER,
    resolved_date_sk       NUMBER REFERENCES dim_date(date_id_sk),
    satisfaction_category  STRING,
    satisfaction_score     NUMBER
);

-- 1.4 Session Dimension
CREATE OR REPLACE TABLE dim_sessions (
    session_sk           NUMBER PRIMARY KEY,
    session_id           STRING,
    customer_id_sk       NUMBER,
    session_duration     NUMBER,
    page_views           NUMBER,
    interaction_date_sk  NUMBER REFERENCES dim_date(date_id_sk),
    device_type          STRING,
    session_category     STRING
);

-- ================================================================================
-- 2. FACT TABLE
-- ================================================================================

CREATE OR REPLACE TABLE fact_transactions (
    transaction_id_sk   NUMBER AUTOINCREMENT PRIMARY KEY,
    transaction_id      STRING,
    customer_id_sk      NUMBER REFERENCES dim_customers(customer_sk),
    support_id_sk       NUMBER DEFAULT -1 REFERENCES dim_support(ticket_sk),
    session_id_sk       NUMBER DEFAULT -1 REFERENCES dim_sessions(session_sk),
    transaction_date    NUMBER REFERENCES dim_date(date_id_sk),
    amount              NUMBER,
    payment_method      STRING,
    city                STRING,
    state               STRING,
    state_code          STRING
);

-- =====================================================
-- 1. STORED PROCEDURES
-- =====================================================

-- 1.1 Date Dimension
CREATE OR REPLACE PROCEDURE sp_populate_dim_date()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_second INTEGER;
BEGIN
    -- Get the current second
    SELECT SECOND(CURRENT_TIMESTAMP()) INTO current_second;

    -- Only proceed if the current second is 0 or 30 (approximate 30-second intervals)
    IF (current_second NOT IN (0, 30)) THEN
        RETURN 'Skipping execution at second ' || current_second || '; only runs at 0 or 30 seconds';
    END IF;

    INSERT INTO dim_date (dates, months, month_name, quarters, years, days)
    SELECT DISTINCT
        CAST(date_val AS DATE),
        EXTRACT(MONTH FROM date_val),
        TO_CHAR(date_val, 'Mon'),
        EXTRACT(QUARTER FROM date_val),
        EXTRACT(YEAR FROM date_val),
        DAYNAME(date_val)
    FROM (
        SELECT transaction_date AS date_val FROM CUSTOMER360.SILVER_LAYER.TRANSACTIONS_SILVER_STM WHERE METADATA$ACTION = 'INSERT'
        UNION
        SELECT signup_date FROM CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER_STM WHERE METADATA$ACTION = 'INSERT'
        UNION
        SELECT created_at FROM CUSTOMER360.SILVER_LAYER.SUPPORT_TICKETS_SILVER_STM WHERE METADATA$ACTION = 'INSERT'
        UNION
        SELECT resolved_at FROM CUSTOMER360.SILVER_LAYER.SUPPORT_TICKETS_SILVER_STM WHERE METADATA$ACTION = 'INSERT'
        UNION
        SELECT interaction_date FROM CUSTOMER360.SILVER_LAYER.WEB_INTERACTIONS_SILVER_STM WHERE METADATA$ACTION = 'INSERT'
    ) combined_dates
    WHERE date_val IS NOT NULL;

    RETURN 'Date dimension populated successfully at second ' || current_second;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error in sp_populate_dim_date: ' || SQLERRM;
END;
$$;

-- 1.2 Customer Dimension
CREATE OR REPLACE PROCEDURE sp_populate_dim_customers()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_second INTEGER;
BEGIN
    -- Get the current second
    SELECT SECOND(CURRENT_TIMESTAMP()) INTO current_second;

    -- Only proceed if the current second is 0 or 30 (approximate 30-second intervals)
    IF (current_second NOT IN (0, 30)) THEN
        RETURN 'Skipping execution at second ' || current_second || '; only runs at 0 or 30 seconds';
    END IF;

    INSERT INTO dim_customers (
        customer_id, first_name, last_name, full_name, email,
        phone, city, state_code, state_name, signup_date_sk
    )
    SELECT DISTINCT
        cms.customer_id, cms.first_name, cms.last_name,
        CONCAT(cms.first_name, ' ', cms.last_name),
        cms.email, cms.phone, cms.city, cms.state_code, cms.state_name,
        dd.date_id_sk
    FROM CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER_STM cms
    JOIN dim_date dd ON cms.signup_date = dd.dates
    WHERE METADATA$ACTION = 'INSERT';

    RETURN 'Customer dimension populated successfully at second ' || current_second;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error in sp_populate_dim_customers: ' || SQLERRM;
END;
$$;

-- 1.3 Support Dimension
CREATE OR REPLACE PROCEDURE sp_populate_dim_support()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_second INTEGER;
BEGIN
    -- Get the current second
    SELECT SECOND(CURRENT_TIMESTAMP()) INTO current_second;

    -- Only proceed if the current second is 0 or 30 (approximate 30-second intervals)
    IF (current_second NOT IN (0, 30)) THEN
        RETURN 'Skipping execution at second ' || current_second || '; only runs at 0 or 30 seconds';
    END IF;

    INSERT INTO dim_support (
        ticket_sk, ticket_id, customer_id_sk, created_date_sk,
        issue_type, resolution_time_days, resolved_date_sk,
        satisfaction_category, satisfaction_score
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY sts.ticket_id),
        sts.ticket_id, dc.customer_sk, dd_created.date_id_sk,
        sts.issue_type, sts.resolution_time_hours,
        dd_resolved.date_id_sk, sts.satisfaction_category, sts.satisfaction_score
    FROM CUSTOMER360.SILVER_LAYER.SUPPORT_TICKETS_SILVER_STM sts
    JOIN CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER cms ON sts.customer_id_sk = cms.customer_id_sk
    JOIN dim_customers dc ON dc.customer_id = cms.customer_id
    JOIN dim_date dd_created ON sts.created_at = dd_created.dates
    JOIN dim_date dd_resolved ON sts.resolved_at = dd_resolved.dates
    WHERE METADATA$ACTION = 'INSERT';

    RETURN 'Support dimension populated successfully at second ' || current_second;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error in sp_populate_dim_support: ' || SQLERRM;
END;
$$;

-- 1.4 Session Dimension
CREATE OR REPLACE PROCEDURE sp_populate_dim_sessions()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_second INTEGER;
BEGIN
    -- Get the current second
    SELECT SECOND(CURRENT_TIMESTAMP()) INTO current_second;

    -- Only proceed if the current second is 0 or 30 (approximate 30-second intervals)
    IF (current_second NOT IN (0, 30)) THEN
        RETURN 'Skipping execution at second ' || current_second || '; only runs at 0 or 30 seconds';
    END IF;

    INSERT INTO dim_sessions (
        session_sk, session_id, customer_id_sk, session_duration,
        page_views, interaction_date_sk, device_type, session_category
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY wis.session_id),
        wis.session_id, dc.customer_sk, wis.session_duration,
        wis.page_views, dd.date_id_sk, wis.device, wis.session_category
    FROM CUSTOMER360.SILVER_LAYER.WEB_INTERACTIONS_SILVER_STM wis
    JOIN CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER cms ON wis.customer_id_sk = cms.customer_id_sk
    JOIN dim_customers dc ON dc.customer_id = cms.customer_id
    JOIN dim_date dd ON wis.interaction_date = dd.dates
    WHERE METADATA$ACTION = 'INSERT';

    RETURN 'Session dimension populated successfully at second ' || current_second;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error in sp_populate_dim_sessions: ' || SQLERRM;
END;
$$;

-- 1.5 Transaction Fact Table
CREATE OR REPLACE PROCEDURE sp_populate_fact_transactions()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_second INTEGER;
BEGIN
    -- Get the current second
    SELECT SECOND(CURRENT_TIMESTAMP()) INTO current_second;

    -- Only proceed if the current second is 0 or 30 (approximate 30-second intervals)
    IF (current_second NOT IN (0, 30)) THEN
        RETURN 'Skipping execution at second ' || current_second || '; only runs at 0 or 30 seconds';
    END IF;

    INSERT INTO fact_transactions (
        transaction_id, customer_id_sk, transaction_date, amount,
        payment_method, city, state, state_code
    )
    SELECT 
        tss.transaction_id, dc.customer_sk, dd.date_id_sk, tss.amount,
        tss.payment_method, tss.city, tss.state, tss.state_code
    FROM CUSTOMER360.SILVER_LAYER.TRANSACTIONS_SILVER_STM tss
    JOIN CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER cms ON tss.customer_id = cms.customer_id
    JOIN dim_customers dc ON dc.customer_id = cms.customer_id
    JOIN dim_date dd ON tss.transaction_date = dd.dates
    WHERE METADATA$ACTION = 'INSERT';

    RETURN 'Transaction fact table populated successfully at second ' || current_second;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error in sp_populate_fact_transactions: ' || SQLERRM;
END;
$$;

-- =====================================================
-- 2. TASKS (Independent Triggers)
-- =====================================================

CREATE OR REPLACE TASK task_dim_date
  WAREHOUSE = CUSTOMER360
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER_STM')
    OR SYSTEM$STREAM_HAS_DATA('CUSTOMER360.SILVER_LAYER.WEB_INTERACTIONS_SILVER_STM')
    OR SYSTEM$STREAM_HAS_DATA('CUSTOMER360.SILVER_LAYER.TRANSACTIONS_SILVER_STM')
    OR SYSTEM$STREAM_HAS_DATA('CUSTOMER360.SILVER_LAYER.SUPPORT_TICKETS_SILVER_STM')
AS
  CALL sp_populate_dim_date();

CREATE OR REPLACE TASK task_dim_customers
  WAREHOUSE = CUSTOMER360
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER_STM')
AS
  CALL sp_populate_dim_customers();

CREATE OR REPLACE TASK task_dim_support
  WAREHOUSE = CUSTOMER360
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER360.SILVER_LAYER.SUPPORT_TICKETS_SILVER_STM')
AS
  CALL sp_populate_dim_support();

CREATE OR REPLACE TASK task_dim_sessions
  WAREHOUSE = CUSTOMER360
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER360.SILVER_LAYER.WEB_INTERACTIONS_SILVER_STM')
AS
  CALL sp_populate_dim_sessions();

CREATE OR REPLACE TASK task_fact_transactions
  WAREHOUSE = CUSTOMER360
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER360.SILVER_LAYER.TRANSACTIONS_SILVER_STM')
AS
  CALL sp_populate_fact_transactions();

-- =====================================================
-- 3. ACTIVATE TASKS
-- =====================================================

ALTER TASK task_dim_date RESUME;
ALTER TASK task_dim_customers RESUME;
ALTER TASK task_dim_support RESUME;
ALTER TASK task_dim_sessions RESUME;
ALTER TASK task_fact_transactions RESUME;

-- ALTER TASK CUSTOMER360.GOLD_LAYER.TASK_ROOT_GOLD suspend;

-- =====================================================
-- 4. VERIFICATION QUERIES
-- =====================================================

-- Check task status
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
WHERE NAME IN ('task_dim_date', 'task_dim_customers', 'task_dim_support', 'task_dim_sessions', 'task_fact_transactions', 'TASK_ROOT_GOLD');

-- Check if the stored procedures executed at the correct seconds
SELECT *
FROM dim_date
LIMIT 10;

SELECT *
FROM dim_customers
LIMIT 10;

SELECT *
FROM dim_support
LIMIT 10;

SELECT *
FROM dim_sessions
LIMIT 10;

SELECT *
FROM fact_transactions
LIMIT 10;