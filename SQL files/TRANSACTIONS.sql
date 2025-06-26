-- ============================
-- Database and Schema Setup
-- ============================
USE DATABASE CUSTOMER360;
USE SCHEMA BRONZE_LAYER;

-- ============================
-- Bronze Layer: Raw Table
-- ============================
CREATE OR REPLACE TABLE CUSTOMER360.BRONZE_LAYER.TRANSACTIONS_RAW (
    TRANSACTION_ID   STRING,
    CUSTOMER_ID      STRING,
    TRANSACTION_DATE STRING,
    AMOUNT           STRING,
    PAYMENT_METHOD   STRING,
    STORE_LOCATION   STRING,
    INGESTED_AT      TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY       STRING DEFAULT CURRENT_USER(),
    UPDATED_BY       STRING DEFAULT CURRENT_USER()
) COMMENT = 'Raw transaction data from Parquet files';

CREATE OR REPLACE STREAM CUSTOMER360.BRONZE_LAYER.TRANSACTIONS_RAW_STM
    ON TABLE CUSTOMER360.BRONZE_LAYER.TRANSACTIONS_RAW
    APPEND_ONLY = TRUE;

CREATE OR REPLACE FILE FORMAT CUSTOMER360.BRONZE_LAYER.PARQUET_FORMAT
    TYPE = PARQUET;

-- Create stage for Parquet files
CREATE OR REPLACE STAGE "CUSTOMER360"."BRONZE_LAYER"."BRONZE_STAGE_TRANSACTIONS_RAW"
    URL = 'azure://bronzecontainer.blob.core.windows.net/bronze-dump/transactions_raw'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'sp=rl&st=2025-06-17T04:48:02Z&se=2025-06-21T12:48:02Z&spr=https&sv=2024-11-04&sr=d&sig=FRxWLQb3u0FDQmx7GvtHMBFpymN03484q9baghMAXZk%3D&sdd=1')
    FILE_FORMAT = (FORMAT_NAME = "CUSTOMER360"."BRONZE_LAYER"."PARQUET_FORMAT")
    COMMENT = 'Azure stage for transaction raw Parquet files';

-- ============================
-- Silver Layer Setup
-- ============================
USE SCHEMA CUSTOMER360.SILVER_LAYER;

CREATE OR REPLACE TABLE CUSTOMER360.SILVER_LAYER.TRANSACTION_SILVER (
    TRANSACTION_ID_SK  NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    TRANSACTION_ID     STRING(6) NOT NULL UNIQUE,
    CUSTOMER_ID        STRING NOT NULL,
    CUSTOMER_SK        NUMBER NOT NULL,
    TRANSACTION_DATE   DATETIME NOT NULL,
    AMOUNT             FLOAT NOT NULL,
    PAYMENT_METHOD     STRING NOT NULL,
    CITY               STRING NOT NULL,
    STATE              STRING NOT NULL,
    STATE_CODE         STRING(2) NOT NULL,
    CREATED_TIMESTAMP  DATETIME DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP  DATETIME DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM CUSTOMER360.SILVER_LAYER.TRANSACTIONS_SILVER_STM
    ON TABLE CUSTOMER360.SILVER_LAYER.TRANSACTION_SILVER
    APPEND_ONLY = TRUE;

-- ============================
-- Merge Procedure
-- ============================
CREATE OR REPLACE PROCEDURE CUSTOMER360.SILVER_LAYER.MERGE_TRANSACTIONS()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS CALLER
AS $$
from snowflake.snowpark.functions import col, substring, trim, upper, split, regexp_replace, to_timestamp, coalesce
from snowflake.snowpark.types import StringType, FloatType

def run(session):
    try:
        # Read from bronze stream
        df = session.table("CUSTOMER360.BRONZE_LAYER.TRANSACTIONS_RAW_STM")

        # Filter only INSERT actions (valid for append-only stream)
        df = df.filter(col("METADATA$ACTION") == "INSERT")

        if df.count() == 0:
            return "No new data in stream"

        # Clean and transform
        df = df.select(
            substring(trim(col('TRANSACTION_ID')), 1, 6).cast(StringType()).alias('transaction_id'),
            trim(col('CUSTOMER_ID')).cast(StringType()).alias('customer_id'),
            to_timestamp(
                regexp_replace(
                    regexp_replace(col("TRANSACTION_DATE").cast(StringType()), r'^[^0-9]+', ''),
                    r'\.\d{3,9}', ''
                )
            ).alias('transaction_date'),
            col('AMOUNT').cast(FloatType()).alias('amount'),
            trim(col('PAYMENT_METHOD')).cast(StringType()).alias('payment_method'),
            trim(col('STORE_LOCATION')).cast(StringType()).alias('store_location')
        )

        # Create temp view
        df.create_or_replace_temp_view('transactions_source')

        # Merge statement
        merge_sql = """
        MERGE INTO CUSTOMER360.SILVER_LAYER.TRANSACTION_SILVER AS target
        USING (
            SELECT 
                SUBSTRING(TRIM(raw.transaction_id), 1, 6) AS transaction_id,
                TRIM(raw.customer_id) AS customer_id,
                COALESCE(cust.CUSTOMER_ID_SK, -1) AS customer_sk,
                raw.transaction_date,
                COALESCE(raw.amount, 0.0) AS amount,
                TRIM(raw.payment_method) AS payment_method,
                TRIM(SPLIT_PART(raw.store_location, ',', 1)) AS city,
                CASE 
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'NY' THEN 'New York'
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'WA' THEN 'Washington'
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'CA' THEN 'California'
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'AZ' THEN 'Arizona'
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'FL' THEN 'Florida'
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'CO' THEN 'Colorado'
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'IL' THEN 'Illinois'
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'GA' THEN 'Georgia'
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'TX' THEN 'Texas'
                    WHEN TRIM(SPLIT_PART(raw.store_location, ',', 2)) = 'MA' THEN 'Massachusetts'
                    ELSE 'Unknown'
                END AS state,
                UPPER(COALESCE(NULLIF(TRIM(SPLIT_PART(raw.store_location, ',', 2)), ''), 'NA')) AS state_code
            FROM transactions_source raw
            LEFT JOIN CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER cust
                ON TRIM(raw.customer_id) = TRIM(cust.customer_id)
            WHERE raw.transaction_id IS NOT NULL
              AND raw.customer_id IS NOT NULL
              AND raw.transaction_date IS NOT NULL
              AND raw.amount IS NOT NULL
              AND raw.payment_method IS NOT NULL
              AND TRIM(SPLIT_PART(raw.store_location, ',', 1)) IS NOT NULL
              AND TRIM(SPLIT_PART(raw.store_location, ',', 2)) IS NOT NULL
        ) AS source
        ON target.TRANSACTION_ID = source.transaction_id
        WHEN MATCHED AND (
            target.CUSTOMER_ID != source.customer_id OR
            target.CUSTOMER_SK != source.customer_sk OR
            target.TRANSACTION_DATE != source.transaction_date OR
            target.AMOUNT != source.amount OR
            target.PAYMENT_METHOD != source.payment_method OR
            target.CITY != source.city OR
            target.STATE != source.state OR
            target.STATE_CODE != source.state_code
        )
        THEN UPDATE SET
            CUSTOMER_ID = source.customer_id,
            CUSTOMER_SK = source.customer_sk,
            TRANSACTION_DATE = source.transaction_date,
            AMOUNT = source.amount,
            PAYMENT_METHOD = source.payment_method,
            CITY = source.city,
            STATE = source.state,
            STATE_CODE = source.state_code,
            UPDATED_TIMESTAMP = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            TRANSACTION_ID, CUSTOMER_ID, CUSTOMER_SK, TRANSACTION_DATE, AMOUNT,
            PAYMENT_METHOD, CITY, STATE, STATE_CODE,
            CREATED_TIMESTAMP, UPDATED_TIMESTAMP
        ) VALUES (
            source.transaction_id, source.customer_id, source.customer_sk, source.transaction_date,
            source.amount, source.payment_method, source.city, source.state, source.state_code,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
        """
        session.sql(merge_sql).collect()
        return "Merge completed successfully"

    except Exception as e:
        return f"Error: {str(e)}"
$$;


-- ============================
-- Tasks
-- ============================
CREATE OR REPLACE TASK CUSTOMER360.BRONZE_LAYER.LOAD_TRANSACTIONS_RAW
    WAREHOUSE = CUSTOMER360
    SCHEDULE = '1 MINUTE'
    AS
    COPY INTO CUSTOMER360.BRONZE_LAYER.TRANSACTIONS_RAW (
        TRANSACTION_ID, CUSTOMER_ID, TRANSACTION_DATE, AMOUNT, PAYMENT_METHOD, STORE_LOCATION
    )
    FROM (
        SELECT
            $1:transaction_id::STRING,
            $1:customer_id::STRING,
            $1:transaction_date::STRING,
            $1:amount::STRING,
            $1:payment_method::STRING,
            $1:store_location::STRING
        FROM @CUSTOMER360.BRONZE_LAYER.BRONZE_STAGE_TRANSACTIONS_RAW
    )
    FILE_FORMAT = (FORMAT_NAME = CUSTOMER360.BRONZE_LAYER.PARQUET_FORMAT)
    ON_ERROR = 'SKIP_FILE';

ALTER TASK CUSTOMER360.BRONZE_LAYER.LOAD_TRANSACTIONS_RAW RESUME;

CREATE OR REPLACE TASK CUSTOMER360.SILVER_LAYER.MERGE_TRANSACTIONS_TASK
    WAREHOUSE = CUSTOMER360
    SCHEDULE = '1 MINUTE'
    AS CALL CUSTOMER360.SILVER_LAYER.MERGE_TRANSACTIONS();

ALTER TASK CUSTOMER360.SILVER_LAYER.MERGE_TRANSACTIONS_TASK RESUME;

-- ============================
-- Optional: Verify Pipeline
-- ============================
SELECT * FROM CUSTOMER360.BRONZE_LAYER.TRANSACTIONS_RAW;
SELECT * FROM CUSTOMER360.BRONZE_LAYER.TRANSACTIONS_RAW_STM;
SELECT * FROM CUSTOMER360.SILVER_LAYER.TRANSACTION_SILVER;
EXECUTE TASK CUSTOMER360.SILVER_LAYER.MERGE_TRANSACTIONS_TASK;
SELECT * FROM CUSTOMER360.SILVER_LAYER.TRANSACTION_SILVER;




