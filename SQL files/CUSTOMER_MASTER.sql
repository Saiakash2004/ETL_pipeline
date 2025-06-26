-- =====================================================================
-- Database and Schema Setup
-- =====================================================================
USE DATABASE CUSTOMER360;
USE SCHEMA BRONZE_LAYER;

-- =====================================================================
-- Bronze Layer: Raw Customer Table
-- =====================================================================
CREATE OR REPLACE TABLE CUSTOMER360.BRONZE_LAYER.CUSTOMER_MASTER_RAW (
    CUSTOMER_ID    TEXT,
    NAME           TEXT,
    EMAIL          TEXT,
    PHONE          TEXT,
    SIGNUP_DATE    DATETIME,
    CITY           TEXT 
    STATE_CODE     TEXT 
    INGESTED_AT    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    UPDATED_AT     TIMESTAMP DEFAULT CURRENT_TIMESTAMP() 
    CREATED_BY     STRING DEFAULT CURRENT_USER()
    UPDATED_BY     STRING DEFAULT CURRENT_USER() 
COMMENT = 'Raw customer data ingested from Parquet files';

-- Create stream for change data capture
CREATE OR REPLACE STREAM CUSTOMER360.BRONZE_LAYER.CUSTOMER_MASTER_RAW_STM
    ON TABLE CUSTOMER360.BRONZE_LAYER.CUSTOMER_MASTER_RAW
    APPEND_ONLY = TRUE

-- Create stage for Parquet files
CREATE OR REPLACE STAGE CUSTOMER360.BRONZE_LAYER.BRONZE_STAGE_CUSTOMER_MASTER_RAW
    URL = 'azure://bronzecontainer.blob.core.windows.net/bronze-dump/customer_master_raw'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'sp=rl&st=2025-06-16T18:31:59Z&se=2025-06-21T02:31:59Z&sv=2024-11-04&sr=d&sig=iK0zNtPC5b%2FEM7DxA1dHdBaV0gqwaocMyWx6UmDzf3Y%3D&sdd=1')
    FILE_FORMAT = (TYPE = PARQUET)


-- =====================================================================
-- Silver Layer: Cleaned Customer Table
-- =====================================================================
USE SCHEMA CUSTOMER360.SILVER_LAYER;

CREATE OR REPLACE TABLE CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER (
    CUSTOMER_ID_SK NUMBER AUTOINCREMENT(1,1) PRIMARY KEY COMMENT 'Surrogate key',
    CUSTOMER_ID    STRING(5) NOT NULL UNIQUE COMMENT 'Unique customer identifier',
    FIRST_NAME     STRING NOT NULL COMMENT 'Customer first name',
    LAST_NAME      STRING NOT NULL COMMENT 'Customer last name',
    EMAIL          STRING NOT NULL COMMENT 'Customer email address',
    PHONE          STRING NOT NULL COMMENT 'Customer phone number',
    CITY           STRING NOT NULL COMMENT 'Customer city',
    STATE_CODE     STRING(2) NOT NULL COMMENT 'Two-letter state code',
    STATE_NAME     STRING NOT NULL COMMENT 'Full state name',
    SIGNUP_DATE    DATETIME NOT NULL COMMENT 'Customer signup date',
    CREATED_AT     TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    UPDATED_AT     TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record update timestamp'
)
COMMENT = 'Cleaned and transformed customer data';

-- Create stream for downstream processing
CREATE OR REPLACE STREAM CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER_STM
    ON TABLE CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER
    APPEND_ONLY = TRUE


-- =====================================================================
-- Stored Procedure: Merge Bronze to Silver
-- =====================================================================
CREATE OR REPLACE PROCEDURE CUSTOMER360.SILVER_LAYER.MERGE_CUSTOMER_MASTER()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS CALLER
AS $$
from snowflake.snowpark.functions import col, split, when, lit, coalesce, to_date, current_timestamp
from snowflake.snowpark.types import StringType, TimestampType

def run(session):
    try:
        # Read from bronze stream
        customer_df = session.table("CUSTOMER360.BRONZE_LAYER.CUSTOMER_MASTER_RAW_STM")
        
        # Skip if no new data
        if customer_df.count() == 0:
            return "No new data in stream"
        
        # Transform columns
        customer_transformed = customer_df.select(
            col("CUSTOMER_ID").cast(StringType()).alias("customer_id"),
            coalesce(split(col("NAME"), lit(" "))[0], lit("Unknown")).alias("first_name"),
            coalesce(split(col("NAME"), lit(" "))[1], lit("Unknown")).alias("last_name"),
            coalesce(col("EMAIL").cast(StringType()), lit("unknown@example.com")).alias("email"),
            coalesce(col("PHONE").cast(StringType()), lit("0000000000")).alias("phone"),
            to_date(col("SIGNUP_DATE")).alias("signup_date"),
            coalesce(col("CITY").cast(StringType()), lit("Unknown")).alias("city"),
            coalesce(col("STATE_CODE").cast(StringType()), lit("XX")).alias("state_code"),
            when(col("STATE_CODE") == "NY", lit("New York"))
            .when(col("STATE_CODE") == "MA", lit("Massachusetts"))
            .when(col("STATE_CODE") == "CA", lit("California"))
            .when(col("STATE_CODE") == "AZ", lit("Arizona"))
            .when(col("STATE_CODE") == "FL", lit("Florida"))
            .when(col("STATE_CODE") == "CO", lit("Colorado"))
            .when(col("STATE_CODE") == "IL", lit("Illinois"))
            .when(col("STATE_CODE") == "GA", lit("Georgia"))
            .when(col("STATE_CODE") == "TX", lit("Texas"))
            .otherwise(lit("Unknown")).alias("state_name"),
            col("INGESTED_AT").cast(TimestampType()).alias("created_at"),
            col("UPDATED_AT").cast(TimestampType()).alias("updated_at")
        )
        
        # Create temp view
        customer_transformed.create_or_replace_temp_view("customer_source")
        
        # Execute merge
        merge_sql = """
        MERGE INTO CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER AS target
        USING customer_source AS source
        ON target.CUSTOMER_ID = source.CUSTOMER_ID
        WHEN MATCHED THEN UPDATE SET
            FIRST_NAME  = source.FIRST_NAME,
            LAST_NAME   = source.LAST_NAME,
            EMAIL       = source.EMAIL,
            PHONE       = source.PHONE,
            CITY        = source.CITY,
            STATE_CODE  = source.STATE_CODE,
            STATE_NAME  = source.STATE_NAME,
            SIGNUP_DATE = source.SIGNUP_DATE,
            UPDATED_AT  = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, CITY, STATE_CODE, 
            STATE_NAME, SIGNUP_DATE, CREATED_AT, UPDATED_AT
        )
        VALUES (
            source.CUSTOMER_ID, source.FIRST_NAME, source.LAST_NAME, source.EMAIL, 
            source.PHONE, source.CITY, source.STATE_CODE, source.STATE_NAME, 
            source.SIGNUP_DATE, source.CREATED_AT, CURRENT_TIMESTAMP()
        )
        """
        session.sql(merge_sql).collect()
        
        return "Merge completed successfully"
    except Exception as e:
        return f"Error: {str(e)}"
$$;

-- =====================================================================
-- Task: Load Parquet Files to Bronze Table
-- =====================================================================
CREATE OR REPLACE TASK CUSTOMER360.BRONZE_LAYER.LOAD_CUSTOMER_MASTER_RAW
    WAREHOUSE = CUSTOMER360
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Loads new Parquet files into CUSTOMER_MASTER_RAW every minute'
    AS
    COPY INTO CUSTOMER360.BRONZE_LAYER.CUSTOMER_MASTER_RAW (
        CUSTOMER_ID, NAME, EMAIL, PHONE, SIGNUP_DATE, CITY, STATE_CODE
    )
    FROM (
        SELECT
            $1:customer_id::TEXT,
            $1:name::TEXT,
            $1:email::TEXT,
            $1:phone::TEXT,
            $1:signup_date::DATETIME,
            $1:city::TEXT,
            $1:state_code::TEXT
        FROM @CUSTOMER360.BRONZE_LAYER.BRONZE_STAGE_CUSTOMER_MASTER_RAW
    )
    FILE_FORMAT = (TYPE = PARQUET)
    ON_ERROR = 'CONTINUE';

-- Grant execute permission at the account level (required in Snowflake)
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ACCOUNTADMIN;

-- Enable the task
ALTER TASK CUSTOMER360.BRONZE_LAYER.LOAD_CUSTOMER_MASTER_RAW RESUME;

-- =====================================================================
-- Task: Merge Bronze to Silver
-- =====================================================================
CREATE OR REPLACE TASK CUSTOMER360.SILVER_LAYER.MERGE_CUSTOMER_MASTER_TASK
    WAREHOUSE = CUSTOMER360
    SCHEDULE = '2 MINUTE'
    COMMENT = 'Merges new data from bronze stream to silver table every 2 minutes'
    AS CALL CUSTOMER360.SILVER_LAYER.MERGE_CUSTOMER_MASTER();



-- Enable the task
ALTER TASK CUSTOMER360.SILVER_LAYER.MERGE_CUSTOMER_MASTER_TASK RESUME;

-- ALTER WAREHOUSE CUSTOMER360 SET AUTO_SUSPEND = 60; 


call MERGE_CUSTOMER_MASTER()
