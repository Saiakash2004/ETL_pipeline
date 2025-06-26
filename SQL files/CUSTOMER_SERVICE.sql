-- =====================================================================
-- Database and Schema Setup
-- =====================================================================
CREATE DATABASE IF NOT EXISTS "CUSTOMER360";
CREATE SCHEMA IF NOT EXISTS "CUSTOMER360"."BRONZE_LAYER";
CREATE SCHEMA IF NOT EXISTS "CUSTOMER360"."SILVER_LAYER";

USE DATABASE "CUSTOMER360";
USE SCHEMA "CUSTOMER360"."BRONZE_LAYER";

-- =====================================================================
-- Bronze Layer: Raw Support Tickets Table
-- =====================================================================
CREATE OR REPLACE TABLE "CUSTOMER360"."BRONZE_LAYER"."CUSTOMER_SUPPORT_RAW" (
    TICKET_ID             TEXT COMMENT 'Unique ticket identifier',
    CUSTOMER_ID           TEXT COMMENT 'Customer identifier',
    CREATED_AT            DATETIME COMMENT 'Ticket creation timestamp',
    RESOLVED_AT           DATETIME COMMENT 'Ticket resolution timestamp',
    ISSUE_TYPE            TEXT COMMENT 'Type of issue reported',
    SATISFACTION_SCORE    NUMBER(3,1) COMMENT 'Customer satisfaction score (e.g., 4.5)',
    INGESTED_AT           TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record ingestion timestamp',
    UPDATED_AT            TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record update timestamp',
    CREATED_BY            STRING DEFAULT CURRENT_USER() COMMENT 'User who created the record',
    UPDATED_BY            STRING DEFAULT CURRENT_USER() COMMENT 'User who last updated the record'
)
COMMENT = 'Raw customer support ticket data ingested from Parquet files';

-- Create stream for change data capture
CREATE OR REPLACE STREAM "CUSTOMER360"."BRONZE_LAYER"."CUSTOMER_SUPPORT_RAW_STM"
    ON TABLE "CUSTOMER360"."BRONZE_LAYER"."CUSTOMER_SUPPORT_RAW"
    APPEND_ONLY = TRUE
    COMMENT = 'Stream capturing new records in CUSTOMER_SUPPORT_RAW for incremental processing';

-- Create stage for Parquet files
CREATE OR REPLACE STAGE "CUSTOMER360"."BRONZE_LAYER"."BRONZE_STAGE_CUSTOMER_SUPPORT_RAW"
    URL = 'azure://bronzecontainer.blob.core.windows.net/bronze-dump/customer_support_raw'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'sp=rl&st=2025-06-16T20:47:23Z&se=2025-06-21T04:47:23Z&spr=https&sv=2024-11-04&sr=d&sig=Y7cTGPtGzL993rzEbcK%2BqlpLD3UgPFdiz13jC0i2a1o%3D&sdd=1')
    FILE_FORMAT = (TYPE = PARQUET)
    COMMENT = 'Azure stage for customer support raw Parquet files';

-- =====================================================================
-- Silver Layer: Cleaned Support Tickets Table
-- =====================================================================
USE SCHEMA "CUSTOMER360"."SILVER_LAYER";

CREATE OR REPLACE TABLE "CUSTOMER360"."SILVER_LAYER"."SUPPORT_TICKETS_SILVER" (
    TICKET_ID_SK           NUMBER AUTOINCREMENT(1,1) PRIMARY KEY COMMENT 'Surrogate key',
    TICKET_ID              STRING NOT NULL UNIQUE COMMENT 'Unique ticket identifier',
    CUSTOMER_ID            STRING NOT NULL COMMENT 'Customer identifier',
    CUSTOMER_ID_SK         NUMBER NOT NULL COMMENT 'Foreign key to CUSTOMER_MASTER_SILVER',
    CREATED_AT             DATETIME NOT NULL COMMENT 'Ticket creation timestamp',
    RESOLVED_AT            DATETIME NOT NULL COMMENT 'Ticket resolution timestamp',
    RESOLUTION_TIME_HOURS  NUMBER(10,2) NOT NULL COMMENT 'Resolution time in hours',
    ISSUE_TYPE             STRING NOT NULL COMMENT 'Type of issue reported',
    SATISFACTION_SCORE     INTEGER NOT NULL COMMENT 'Customer satisfaction score (integer)',
    SATISFACTION_CATEGORY  STRING NOT NULL COMMENT 'Satisfaction category (e.g., High, Low)',
    CREATED_TIMESTAMP      TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    UPDATED_TIMESTAMP      TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record update timestamp'
)
COMMENT = 'Cleaned and transformed customer support ticket data';

-- Create stream for downstream processing
CREATE OR REPLACE STREAM "CUSTOMER360"."SILVER_LAYER"."SUPPORT_TICKETS_SILVER_STM"
    ON TABLE "CUSTOMER360"."SILVER_LAYER"."SUPPORT_TICKETS_SILVER"
    APPEND_ONLY = TRUE
    COMMENT = 'Stream capturing changes in SUPPORT_TICKETS_SILVER for further processing';

-- =====================================================================
-- Logging Table for Merge Operations (Aligned with Customer Master)
-- =====================================================================
CREATE OR REPLACE TABLE "CUSTOMER360"."SILVER_LAYER"."MERGE_LOG" (
    EXECUTION_ID        NUMBER AUTOINCREMENT(1,1) PRIMARY KEY COMMENT 'Unique execution identifier',
    PROCEDURE_NAME      STRING NOT NULL COMMENT 'Name of the stored procedure',
    EXECUTION_START     TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Start time of execution',
    EXECUTION_END       TIMESTAMP COMMENT 'End time of execution',
    STATUS              STRING NOT NULL COMMENT 'Status of the execution (SUCCESS/ERROR)',
    ROWS_PROCESSED      NUMBER COMMENT 'Number of rows processed',
    ERROR_MESSAGE       STRING COMMENT 'Error message if execution failed'
)
COMMENT = 'Log table to track merge operations for auditing';

-- =====================================================================
-- Stored Procedure: Merge Bronze to Silver for Support Tickets
-- =====================================================================
CREATE OR REPLACE PROCEDURE "CUSTOMER360"."SILVER_LAYER"."MERGE_SUPPORT_TICKETS"()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS CALLER
    
AS $$
from snowflake.snowpark.functions import col, coalesce, lit, to_date, current_timestamp, datediff, when
from snowflake.snowpark.types import StringType, TimestampType, FloatType, IntegerType

def run(session):
    try:
        # Log execution start
        session.sql("""
            INSERT INTO CUSTOMER360.SILVER_LAYER.MERGE_LOG (PROCEDURE_NAME, STATUS, EXECUTION_START)
            VALUES ('MERGE_SUPPORT_TICKETS', 'RUNNING', CURRENT_TIMESTAMP())
        """).collect()
        execution_id = session.sql("SELECT LAST_INSERT_ID()").collect()[0][0]

        # Read from bronze stream
        support_df = session.table("CUSTOMER360.BRONZE_LAYER.CUSTOMER_SUPPORT_RAW_STM")
        
        # Skip if no new data
        if support_df.count() == 0:
            session.sql(f"""
                UPDATE CUSTOMER360.SILVER_LAYER.MERGE_LOG
                SET STATUS = 'SUCCESS', EXECUTION_END = CURRENT_TIMESTAMP(), ROWS_PROCESSED = 0
                WHERE EXECUTION_ID = {execution_id}
            """).collect()
            return "No new data in stream"
        
        # Join with CUSTOMER_MASTER_SILVER to get CUSTOMER_ID_SK
        customer_df = session.table("CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER")
        joined_df = support_df.join(
            customer_df,
            support_df.col("CUSTOMER_ID") == customer_df.col("CUSTOMER_ID"),
            "left"
        )
        
        # Transform columns, including stream metadata
        transformed_df = joined_df.select(
            col("TICKET_ID").cast(StringType()).alias("ticket_id"),
            coalesce(col("CUSTOMER_ID").cast(StringType()), lit("UNKNOWN")).alias("customer_id"),
            coalesce(col("CUSTOMER_ID_SK"), lit(0)).alias("customer_id_sk"),
            coalesce(to_date(col("CREATED_AT")), lit("1970-01-01")).alias("created_at"),
            coalesce(to_date(col("RESOLVED_AT")), lit("1970-01-01")).alias("resolved_at"),
            coalesce(
                datediff("hour", col("CREATED_AT"), col("RESOLVED_AT")) / 24.0,
                lit(0.0)
            ).cast(FloatType()).alias("resolution_time_hours"),
            coalesce(col("ISSUE_TYPE").cast(StringType()), lit("Unknown")).alias("issue_type"),
            coalesce(col("SATISFACTION_SCORE").cast(IntegerType()), lit(0)).alias("satisfaction_score"),
            when(col("SATISFACTION_SCORE") >= 4, lit("High"))
            .when(col("SATISFACTION_SCORE") >= 2, lit("Medium"))
            .otherwise(lit("Low")).alias("satisfaction_category"),
            col("INGESTED_AT").cast(TimestampType()).alias("created_timestamp"),
            col("UPDATED_AT").cast(TimestampType()).alias("updated_timestamp"),
            col("METADATA$ACTION").alias("metadata_action"),
            col("METADATA$ROW_ID").alias("metadata_row_id")
        ).filter(col("METADATA$ACTION") == "INSERT")  # Process only INSERTs for append-only stream
        
        # Create temp view with stream metadata
        transformed_df.create_or_replace_temp_view("support_source")
        
        # Execute merge, ensuring stream consumption
        merge_sql = """
        MERGE INTO CUSTOMER360.SILVER_LAYER.SUPPORT_TICKETS_SILVER AS target
        USING (
            SELECT 
                ticket_id,
                customer_id,
                customer_id_sk,
                created_at,
                resolved_at,
                resolution_time_hours,
                issue_type,
                satisfaction_score,
                satisfaction_category,
                created_timestamp,
                updated_timestamp,
                metadata_action,
                metadata_row_id
            FROM support_source
            WHERE metadata_action = 'INSERT'
        ) AS source
        ON target.TICKET_ID = source.ticket_id
        WHEN MATCHED AND source.metadata_action = 'INSERT' THEN UPDATE SET
            CUSTOMER_ID            = source.customer_id,
            CUSTOMER_ID_SK         = source.customer_id_sk,
            CREATED_AT             = source.created_at,
            RESOLVED_AT            = source.resolved_at,
            RESOLUTION_TIME_HOURS  = source.resolution_time_hours,
            ISSUE_TYPE             = source.issue_type,
            SATISFACTION_SCORE     = source.satisfaction_score,
            SATISFACTION_CATEGORY  = source.satisfaction_category,
            UPDATED_TIMESTAMP      = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED AND source.metadata_action = 'INSERT' THEN INSERT (
            TICKET_ID, CUSTOMER_ID, CUSTOMER_ID_SK, CREATED_AT, RESOLVED_AT,
            RESOLUTION_TIME_HOURS, ISSUE_TYPE, SATISFACTION_SCORE, SATISFACTION_CATEGORY,
            CREATED_TIMESTAMP, UPDATED_TIMESTAMP
        )
        VALUES (
            source.ticket_id, source.customer_id, source.customer_id_sk, source.created_at,
            source.resolved_at, source.resolution_time_hours, source.issue_type,
            source.satisfaction_score, source.satisfaction_category, source.created_timestamp,
            CURRENT_TIMESTAMP()
        )
        """
        session.sql(merge_sql).collect()
        
        # Log execution success
        rows_processed = transformed_df.count()
        session.sql(f"""
            UPDATE CUSTOMER360.SILVER_LAYER.MERGE_LOG
            SET STATUS = 'SUCCESS', EXECUTION_END = CURRENT_TIMESTAMP(), ROWS_PROCESSED = {rows_processed}
            WHERE EXECUTION_ID = {execution_id}
        """).collect()
        
        return "Merge completed successfully"
    except Exception as e:
        # Log execution failure
        session.sql(f"""
            UPDATE CUSTOMER360.SILVER_LAYER.MERGE_LOG
            SET STATUS = 'ERROR', EXECUTION_END = CURRENT_TIMESTAMP(), ERROR_MESSAGE = '{str(e).replace("'", "''")}'
            WHERE EXECUTION_ID = {execution_id}
        """).collect()
        return f"Error: {str(e)}"
$$;

-- =====================================================================
-- Task: Load Parquet Files into Bronze Table
-- =====================================================================
CREATE OR REPLACE TASK "CUSTOMER360"."BRONZE_LAYER"."LOAD_CUSTOMER_SUPPORT_RAW"
    WAREHOUSE = CUSTOMER360
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Loads new Parquet files into CUSTOMER_SUPPORT_RAW every minute'
    AS
    COPY INTO "CUSTOMER360"."BRONZE_LAYER"."CUSTOMER_SUPPORT_RAW" (
        TICKET_ID, CUSTOMER_ID, CREATED_AT, RESOLVED_AT, ISSUE_TYPE, SATISFACTION_SCORE
    )
    FROM (
        SELECT
            $1:ticket_id::TEXT,
            $1:customer_id::TEXT,
            $1:created_at::DATETIME,
            $1:resolved_at::DATETIME,
            $1:issue_type::TEXT,
            $1:satisfaction_score::NUMBER
        FROM @"CUSTOMER360"."BRONZE_LAYER"."BRONZE_STAGE_CUSTOMER_SUPPORT_RAW"
    )
    FILE_FORMAT = (TYPE = PARQUET)
    ON_ERROR = 'SKIP_FILE';

-- Grant execute permission at the account level (required in Snowflake, aligned with customer_master)
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ACCOUNTADMIN;

-- Enable the task (aligned with customer_master)
ALTER TASK "CUSTOMER360"."BRONZE_LAYER"."LOAD_CUSTOMER_SUPPORT_RAW" RESUME;

-- =====================================================================
-- Task: Merge Bronze to Silver (separate schedule, aligned with customer_master)
-- =====================================================================
CREATE OR REPLACE TASK "CUSTOMER360"."SILVER_LAYER"."MERGE_SUPPORT_TICKETS_TASK"
    WAREHOUSE = CUSTOMER360
    SCHEDULE = '2 MINUTE'
    COMMENT = 'Merges new support ticket data from bronze stream to silver table every 2 minutes'
    AS CALL "CUSTOMER360"."SILVER_LAYER"."MERGE_SUPPORT_TICKETS"();

-- (No need to grant EXECUTE TASK on individual tasks; already granted on ACCOUNT)

-- Enable the task (aligned with customer_master)
ALTER TASK "CUSTOMER360"."SILVER_LAYER"."MERGE_SUPPORT_TICKETS_TASK" RESUME;

ALTER WAREHOUSE compute_wh SET AUTO_SUSPEND = 10;  -- suspend after 60 seconds of inactivity (aligned with customer_master)

-- Verify the bronze table data (aligned with customer_master)
SELECT * FROM CUSTOMER360.BRONZE_LAYER.CUSTOMER_SUPPORT_RAW;

select * from customer360.silver_layer.customer_master_silver;

call 

