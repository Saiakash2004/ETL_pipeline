-- ================================================================================
-- SECTION 1: DATABASE & SCHEMA CONTEXT
-- ================================================================================
USE DATABASE CUSTOMER360;
USE SCHEMA BRONZE_LAYER;

-- ================================================================================
-- SECTION 2: BRONZE LAYER - RAW WEB INTERACTIONS TABLE
-- ================================================================================
CREATE OR REPLACE TABLE BRONZE_LAYER.WEB_INTERACTIONS_RAW (
    SESSION_ID         STRING COMMENT 'Unique session identifier',
    CUSTOMER_ID        STRING COMMENT 'Customer identifier',
    PAGE_VIEWS         NUMBER COMMENT 'Number of page views in the session',
    SESSION_DURATION   NUMBER COMMENT 'Session duration in seconds',
    DEVICE             STRING COMMENT 'Device used for the session',
    INTERACTION_DATE   DATETIME COMMENT 'Date and time of the interaction',

    -- Audit Columns
    INGESTED_AT        TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record ingestion timestamp',
    UPDATED_AT         TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record update timestamp',
    CREATED_BY         STRING DEFAULT CURRENT_USER() COMMENT 'User who created the record',
    UPDATED_BY         STRING DEFAULT CURRENT_USER() COMMENT 'User who last updated the record'
)
COMMENT = 'Raw web interaction data ingested from Parquet files';-- ================================================================================
-- SECTION 1: DATABASE & SCHEMA CONTEXT
-- ================================================================================
USE DATABASE CUSTOMER360;
USE SCHEMA BRONZE_LAYER;

-- ================================================================================
-- SECTION 2: BRONZE LAYER - RAW WEB INTERACTIONS TABLE
-- ================================================================================
CREATE OR REPLACE TABLE BRONZE_LAYER.WEB_INTERACTIONS_RAW (
    SESSION_ID         STRING COMMENT 'Unique session identifier',
    CUSTOMER_ID        STRING COMMENT 'Customer identifier',
    PAGE_VIEWS         NUMBER COMMENT 'Number of page views in the session',
    SESSION_DURATION   NUMBER COMMENT 'Session duration in seconds',
    DEVICE             STRING COMMENT 'Device used for the session',
    INTERACTION_DATE   DATETIME COMMENT 'Date and time of the interaction',

    -- Audit Columns
    INGESTED_AT        TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record ingestion timestamp',
    UPDATED_AT         TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record update timestamp',
    CREATED_BY         STRING DEFAULT CURRENT_USER() COMMENT 'User who created the record',
    UPDATED_BY         STRING DEFAULT CURRENT_USER() COMMENT 'User who last updated the record'
)
COMMENT = 'Raw web interaction data ingested from Parquet files';

-- Create stream on bronze table
CREATE OR REPLACE STREAM BRONZE_LAYER.WEB_INTERACTIONS_RAW_STM
    ON TABLE BRONZE_LAYER.WEB_INTERACTIONS_RAW
    APPEND_ONLY = TRUE
    COMMENT = 'Stream capturing new records in WEB_INTERACTIONS_RAW for incremental processing';

-- ================================================================================
-- SECTION 3: FILE FORMAT & STAGE FOR INGESTION
-- ================================================================================
CREATE OR REPLACE FILE FORMAT BRONZE_LAYER.PARQUET_FORMAT
    TYPE = PARQUET
    COMMENT = 'File format for Parquet files';

CREATE OR REPLACE STAGE bronze_stage_web_interactions_raw
  URL = 'azure://bronzecontainer.blob.core.windows.net/bronze-dump/web_interactions_raw'
  CREDENTIALS = (AZURE_SAS_TOKEN = 'sp=rl&st=2025-06-17T05:13:59Z&se=2025-06-21T13:13:59Z&spr=https&sv=2024-11-04&sr=d&sig=TX7XEXFV6cNmp9rICIaXZqZLSaLbSnWf6UXhqx%2Bc0fQ%3D&sdd=1')
  FILE_FORMAT = (FORMAT_NAME =BRONZE_LAYER.PARQUET_FORMAT)
  COMMENT = 'Stage for web interactions raw data';

-- ================================================================================
-- SECTION 4: SILVER LAYER - CLEANSED WEB INTERACTIONS TABLE
-- ================================================================================
USE SCHEMA SILVER_LAYER;

CREATE OR REPLACE TABLE SILVER_LAYER.WEB_INTERACTIONS_SILVER (
    SESSION_ID_SK        NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY COMMENT 'Surrogate key',
    SESSION_ID           STRING UNIQUE NOT NULL COMMENT 'Unique session identifier',
    CUSTOMER_ID          STRING NOT NULL COMMENT 'Customer identifier',
    CUSTOMER_ID_SK       NUMBER NOT NULL COMMENT 'Foreign key to CUSTOMER_MASTER_SILVER',
    PAGE_VIEWS           NUMBER NOT NULL COMMENT 'Number of page views in the session',
    SESSION_DURATION     NUMBER NOT NULL COMMENT 'Session duration in seconds',
    DEVICE               STRING NOT NULL COMMENT 'Device used for the session',
    INTERACTION_DATE     DATETIME NOT NULL COMMENT 'Date and time of the interaction',
    SESSION_CATEGORY     STRING NOT NULL COMMENT 'Category of the session based on duration',

    -- Audit Columns
    CREATED_TIMESTAMP    TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    UPDATED_TIMESTAMP    TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record update timestamp'
)
COMMENT = 'Cleaned and transformed web interaction data';

-- Stream on silver table for downstream consumers
CREATE OR REPLACE STREAM SILVER_LAYER.WEB_INTERACTIONS_SILVER_STM
    ON TABLE SILVER_LAYER.WEB_INTERACTIONS_SILVER
    APPEND_ONLY = TRUE
    COMMENT = 'Stream capturing changes in WEB_INTERACTIONS_SILVER for further processing';

-- ================================================================================
-- SECTION 5: STORED PROCEDURE TO MERGE BRONZE → SILVER
-- ================================================================================
CREATE OR REPLACE PROCEDURE SILVER_LAYER.MERGE_WEB_INTERACTIONS()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark.functions import col, upper, initcap, coalesce
from snowflake.snowpark.types import StringType, IntegerType, TimestampType

def run(session):
    try:
        df_bronze_stream = session.table("CUSTOMER360.BRONZE_LAYER.WEB_INTERACTIONS_RAW_STM")

        if df_bronze_stream.count() == 0:
            return "No new data to process"

        df_clean = df_bronze_stream.select(
            col("SESSION_ID").cast(StringType()).alias("session_id"),
            col("CUSTOMER_ID").cast(StringType()).alias("customer_id"),
            col("PAGE_VIEWS").cast(IntegerType()).alias("page_views"),
            col("SESSION_DURATION").cast(IntegerType()).alias("session_duration"),
            col("DEVICE").cast(StringType()).alias("device"),
            col("INTERACTION_DATE").cast(TimestampType()).alias("interaction_date")
        ).filter(
            col("SESSION_ID").is_not_null() &
            col("CUSTOMER_ID").is_not_null() &
            col("PAGE_VIEWS").is_not_null() &
            col("SESSION_DURATION").is_not_null() &
            col("DEVICE").is_not_null() &
            col("INTERACTION_DATE").is_not_null()
        )

        df_clean.create_or_replace_temp_view("web_interactions_source")

        merge_sql = """
        MERGE INTO CUSTOMER360.SILVER_LAYER.WEB_INTERACTIONS_SILVER AS target
        USING (
            SELECT
                UPPER(src.session_id) AS session_id,
                UPPER(src.customer_id) AS customer_id,
                COALESCE(cust.CUSTOMER_ID_SK, -1) AS customer_id_sk,
                src.page_views,
                src.session_duration,
                INITCAP(src.device) AS device,
                src.interaction_date,
                CASE
                    WHEN src.session_duration BETWEEN 1 AND 10 THEN 'Short'
                    WHEN src.session_duration BETWEEN 11 AND 60 THEN 'Medium'
                    WHEN src.session_duration BETWEEN 61 AND 120 THEN 'Long'
                    WHEN src.session_duration BETWEEN 121 AND 180 THEN 'Very Long'
                    ELSE 'Unknown'
                END AS session_category
            FROM web_interactions_source src
            LEFT JOIN CUSTOMER360.SILVER_LAYER.CUSTOMER_MASTER_SILVER cust
              ON UPPER(src.customer_id) = UPPER(cust.CUSTOMER_ID)
        ) AS source
        ON target.SESSION_ID = source.session_id
        WHEN MATCHED THEN UPDATE SET
            CUSTOMER_ID = source.customer_id,
            CUSTOMER_ID_SK = source.customer_id_sk,
            PAGE_VIEWS = source.page_views,
            SESSION_DURATION = source.session_duration,
            DEVICE = source.device,
            INTERACTION_DATE = source.interaction_date,
            SESSION_CATEGORY = source.session_category,
            UPDATED_TIMESTAMP = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            SESSION_ID, CUSTOMER_ID, CUSTOMER_ID_SK,
            PAGE_VIEWS, SESSION_DURATION, DEVICE,
            INTERACTION_DATE, SESSION_CATEGORY,
            CREATED_TIMESTAMP, UPDATED_TIMESTAMP
        )
        VALUES (
            source.session_id, source.customer_id, source.customer_id_sk,
            source.page_views, source.session_duration, source.device,
            source.interaction_date, source.session_category,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
        """

        session.sql(merge_sql).collect()
        return "Merge completed successfully"
    except Exception as e:
        return f"Error: {str(e)}"
$$;

-- ================================================================================
-- SECTION 6: TASK TO LOAD PARQUET FILES INTO BRONZE TABLE
-- ================================================================================
CREATE OR REPLACE TASK BRONZE_LAYER.LOAD_WEB_INTERACTIONS_RAW
    WAREHOUSE = CUSTOMER360
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Loads new Parquet files into WEB_INTERACTIONS_RAW every minute'
AS
COPY INTO BRONZE_LAYER.WEB_INTERACTIONS_RAW (
    SESSION_ID, CUSTOMER_ID, PAGE_VIEWS, SESSION_DURATION, DEVICE, INTERACTION_DATE
)
FROM (
    SELECT
        $1:session_id::STRING,
        $1:customer_id::STRING,
        $1:page_views::NUMBER,
        $1:session_duration::NUMBER,
        $1:device::STRING,
        $1:interaction_date::DATETIME
    FROM @BRONZE_LAYER.BRONZE_STAGE_WEB_INTERACTIONS_RAW
)
FILE_FORMAT = (FORMAT_NAME = BRONZE_LAYER.PARQUET_FORMAT)
ON_ERROR = 'SKIP_FILE';

ALTER TASK BRONZE_LAYER.LOAD_WEB_INTERACTIONS_RAW RESUME;

-- ================================================================================
-- SECTION 7: TASK TO RUN MERGE BRONZE → SILVER PROCEDURE
-- ================================================================================
CREATE OR REPLACE TASK SILVER_LAYER.MERGE_WEB_INTERACTIONS_TASK
    WAREHOUSE = CUSTOMER360
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Merges new web interaction data from bronze to silver every 2 minutes'
AS
CALL SILVER_LAYER.MERGE_WEB_INTERACTIONS();

ALTER TASK SILVER_LAYER.MERGE_WEB_INTERACTIONS_TASK RESUME;

-- ================================================================================
-- SECTION 8: VALIDATION QUERIES
-- ================================================================================
SELECT * FROM BRONZE_LAYER.WEB_INTERACTIONS_RAW;
SELECT * FROM SILVER_LAYER.WEB_INTERACTIONS_SILVER;
-- CALL SILVER_LAYER.MERGE_WEB_INTERACTIONS();
