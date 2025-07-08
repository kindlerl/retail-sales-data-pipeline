USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- SELECT CURRENT_ACCOUNT(), CURRENT_REGION();
-- SHOW WAREHOUSES;
-- ALTER WAREHOUSE COMPUTE_WH RESUME;

-- =========================================
-- ==  CREATE THE DATABASE & SCHEMA       ==
-- =========================================
-- All activity for this project will reside within a database,
-- so create the main database.
CREATE OR REPLACE DATABASE WALMART_DB;

-- Create the schema
CREATE OR REPLACE SCHEMA WALMART_DB.BRONZE;

-- Set the Bronze schema as active
USE SCHEMA WALMART_DB.BRONZE;

-- =============================================
-- ==  CREATE THE SNOWFLAKE -> S3 INTEGRATION ==
-- =============================================
-- Next, we need to create the integration between Snowflake and AWS S3
CREATE OR REPLACE STORAGE INTEGRATION WALMART_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  -- STORAGE_AWS_ROLE_ARN = 'Your_scd2_role_arn_from_aws'
  -- STORAGE_ALLOWED_LOCATIONS = ('Your_S3_URI_of_data_bucket_with_folder');
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::205842488239:role/walmart-e2e-project-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://rlk-walmart-e2e-project-bucket/raw-data/');

-- Now, we need to pull information from the WALMART_INTEGRATION
-- configuration to update the configuration for our S3 bucket
-- to complete the Trust handshake between AWS and Snowflake
DESC INTEGRATION WALMART_INTEGRATION;

-- STORAGE_AWS_IAM_USER_ARN = arn:aws:iam::409763989324:user/npj21000-s
-- STORAGE_AWS_EXTERNAL_ID = 

-- These were copied into the "Trust relationships" tab for the "walmart-e2d-project-role"
-- The resulting JSON:
-- {
--     "Version": "2012-10-17",
--     "Statement": [
--         {
--             "Effect": "Allow",
--             "Principal": {
--                 "AWS": "arn:aws:iam::409763989324:user/npj21000-s"
--             },
--             "Action": "sts:AssumeRole",
--             "Condition": {
--                 "StringEquals": {
--                     "sts:ExternalId": "SQC50361_SFCRole=3_rdFWK/PWI9nnbiIxS6Ah0Vgn2ZY="
--                 }
--             }
--         }
--     ]
-- }

-- =============================================
-- ==  CREATE THE CSV FILE FORMAT             ==
-- =============================================
-- Next, we need to define a FILE FORMAT that we'll be using to copy the data into the
-- STAGE area within Snowflake.
CREATE OR REPLACE FILE FORMAT WALMART_STAGE_CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', 'NA', 'na')  -- Had to add 'NA', 'na' to handle the MardownX fields
  EMPTY_FIELD_AS_NULL = true;

-- =============================================
-- ==  CREATE THE SNOWFLAKE STAGE AREA        ==
-- =============================================
-- Next, let's create the STAGE
CREATE OR REPLACE STAGE WALMART_DB.BRONZE.WALMART_S3_STAGE
  STORAGE_INTEGRATION = WALMART_INTEGRATION
  URL = 's3://rlk-walmart-e2e-project-bucket/raw-data/'
  FILE_FORMAT = WALMART_STAGE_CSV_FORMAT;

-- Confirm the link between Snowflake and AWS S3 is setup
ls @WALMART_DB.BRONZE.WALMART_S3_STAGE

-- All 3 data files were viewable!

-- =============================================
-- ==  CREATE THE BRONZE TABLES               ==
-- =============================================
-- We are now ready to copy the data from S3 into the RAW
-- tables in Snowflake (Bronze layer)

-- Create the tables first
-- department.csv
-- Store	Dept	Date	Weekly_Sales	IsHoliday
-- 1	1	2/5/10	24924.5	FALSE
CREATE OR REPLACE TABLE WALMART_DB.BRONZE.RAW_DEPARTMENT (
    Store INT,
    Dept INT,
    Date DATE,
    Weekly_Sales FLOAT,
    IsHoliday BOOLEAN
);

-- stores.csv
-- Store	Type	Size
-- 1	A	151315
CREATE OR REPLACE TABLE WALMART_DB.BRONZE.RAW_STORES (
    Store INT,
    Type STRING,
    Size INT
);

-- fact.csv
-- Store	Date	Temperature	Fuel_Price	MarkDown1	MarkDown2	MarkDown3	MarkDown4	MarkDown5	CPI	Unemployment	IsHoliday
-- 1	2/5/10	42.31	2.572	NA	NA	NA	NA	NA	211.0963582	8.106	FALSE
CREATE OR REPLACE TABLE WALMART_DB.BRONZE.RAW_FACT (
    Store INT,
    Date STRING,  -- Originally created as type DATE, but encountered parsing issues.  Let DBT handle the conversion
    Temperature FLOAT,
    Fuel_Price FLOAT,
    MarkDown1 FLOAT,
    MarkDown2 FLOAT,
    MarkDown3 FLOAT,
    MarkDown4 FLOAT,
    MarkDown5 FLOAT,
    CPI FLOAT,
    Unemployment FLOAT,
    IsHoliday BOOLEAN
);

-- =============================================
-- ==  COPY THE DATA FROM THE STAGE AREA TO   ==
-- ==  THE TABLES IN THE BRONZE SCHEMA        ==
-- =============================================
-- Now, copy the data from the CSV files into each table using the COPY INTO command.
-- Load department.csv
COPY INTO WALMART_DB.BRONZE.RAW_DEPARTMENT
FROM @WALMART_DB.BRONZE.WALMART_S3_STAGE/department.csv
FILE_FORMAT = (FORMAT_NAME = WALMART_STAGE_CSV_FORMAT)
ON_ERROR = 'CONTINUE';

-- Load stores.csv
COPY INTO WALMART_DB.BRONZE.RAW_STORES
FROM @WALMART_DB.BRONZE.WALMART_S3_STAGE/stores.csv
FILE_FORMAT = (FORMAT_NAME = WALMART_STAGE_CSV_FORMAT)
ON_ERROR = 'CONTINUE';

-- Load fact.csv
COPY INTO WALMART_DB.BRONZE.RAW_FACT
FROM @WALMART_DB.BRONZE.WALMART_S3_STAGE/fact.csv
FILE_FORMAT = (FORMAT_NAME = WALMART_STAGE_CSV_FORMAT)
ON_ERROR = 'CONTINUE';

-- =============================================
-- ==  PERFORM SOME VALIDATION QUERIES        ==
-- =============================================
SELECT * FROM WALMART_DB.BRONZE.RAW_DEPARTMENT LIMIT 30;
SELECT * FROM WALMART_DB.BRONZE.RAW_STORES LIMIT 30;
SELECT * FROM WALMART_DB.BRONZE.RAW_FACT
ORDER BY
    Date ASC
LIMIT 30;

DESC TABLE WALMART_DB.BRONZE.RAW_STORES;

SELECT * FROM WALMART_DB.SILVER.WALMART_STORE_DIM LIMIT 30;
SELECT * FROM WALMART_DB.SILVER.WALMART_DATE_DIM LIMIT 30;
SELECT * FROM WALMART_DB.SILVER.WALMART_FACT_TABLE LIMIT 300;
SELECT * FROM WALMART_DB.BRONZE.RAW_DEPARTMENT LIMIT 30;

SELECT COUNT(*) FROM WALMART_DB.BRONZE.RAW_DEPARTMENT;
SELECT COUNT(*) FROM WALMART_DB.SILVER.WALMART_STORE_DIM;
SELECT COUNT(*) FROM WALMART_DB.SILVER.WALMART_DATE_DIM;
SELECT COUNT(*) FROM WALMART_DB.SILVER.WALMART_FACT_TABLE;

-- =============================================
-- ==  RESOLVE DATA INGETRITY ISSUES          ==
-- =============================================
-- Having an issue with the TO_CHAR function not returning the
-- month name or day name properly.  Perform a test using the 
-- TO_VARCHAR function to see if that makes a difference.
SELECT
  CURRENT_DATE AS date_day,
  TO_VARCHAR(CURRENT_DATE, 'MONTH') AS month_raw,
  INITCAP(TRIM(TO_VARCHAR(CURRENT_DATE, 'MONTH'))) AS month_name,
  TO_VARCHAR(CURRENT_DATE, 'DAY') AS day_raw,
  INITCAP(TRIM(TO_VARCHAR(CURRENT_DATE, 'DAY'))) AS day_name

-- There was no difference.  The month name for month 11 is still
-- returning "Novth" and the day name is returning "Day"

-- Test to see if the issue with the TO_CHAR function is related
-- to the language or the nls_date_language setting.
ALTER SESSION SET LANGUAGE = 'ENGLISH';
ALTER SESSION SET NLS_DATE_LANGUAGE = 'AMERICAN';
SELECT
  CURRENT_DATE AS date_day,
  TO_CHAR(CURRENT_DATE, 'MONTH') AS month_raw,
  INITCAP(TRIM(TO_CHAR(CURRENT_DATE, 'MONTH'))) AS month_name,
  TO_CHAR(CURRENT_DATE, 'DAY') AS day_raw,
  INITCAP(TRIM(TO_CHAR(CURRENT_DATE, 'DAY'))) AS day_name

-- My permisssions for my account do not allow me to alter the session,
-- so I decided to take a step back and avoid using the TO_CHAR
-- or TO_VARCHAR functions.  Since I was only trying to convert
-- 12 months and 7 days, I used the simple CASE statement to
-- do the conversions.  Simple, straight forward, baseline 
-- functionality.  And it worked.

-- Need to see if there are date differences between the raw_fact
-- and raw_department tables.
SELECT
    count(date)
FROM
    raw_department
WHERE
    date NOT IN (
        SELECT date from raw_fact
    )

-- There are 279,054 date differences.  That would explain the
-- referential integrity test failures.  Need to combine all the 
-- dates together in our walmart_date_dim before we build our 
-- walmart_fact_table.

-- ===================================================
-- ==  CREATE THE SQL USED TO GENERATE THE REPORTS  ==
-- ===================================================


-- ======================================================
-- REPORT #1 - Weekly Sales by Store and Holiday
-- ======================================================

SELECT 
    store_id, 
    is_holiday, 
    SUM(store_weekly_sales) AS weekly_sales
FROM WALMART_DB.SILVER.walmart_fact_table
GROUP BY store_id, is_holiday
ORDER BY store_id, is_holiday

-- ======================================================
-- REPORT #2 - Weekly Sales by Temperature and Year
-- ======================================================

SELECT 
  f.store_temperature,
  d.year,
  SUM(f.store_weekly_sales)
FROM 
  WALMART_DB.SILVER.walmart_fact_table f
LEFT JOIN
  WALMART_DB.SILVER.walmart_date_dim d
ON
  f.date_id = d.date_id
GROUP BY f.store_temperature, d.year
ORDER BY f.store_temperature, d.year

-- ======================================================
-- REPORT #3 - Weekly Sales by Store Size
-- ======================================================

-- Inspect the store sizes to see how granular they are
SELECT COUNT(DISTINCT store_size) FROM WALMART_DB.SILVER.walmart_store_dim;

-- Area line chart
WITH sales_per_store_per_week AS (
    SELECT
        store_id,
        date_id,
        SUM(store_weekly_sales) AS sales_per_store_per_week
    FROM
        WALMART_DB.SILVER.walmart_fact_table
    GROUP BY
        1,2
)
SELECT
    wsd.store_id,
    wsd.store_size,
    AVG(pspw.sales_per_store_per_week) AS total_weekly_sales_per_store
FROM
    sales_per_store_per_week pspw
JOIN
    WALMART_DB.SILVER.walmart_store_dim wsd
ON
    pspw.store_id = wsd.store_id
GROUP BY
    1,2
ORDER BY
    1

-- ChatGPT version
WITH store_weekly AS (
    SELECT 
        wft.store_id,
        wft.date_id,
        SUM(wft.store_weekly_sales) AS weekly_total_per_store
    FROM WALMART_DB.SILVER.walmart_fact_table wft
    GROUP BY 1, 2
)

SELECT 
    wsd.store_id,
    wsd.store_size,
    SUM(sw.weekly_total_per_store) AS total_weekly_sales
FROM store_weekly sw
JOIN WALMART_DB.SILVER.walmart_store_dim wsd 
    ON sw.store_id = wsd.store_id
GROUP BY 1, 2
ORDER BY 1;


SELECT 
    wsd.store_id,
    wsd.store_size,
    SUM(wft.store_weekly_sales) AS total_weekly_sales
FROM 
    WALMART_DB.SILVER.walmart_fact_table wft
JOIN
    WALMART_DB.SILVER.walmart_store_dim wsd
ON 
    wft.store_id = wsd.store_id
GROUP BY 1,2
ORDER BY 1,2

-- Summary Table
SELECT 
    wft.store_id, 
    wsd.store_size, 
    sum(wft.store_weekly_sales) AS weekly_sales
FROM
    WALMART_DB.SILVER.walmart_fact_table wft
JOIN
    WALMART_DB.SILVER.walmart_store_dim wsd
ON
    wft.store_id = wsd.store_id
WHERE 
    store_size IN (203742, 205863, 202307,126512)
GROUP BY
    wft.store_id, store_size;

-- Summary Table for one month
SELECT 
    wsd.store_id,
    wsd.store_size,
    AVG(wft.store_weekly_sales) AS avg_weekly_sales,
    SUM(wft.store_weekly_sales) AS total_weekly_sales
FROM WALMART_DB.SILVER.walmart_fact_table wft
JOIN WALMART_DB.SILVER.walmart_store_dim wsd 
    ON wft.store_id = wsd.store_id
JOIN WALMART_DB.SILVER.walmart_date_dim d 
    ON wft.date_id = d.date_id
WHERE d.store_date BETWEEN '2010-03-01' AND '2010-03-31'
GROUP BY wsd.store_id, wsd.store_size
ORDER BY total_weekly_sales DESC


DESC TABLE WALMART_DB.SILVER.walmart_date_dim;

SELECT 
    wsd.store_id,
    wsd.store_size,
    TO_CHAR(SUM(wft.store_weekly_sales) * 0.01, '$999,999,999,999.00') AS weekly_sales
FROM 
    WALMART_DB.SILVER.walmart_fact_table wft
JOIN 
    WALMART_DB.SILVER.walmart_store_dim wsd 
ON 
    wft.store_id = wsd.store_id
JOIN 
    WALMART_DB.SILVER.walmart_date_dim wdd 
ON 
    wft.date_id = wdd.date_id
-- WHERE 
--     wdd.date BETWEEN '2010-03-01' AND '2010-03-31'
GROUP BY wsd.store_id, wsd.store_size
ORDER BY weekly_sales DESC

-- ======================================================
-- REPORT #4 - Weekly Sales by Store Type and Month
-- ======================================================

-- Test SQL
SELECT 
    wdd.month_name,
    wsd.store_type,
    TO_CHAR(SUM(wft.store_weekly_sales), '$999,999,999,999.99') AS weekly_sales
FROM 
    WALMART_DB.SILVER.walmart_fact_table wft
JOIN 
    WALMART_DB.SILVER.walmart_store_dim wsd 
ON 
    wft.store_id = wsd.store_id
JOIN
    WALMART_DB.SILVER.walmart_date_dim wdd
ON
    wft.date_id = wdd.date_id
WHERE 
    wsd.store_size IS NOT NULL
GROUP BY 1,2
ORDER BY MIN(wdd.month),2

SELECT 
    wdd.month_name,
    SUM(CASE WHEN wsd.store_type = 'A' THEN wft.store_weekly_sales ELSE 0 END) AS store_type_a_sales,
    SUM(CASE WHEN wsd.store_type = 'B' THEN wft.store_weekly_sales ELSE 0 END) AS store_type_b_sales,
    SUM(CASE WHEN wsd.store_type = 'C' THEN wft.store_weekly_sales ELSE 0 END) AS store_type_c_sales
FROM 
    WALMART_DB.SILVER.walmart_fact_table wft
JOIN 
    WALMART_DB.SILVER.walmart_store_dim wsd 
ON 
    wft.store_id = wsd.store_id
JOIN
    WALMART_DB.SILVER.walmart_date_dim wdd
ON
    wft.date_id = wdd.date_id
WHERE 
    wsd.store_size IS NOT NULL
GROUP BY 1
ORDER BY MIN(wdd.month)
    
-- ======================================================
-- Report #5 - Markdown Sales by Year and Store
-- ======================================================

-- BAR CHART
-- For the grouped bar chart, the store is not represented
SELECT 
    wdd.year,
    COALESCE(SUM(wft.markdown1),0) AS markdown1,
    COALESCE(SUM(wft.markdown2),0) AS markdown2,
    COALESCE(SUM(wft.markdown3),0) AS markdown3,
    COALESCE(SUM(wft.markdown4),0) AS markdown4,
    COALESCE(SUM(wft.markdown5),0) AS markdown5
FROM 
    WALMART_DB.SILVER.walmart_fact_table wft
JOIN
    WALMART_DB.SILVER.walmart_date_dim wdd
ON
    wft.date_id = wdd.date_id
-- WHERE 
--     wft.markdown1 IS NOT NULL
-- AND
--     wft.markdown2 IS NOT NULL
-- AND
--     wft.markdown3 IS NOT NULL
-- AND
--     wft.markdown4 IS NOT NULL
-- AND
--     wft.markdown5 IS NOT NULL
GROUP BY 1
ORDER BY 1

-- SUMMARY CHART
SELECT 
    wdd.year,
    wft.store_id,
    SUM(wft.markdown1) AS markdown1,
    SUM(wft.markdown2) AS markdown2,
    SUM(wft.markdown3) AS markdown3,
    SUM(wft.markdown4) AS markdown4,
    SUM(wft.markdown5) AS markdown5
FROM 
    WALMART_DB.SILVER.walmart_fact_table wft
JOIN
    WALMART_DB.SILVER.walmart_date_dim wdd
ON
    wft.date_id = wdd.date_id
WHERE 
    wft.markdown1 IS NOT NULL
AND
    wft.markdown2 IS NOT NULL
AND
    wft.markdown3 IS NOT NULL
AND
    wft.markdown4 IS NOT NULL
AND
    wft.markdown5 IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2

-- ======================================================
-- REPORT #6 - Weekly Sales by Store Type
-- ======================================================

-- Pie Chart
SELECT 
    wsd.store_type,
    SUM(wft.store_weekly_sales) AS weekly_sales
FROM 
    WALMART_DB.SILVER.walmart_fact_table wft
JOIN
    WALMART_DB.SILVER.walmart_store_dim wsd
ON
    wft.store_id = wsd.store_id
WHERE 
    wsd.store_size IS NOT NULL
GROUP BY 1

-- Grouped Bar Chart
SELECT
    wft.store_id,
    wsd.store_type,
    SUM(wft.store_weekly_sales) AS weekly_sales
FROM
    WALMART_DB.SILVER.walmart_fact_table wft
JOIN
    WALMART_DB.SILVER.walmart_store_dim wsd
ON
    wft.store_id = wsd.store_id
GROUP BY
    1,2
ORDER BY
    2,3 DESC

-- ======================================================
-- REPORT #7 - Fuel Price by Year
-- ======================================================

-- Found a potential problem - no fuel prices were showing
-- for year 2010

-- Are there any fuel prices for 2010?
SELECT
    *
FROM
    WALMART_DB.BRONZE.raw_fact
ORDER BY
    date ASC
LIMIT
    50

SELECT MIN(Date), MAX(Date)
FROM WALMART_DB.BRONZE.RAW_FACT;

SELECT MIN(Date_id), MAX(Date_id)
FROM WALMART_DB.SILVER.WALMART_FACT_TABLE

SELECT COUNT(*)
FROM WALMART_DB.BRONZE.RAW_FACT
WHERE Date IS NULL;

SELECT COUNT(*)
FROM WALMART_DB.SILVER.WALMART_FACT_TABLE
   
-- Donut chart
WITH ByStoreYear AS (
    SELECT
        wft.store_id,
        wdd.year,
        COALESCE(SUM(wft.fuel_price), 0) AS fuel_price
    FROM
        WALMART_DB.SILVER.walmart_fact_table wft
    JOIN
        WALMART_DB.SILVER.walmart_date_dim wdd
    ON
        wft.date_id = wdd.date_id
    GROUP BY
        1,2
    ORDER BY
        1,2
)
SELECT
    store_id,
    SUM(CASE WHEN year = 2010 THEN fuel_price ELSE 0 END) AS total_fuel_2010,
    SUM(CASE WHEN year = 2011 THEN fuel_price ELSE 0 END) AS total_fuel_2011,
    SUM(CASE WHEN year = 2012 THEN fuel_price ELSE 0 END) AS total_fuel_2012
FROM
    ByStoreYear
GROUP BY
    store_id
ORDER BY
    store_id

-- ======================================================
-- REPORT #8 - WEEKLY SALES BY YEAR, MONTH, AND DAY
-- ======================================================

-- By Year
SELECT
    wdd.year,
    sum(wft.store_weekly_sales) as total_annual_sales
FROM
    walmart_fact_table wft
JOIN
    walmart_date_dim wdd
ON
    wft.date_id = wdd.date_id
GROUP BY
    wdd.year
ORDER BY
    1

-- By Month
SELECT
    wdd.month,
    wdd.month_name,
    sum(wft.store_weekly_sales) as total_monthly_sales
FROM
    walmart_fact_table wft
JOIN
    walmart_date_dim wdd
ON
    wft.date_id = wdd.date_id
GROUP BY
    1,2
ORDER BY
    1

-- By Day
SELECT
    wdd.day,
    sum(wft.store_weekly_sales) as total_daily_sales
FROM
    walmart_fact_table wft
JOIN
    walmart_date_dim wdd
ON
    wft.date_id = wdd.date_id
GROUP BY
    1
ORDER BY
    1

-- ======================================================
-- REPORT #9 - Weekly Sales by CPI
-- ======================================================

DESCRIBE TABLE WALMART_FACT_TABLE

-- Missing the "CPI" attribute...D'oh!
-- Truncate the table, then rebuild by running the updated
-- dbt model

TRUNCATE TABLE WALMART_DB.SILVER.WALMART_FACT_TABLE;

SELECT COUNT(*) FROM WALMART_DB.SILVER.WALMART_FACT_TABLE;

-- Report #9 is simply plotting the relationship between the given
-- store_weekly_sales number and the CPI (Consumer Price Index).
-- There is no aggregation taking place.  This is essentially looking
-- for sales patterns arising when the CPI fluctuates.
SELECT
    wft.CPI,
    wft.store_weekly_sales
FROM
    walmart_fact_table wft

-- ======================================================
-- Report #10 - Weekly Sales by Department
-- ======================================================

SELECT
    dept_id,
    sum(store_weekly_sales) as weekly_sales
FROM
    WALMART_FACT_TABLE
GROUP BY
    dept_id
ORDER BY
    dept_id

-- To get a total weekly sales, sum all the weekly sales
-- by department once the dataframe is created

-- Check the initial snapshot
SELECT 
    date_id,
    store_id,
    dept_id,
    cpi,
    dbt_valid_from,
    dbt_valid_to
FROM WALMART_DB.SNAPSHOTS.WALMART_FACT_SNAPSHOT
WHERE
    date_id = 20100205
AND
    store_id = 1
AND
    dept_id = 1;

-- Modify a value in the WALMART_FACT_TABLE to simulate a data change
UPDATE WALMART_DB.SILVER.WALMART_FACT_TABLE 
SET CPI = 221.0000001 
WHERE DATE_ID = 20100205
AND STORE_ID = 1
AND DEPT_ID = 1;























  

