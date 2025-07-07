

WITH weekly_sales AS (
  SELECT
    store AS store_id,
    dept AS dept_id,
    date,
    weekly_sales
  FROM
    WALMART_DB.BRONZE.raw_department
),
fact_data AS (
  SELECT
    date,
    store AS store_id,
    fuel_price,
    temperature AS store_temperature,
    cpi,
    unemployment,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    isholiday
  FROM
    WALMART_DB.BRONZE.raw_fact
),
date_lookup AS (
  SELECT
    date AS date_day,
    TO_CHAR(date, 'YYYYMMDD')::INT AS date_id
  FROM
    WALMART_DB.SILVER.walmart_date_dim
)
SELECT
  TO_CHAR(w.date, 'YYYYMMDD')::INT AS date_id,
  w.store_id,
  w.dept_id,
  w.weekly_sales AS store_weekly_sales,
  f.fuel_price,
  f.store_temperature,
  f.unemployment,
  f.markdown1,
  f.markdown2,
  f.markdown3,
  f.markdown4,
  f.markdown5,
  f.isholiday as is_holiday,
  CURRENT_TIMESTAMP() AS Insert_date,
  CURRENT_TIMESTAMP() AS Update_date,
  CURRENT_TIMESTAMP() AS vrsn_start_date,
  NULL AS vrsn_end_date
FROM
  weekly_sales w
LEFT JOIN
  fact_data f
ON
  w.store_id = f.store_id
AND
  w.date = f.date
LEFT JOIN
  date_lookup d
ON
  w.date = d.date_day