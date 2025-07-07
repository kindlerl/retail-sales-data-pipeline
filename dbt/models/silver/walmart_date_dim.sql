{{ 
    config(
        database='WALMART_DB',
        schema='SILVER',
        materialized='table',
        tags=['silver', 'scd1']
    )
}}

WITH distinct_dates AS (
    SELECT DISTINCT 
      CAST(date AS DATE) AS date_day
    FROM
      {{ source('walmart', 'raw_fact') }}
),
holiday_flags AS (
  SELECT
    CAST(date AS DATE) AS date_day,
    MAX(IsHoliday) AS IsHoliday  -- Use Min or Max function to remove duplicates; Only works if the data is clean
  FROM
    {{ source('walmart', 'raw_fact') }}
  GROUP BY
    1 -- the date_day
)
SELECT
  TO_CHAR(d.date_day, 'YYYYMMDD')::INT AS date_id,
  d.date_day as date,
  EXTRACT(DAY FROM d.date_day) AS day,
  EXTRACT(WEEK FROM d.date_day) AS week,
  EXTRACT(MONTH FROM d.date_day) AS month,
  CASE EXTRACT(MONTH FROM d.date_day)
    WHEN 1 THEN 'January'
    WHEN 2 THEN 'February'
    WHEN 3 THEN 'March'
    WHEN 4 THEN 'April'
    WHEN 5 THEN 'May'
    WHEN 6 THEN 'June'
    WHEN 7 THEN 'July'
    WHEN 8 THEN 'August'
    WHEN 9 THEN 'September'
    WHEN 10 THEN 'October'
    WHEN 11 THEN 'November'
    WHEN 12 THEN 'December'
  END AS month_name,
  EXTRACT(QUARTER FROM d.date_day) AS quarter,
  EXTRACT(YEAR FROM d.date_day) AS year,
  EXTRACT(DAYOFWEEK FROM d.date_day) AS dayofweek,
  CASE EXTRACT(DAYOFWEEK FROM d.date_day)
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS day_name,
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM d.date_day) IN (1,7) THEN TRUE
    ELSE FALSE
  END AS is_weekend,
  h.IsHoliday,
  CURRENT_TIMESTAMP() AS Insert_date,
  CURRENT_TIMESTAMP() AS Update_date
FROM
  distinct_dates d
LEFT JOIN
  holiday_flags h
ON
  d.date_day = h.date_day




