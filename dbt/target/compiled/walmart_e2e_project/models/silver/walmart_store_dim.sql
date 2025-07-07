

WITH raw_stores AS (
  SELECT 
    store,
    type,
    size
  FROM
    WALMART_DB.BRONZE.raw_stores
),
raw_department AS (
  SELECT DISTINCT
    store,
    dept
  FROM
    WALMART_DB.BRONZE.raw_department
)
SELECT
  md5(cast(coalesce(cast(s.store as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(d.dept as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS store_dept_sk,
  s.store AS Store_id,
  d.dept AS Dept_id,
  s.type AS Store_type,
  s.size AS Store_size,
  CURRENT_TIMESTAMP() AS Insert_date,
  CURRENT_TIMESTAMP() AS Update_date
FROM
  raw_stores s
LEFT JOIN
  raw_department d
ON
  s.store = d.store