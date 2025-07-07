{{ 
    config(
        database='WALMART_DB',
        schema='SILVER',
        materialized='table',
        tags=['silver', 'scd1']
    )
}}

WITH raw_stores AS (
  SELECT 
    store,
    type,
    size
  FROM
    {{ source('walmart', 'raw_stores') }}
),
raw_department AS (
  SELECT DISTINCT
    store,
    dept
  FROM
    {{ source('walmart', 'raw_department') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['s.store', 'd.dept']) }} AS store_dept_sk,
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




