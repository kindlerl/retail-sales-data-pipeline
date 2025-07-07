{{
    config
    (
        materialized = 'table'
    )
}}

SELECT *
FROM  {{ ref('project2_raw') }} 
WHERE name IN ('xyz', 'abc')