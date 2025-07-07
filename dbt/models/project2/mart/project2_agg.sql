-- You can override the configuration in dbt_project.yml at the model level.

{{
    config
    (
        materialized = 'table',
        schema = 'AGG'
    )
}}

SELECT *
FROM  {{ ref('project2_mart') }} 
WHERE name IN ('abc')