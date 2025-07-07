{{
    config
    (
        materialized = 'table'
    )
}}

SELECT * FROM {{ ref('project1_bronze') }} WHERE id = 1