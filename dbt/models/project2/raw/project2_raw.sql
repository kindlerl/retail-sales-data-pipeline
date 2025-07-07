{{
    config
    (
        materialized = 'table'
    )
}}

SELECT 'abc' AS name
UNION
SELECT 'xyz' AS name
UNION
SELECT 'bcd' AS name