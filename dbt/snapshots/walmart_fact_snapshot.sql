{% snapshot walmart_fact_snapshot %}
{{
    config(
        target_database='WALMART_DB',
        target_schema='SNAPSHOTS',
        unique_key=['DATE_ID', 'DEPT_ID', 'STORE_ID'],
        strategy='check',
        check_cols=['STORE_WEEKLY_SALES','FUEL_PRICE','STORE_TEMPERATURE','UNEMPLOYMENT','CPI',
                    'MARKDOWN1','MARKDOWN2','MARKDOWN3','MARKDOWN4','MARKDOWN5','IS_HOLIDAY'
        ]
    )
}}
select * from {{ source('silver', 'WALMART_FACT_TABLE') }}
{% endsnapshot %}
