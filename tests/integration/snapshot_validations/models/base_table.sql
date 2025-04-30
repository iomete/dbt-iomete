{{
    config(materialized='incremental',
    unique_key='id')
}}

select * from {{ ref('seed_data') }}