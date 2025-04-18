{{ 
    config(materialized='table') 
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = 'string' %}

select id
       ,cast(field1 as {{string_type}}) as field1
       --,field2
       ,cast(field3 as {{string_type}}) as field3
       ,cast(field4 as {{string_type}}) as field4

from source_data
order by id