{{
    config(
        materialized='table'
    )
}}

select
    *
from {{ ref('stg_banking__banks') }}
where bank_id = 6