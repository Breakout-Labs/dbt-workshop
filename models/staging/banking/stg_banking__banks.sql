with source as (
    select * from {{ source('banking', 'banks') }}
),

renamed as (
    select
        id as bank_id,
        bank_name,
        country_name
    from source
)

select * from renamed