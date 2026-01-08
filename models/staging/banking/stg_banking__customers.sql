with source as (
    select * from {{ source('banking', 'customers') }}
),

renamed as (
    select
        id as customer_id,
        bank_id,
        first_name,
        last_name,
        email as customer_email,
        date_of_birth,
        zipcode,
        country,
        created_ts as created_at,
        updated_ts as updated_at,
        _synced_ts
    from source
)

select * from renamed