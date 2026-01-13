with 

source as (

    select * from {{ source('banking', 'loan_applications') }}

),

renamed as (

    select
        id as application_id,
        bank_id,
        customer_id,
        requested_amount,
        currency,
        loan_type,
        loan_status,
        loan_term_months,
        submitted_ts as submitted_at,
        updated_ts as updated_at,
        _synced_ts

    from source

)

select * from renamed