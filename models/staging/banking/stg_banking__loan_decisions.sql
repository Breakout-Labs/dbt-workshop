with source as (
    select * from {{ source('banking', 'loan_decisions') }}
),

renamed as (
    select
        id as decision_id,
        application_id,
        underwriter_id,
        decision_status,
        approved_amount,
        reason as reject_reason,
        decision_ts as decision_at,
        updated_ts as updated_at,
        _synced_ts
    from source
)

select * from renamed