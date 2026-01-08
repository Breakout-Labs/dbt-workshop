with applications as (
    select
        * except (id, submitted_ts),
        id as application_id,
        submitted_ts as submitted_at
    from `breakout-labs-training`.raw_banking.loan_applications
),

decisions as (
    select
        *
    from {{ ref('stg_banking__loan_decisions') }}
),

latest_decision as (
    select
        application_id,
        decision_status as latest_decision_status,
        approved_amount as latest_approved_amount,
        decision_ts as latest_decision_ts
    from decisions
    qualify row_number() over (
        partition by application_id
        order by decision_ts desc
    ) = 1
),

app_with_latest_decision as (
    select
        applications.*,
        latest_decision.latest_decision_status,
        latest_decision.latest_approved_amount,
        latest_decision.latest_decision_ts,

        /* handy flags */
        case when latest_decision.latest_decision_status = 'approved' then 1 else 0 end as is_approved,
        case when latest_decision.latest_decision_status = 'rejected' then 1 else 0 end as is_rejected,

        /* turnaround time (days) */
        date_diff(latest_decision.latest_decision_ts, applications.submitted_at, day) as decision_turnaround_days
    from applications
    left join latest_decision using (application_id)
),

aggregated as (
    select
        bank_id,
        loan_type,

        /* funnel time grain */
        date_trunc(submitted_at, year) as year,

        /* volume */
        null as applications_submitted,         -- TODO: Fill nulls
        null as applications_approved,          -- TODO: Fill nulls
        null as applications_rejected,          -- TODO: Fill nulls

        /* rates */
        null as approval_rate_pct,              -- TODO: Fill nulls

        /* amounts */
        null as total_requested_amount,         -- TODO: Fill nulls
        null as total_approved_amount,          -- TODO: Fill nulls
        null as avg_requested_amount,           -- TODO: Fill nulls
        null as avg_approved_amount,            -- TODO: Fill nulls

        /* speed */
        null as avg_decision_turnaround_days    -- TODO: Fill nulls
    from app_with_latest_decision
    group by 1, 2, 3
)

select
    *
from aggregated
order by year desc, bank_id, loan_type
