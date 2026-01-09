{{ config(materialized='table') }}

with applications as (
    select
        *
    from {{ ref('stg_banking__loan_applications') }}
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
        decision_at as latest_decision_at
    from decisions
    qualify row_number() over (
        partition by application_id
        order by decision_at desc
    ) = 1
),

app_with_latest_decision as (
    select
        applications.*,
        latest_decision.latest_decision_status,
        latest_decision.latest_approved_amount,
        latest_decision.latest_decision_at,

        /* handy flags */
        case when latest_decision.latest_decision_status = 'approved' then 1 else 0 end as is_approved,
        case when latest_decision.latest_decision_status = 'rejected' then 1 else 0 end as is_rejected,

        /* turnaround time (days) */
        date_diff(latest_decision.latest_decision_at, applications.submitted_at, day) as decision_turnaround_days
    from applications
    left join latest_decision using (application_id)
),

aggregated as (
    select
        bank_id,
        loan_type,

        /* funnel time grain */
        extract(year from submitted_at) as year,

        /* volume */
        count(*) as applications_submitted,
        sum(is_approved) as applications_approved,
        sum(is_rejected) as applications_rejected,

        /* rates */
        round( (sum(is_approved) / nullif(count(*), 0)) * 100, 2) as approval_rate_pct,

        /* amounts */
        sum(requested_amount) as total_requested_amount,
        sum(coalesce(latest_approved_amount, 0)) as total_approved_amount,
        round(avg(requested_amount)) as avg_requested_amount,
        round(avg(latest_approved_amount)) as avg_approved_amount,

        /* speed */
        round(avg(decision_turnaround_days)) as avg_decision_turnaround_days
    from app_with_latest_decision
    group by 1, 2, 3
)

select
    *
from aggregated
order by year desc, bank_id, loan_type