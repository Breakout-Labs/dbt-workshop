{{
  config(
    materialized = 'table'
    )
}}

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
        
        /*
        If there is no decision row, the decision_status will be null.
        We treat null as "under_review" because the application has not been decided yet.
        */
        coalesce(
            decisions.decision_status,
            'under_review'        -- No decision -> application still under review
        ) as latest_decision_status,
        
        decisions.approved_amount as latest_approved_amount,
        decisions.decision_at as latest_decision_at
    from applications
    left join decisions using (application_id)
    
    /* QUALIFY + ROW_NUMBER() to keep only the most recent decision row per application */
    qualify row_number() over (
        partition by application_id
        order by decisions.decision_at desc
    ) = 1
),

app_with_latest_decision as (
    select
        applications.*,
        latest_decision.latest_decision_status,
        latest_decision.latest_approved_amount,
        latest_decision.latest_decision_at,

        /* Handy flags to turn categories into 1/0 values -> easier to sum later */
        if(latest_decision.latest_decision_status = 'approved', 1, 0) as is_approved,
        if(latest_decision.latest_decision_status = 'rejected', 1, 0) as is_rejected,
        if(latest_decision.latest_decision_status = 'under_review', 1, 0) as is_under_review,

        /* Turnaround time (days) */
        date_diff(latest_decision.latest_decision_at, applications.submitted_at, day) as decision_turnaround_days
    from applications
    left join latest_decision using (application_id)
),

aggregated as (
    select
        bank_id,
        loan_type,

        /* Funnel time grain */
        extract(year from submitted_at) as year,

        /* Volume */
        count(*) as applications_submitted,
        sum(is_approved) as applications_approved,
        sum(is_rejected) as applications_rejected,
        sum(is_under_review) as applications_under_review,

        /* Rates */
        round( (sum(is_approved) / nullif(count(*), 0)) * 100, 2) as approval_rate_pct,

        /* Amounts */
        sum(requested_amount) as total_requested_amount,
        sum(coalesce(latest_approved_amount, 0)) as total_approved_amount,
        round(avg(requested_amount)) as avg_requested_amount,
        round(avg(latest_approved_amount)) as avg_approved_amount,

        /* Speed */
        round(avg(decision_turnaround_days)) as avg_decision_turnaround_days
    from app_with_latest_decision
    group by 1, 2, 3
)

-- Final output: the aggregated funnel table
select
    *
from aggregated
order by year desc, bank_id, loan_type