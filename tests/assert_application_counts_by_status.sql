select
    *
from {{ ref('loan_application_funnel') }}
where applications_submitted != applications_approved + applications_rejected + applications_under_review