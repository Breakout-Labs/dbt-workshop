select
    id,
    application_id,
    decision_status,
    reason
from {{ source('banking', 'loan_decisions') }}
where lower(decision_status) = 'approved'
  and reason is not null
