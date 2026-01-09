select
    d.id as decision_id,
    d.application_id,
    a.submitted_ts,
    d.decision_ts
from {{ source('banking', 'loan_decisions') }} d
join {{ source('banking', 'loan_applications') }} a
    on d.application_id = a.id
where d.decision_ts is not null
  and a.submitted_ts is not null
  and d.decision_ts < a.submitted_ts