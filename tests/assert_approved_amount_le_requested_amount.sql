select
    d.id as decision_id,
    d.application_id,
    d.approved_amount,
    a.requested_amount
from {{ source('banking', 'loan_decisions') }} d
join {{ source('banking', 'loan_applications') }} a
    on d.application_id = a.id
where d.approved_amount is not null
  and a.requested_amount is not null
  and d.approved_amount > a.requested_amount