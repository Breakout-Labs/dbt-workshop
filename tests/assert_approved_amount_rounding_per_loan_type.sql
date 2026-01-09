select
    d.id as decision_id,
    d.application_id,
    a.loan_type,
    d.approved_amount
from {{ source('banking', 'loan_decisions') }} d
join {{ source('banking', 'loan_applications') }} a
    on d.application_id = a.id
where d.approved_amount is not null
  and (
        (a.loan_type = 'mortgage' and mod(cast(d.approved_amount as int), 1000) <> 0)
     or (a.loan_type in ('auto', 'personal') and mod(cast(d.approved_amount as int), 100) <> 0)
  )