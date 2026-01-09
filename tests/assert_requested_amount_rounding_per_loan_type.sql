select
    id,
    loan_type,
    requested_amount
from {{ source('banking', 'loan_applications') }}
where
    (
        loan_type = 'mortgage'
        and mod(cast(requested_amount as int), 1000) <> 0
    )
    or (
        loan_type in ('auto', 'personal')
        and mod(cast(requested_amount as int), 100) <> 0
    )