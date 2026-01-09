select
    id,
    loan_type,
    requested_amount
from {{ source('banking', 'loan_applications') }}
where requested_amount <= 0