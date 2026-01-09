select
    id,
    loan_type,
    loan_term_months
from {{ source('banking', 'loan_applications') }}
where
    (
        loan_type = 'mortgage'
        and loan_term_months not in (120, 180, 240, 300, 360)
    )
    or (
        loan_type = 'auto'
        and loan_term_months not in (24, 36, 48, 60, 72, 84)
    )
    or (
        loan_type = 'personal'
        and loan_term_months not in (12, 24, 36, 48, 60)
    )