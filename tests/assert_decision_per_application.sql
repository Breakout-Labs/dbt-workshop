select
    a.id as application_id
from {{ source('banking', 'loan_applications') }} a
left join {{ source('banking', 'loan_decisions') }} d
    on d.application_id = a.id
where d.application_id is null