select
    *
from {{ source('banking', 'loan_applications') }}
where not (submitted_ts < updated_ts) or not (updated_ts <= _synced_ts)