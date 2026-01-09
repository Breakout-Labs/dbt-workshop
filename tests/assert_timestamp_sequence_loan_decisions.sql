select
    *
from {{ source('banking', 'loan_decisions') }}
where not (decision_ts < updated_ts) or not (updated_ts <= _synced_ts)