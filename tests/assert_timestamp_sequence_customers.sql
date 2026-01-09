select
    *
from {{ source('banking', 'customers') }}
where not (created_ts < updated_ts) or not (updated_ts <= _synced_ts)