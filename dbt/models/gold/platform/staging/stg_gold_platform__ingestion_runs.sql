select
    run_id,
    ingestion_name,
    status,
    rows_read,
    rows_written,
    started_at,
    finished_at,
    (unix_millis(finished_at) - unix_millis(started_at)) / 1000.0 as duration_seconds
from {{ source('controller', 'execution_log') }}
