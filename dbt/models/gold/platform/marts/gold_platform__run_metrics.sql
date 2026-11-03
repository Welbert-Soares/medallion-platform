{{
    config(materialized='table')
}}

-- One row per Run, from the Ingestion Runs it executed. A Run "failed" when any of its
-- Ingestions failed; "skipped" means nothing new to land (Watermark already current).
select
    run_id,
    min(started_at)                                            as started_at,
    max(finished_at)                                           as finished_at,
    cast(sum(duration_seconds) as decimal(10,1))               as ingest_seconds,
    count(*)                                                   as ingestion_runs,
    count_if(status = 'success')                               as succeeded,
    count_if(status = 'skipped')                               as skipped,
    count_if(status = 'failed')                                as failed,
    cast(sum(rows_read) as bigint)                             as rows_read,
    cast(sum(rows_written) as bigint)                          as rows_written,
    case when count_if(status = 'failed') > 0 then 'failed' else 'success' end as status
from {{ ref('stg_gold_platform__ingestion_runs') }}
group by run_id
