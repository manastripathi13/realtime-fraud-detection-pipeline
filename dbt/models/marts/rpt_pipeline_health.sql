-- Gold: end-to-end latency (event created -> row in warehouse) and detection quality.
select
    count(*)                                                                as transactions,
    sum(case when is_alert then 1 else 0 end)                               as alerts,
    round({{ percentile('event_to_warehouse_seconds', 0.5) }}, 2)           as p50_latency_seconds,
    round({{ percentile('event_to_warehouse_seconds', 0.95) }}, 2)          as p95_latency_seconds,
    round(max(event_to_warehouse_seconds), 2)                               as max_latency_seconds,
    sum(case when load_count > 1 then 1 else 0 end)                         as duplicates_absorbed_by_merge,
    round(100.0 * sum(case when is_alert and is_labelled_fraud then 1 else 0 end)
          / nullif(sum(case when is_labelled_fraud then 1 else 0 end), 0), 1) as recall_pct,
    round(100.0 * sum(case when is_alert and is_labelled_fraud then 1 else 0 end)
          / nullif(sum(case when is_alert then 1 else 0 end), 0), 1)       as precision_pct
from {{ ref('stg_transactions') }}
