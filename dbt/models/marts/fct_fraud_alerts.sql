-- Gold: one row per alert. This is what a fraud analyst works from.
select
    transaction_id,
    event_ts,
    loaded_ts,
    event_to_warehouse_seconds as alert_latency_seconds,
    card_id,
    customer_id,
    merchant_id,
    merchant_category,
    amount_usd,
    fraud_score,
    alert_reasons,
    rules_version,
    is_labelled_fraud
from {{ ref('stg_transactions') }}
where is_alert
