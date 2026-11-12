-- Silver: clean names and types, add a few helper columns. One row per transaction.
select
    transaction_id,
    card_id,
    customer_id,
    merchant_id,
    merchant_category,
    cast(amount as {{ dbt.type_float() }})              as amount_usd,
    ip_country,
    home_country,
    device_id,
    is_known_device,
    channel,
    card_txn_count_window,
    fraud_score,
    is_alert,
    alert_reasons,
    rules_version,
    coalesce(label like 'fraud%', false)                as is_labelled_fraud,  -- ground truth from the simulator
    cast(event_time as timestamp)                       as event_ts,
    cast(loaded_at as timestamp)                        as loaded_ts,
    cast(event_time as date)                            as event_date,
    {{ dbt.datediff('cast(event_time as timestamp)', 'cast(loaded_at as timestamp)', 'millisecond') }} / 1000.0
                                                        as event_to_warehouse_seconds,
    load_count
from {{ source('raw', 'scored_transactions') }}
