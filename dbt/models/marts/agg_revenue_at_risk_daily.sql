-- Gold: daily view per merchant category. "Revenue at risk" = value of transactions flagged as fraud.
select
    event_date,
    merchant_category,
    count(*)                                                        as transactions,
    sum(amount_usd)                                                 as gross_booking_value_usd,
    sum(case when is_alert then 1 else 0 end)                       as alerts,
    sum(case when is_alert then amount_usd else 0 end)              as revenue_at_risk_usd,
    round(100.0 * sum(case when is_alert then amount_usd else 0 end)
          / nullif(sum(amount_usd), 0), 2)                          as revenue_at_risk_pct
from {{ ref('stg_transactions') }}
group by 1, 2
