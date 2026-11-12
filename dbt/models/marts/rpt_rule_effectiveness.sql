-- Gold: how good is each rule? Uses the simulator's ground-truth label.
-- precision = of the alerts this rule helped raise, how many were real fraud.
with hits as (
    select r.rule_name, t.*
    from {{ ref('stg_transactions') }} t
    join {{ ref('fraud_rules') }} r
      on t.alert_reasons like '%"' || r.rule_name || '"%'
)
select
    rule_name,
    count(*)                                                            as times_fired,
    sum(case when is_labelled_fraud then 1 else 0 end)                  as fired_on_fraud,
    round(100.0 * sum(case when is_labelled_fraud then 1 else 0 end) / count(*), 1) as precision_pct
from hits
group by 1
