-- Every alert must say WHY it fired. Returns rows that break the rule (should be zero).
select transaction_id
from {{ ref('fct_fraud_alerts') }}
where alert_reasons is null or alert_reasons = '[]'
