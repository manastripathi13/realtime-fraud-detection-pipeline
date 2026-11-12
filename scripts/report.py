"""Print the Gold-layer results after a demo run."""
import duckdb

con = duckdb.connect("data/warehouse.duckdb", read_only=True)
for title, sql in [
    ("Pipeline health (latency in seconds, event -> warehouse)", "select * from gold.rpt_pipeline_health"),
    ("Rule effectiveness", "select * from gold.rpt_rule_effectiveness order by times_fired desc"),
    ("Revenue at risk by merchant category", "select merchant_category, transactions, round(gross_booking_value_usd) gbv_usd, alerts, round(revenue_at_risk_usd) at_risk_usd, revenue_at_risk_pct from gold.agg_revenue_at_risk_daily order by at_risk_usd desc"),
    ("Dead-letter messages", "select count(*) as dead_letter_messages from raw.dead_letter"),
]:
    print(f"\n== {title}")
    print(con.sql(sql))
