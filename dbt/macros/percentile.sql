{# Percentile works differently in each warehouse, so we hide the difference behind one macro. #}
{% macro percentile(column, p) %}{{ return(adapter.dispatch('percentile')(column, p)) }}{% endmacro %}

{% macro duckdb__percentile(column, p) %}quantile_cont({{ column }}, {{ p }}){% endmacro %}

{% macro bigquery__percentile(column, p) %}approx_quantiles({{ column }}, 100)[offset({{ (p * 100) | int }})]{% endmacro %}
