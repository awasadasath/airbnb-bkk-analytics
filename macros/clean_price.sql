{% macro clean_price(column_name) %}
    TRY_CAST(REPLACE(REPLACE({{ column_name }}, '$', ''), ',', '') AS NUMERIC(18, 2))
{% endmacro %}