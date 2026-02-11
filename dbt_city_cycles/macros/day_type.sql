{% macro day_type(timestamp_column) %}
CASE
    WHEN extract(isodow from {{ timestamp_column }}::timestamp) < 6 THEN 'weekday'
    ELSE 'weekend'
END
{% endmacro %}
