{% macro user_type_mapping(column_name) %}
CASE
    WHEN {{ column_name }} = 'Subscriber' THEN 'member'
    WHEN {{ column_name }} = 'Customer' THEN 'casual'
    ELSE {{ column_name }}
END
{% endmacro %}
