{% macro grant_select(user_email, schema=target.dataset) %}

  {% set sql %}
    GRANT `roles/bigquery.dataViewer`
    ON SCHEMA {{schema}}
    TO "user:{{user_email}}"
  {% endset %}

  {{ log('Granting select on all tables and views in schema ' ~ target.schema ~ ' to role ' ~ role, info=True) }}
  {% do run_query(sql) %}
  {{ log('Privileges granted', info=True) }}

{% endmacro %}