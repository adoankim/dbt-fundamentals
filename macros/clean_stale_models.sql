{% macro clean_stale_models_bq(project_id=target.database, dataset_id=target.schema, days=7, dry_run=True) %}

    {% set get_drop_commands_query %}
        SELECT 
            'DROP TABLE '
            || '{{ project_id }}.'
            || destination_table.dataset_id
            || '.'
            || destination_table.table_id
            || ';'
        FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
            destination_table.project_id = '{{ project_id }}' AND
            destination_table.dataset_id = '{{ dataset_id }}'
        GROUP BY destination_table.dataset_id, destination_table.table_id
        HAVING MAX(creation_time) <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ days }} DAY)
    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

    {% for drop_query in drop_queries %}
        {% if execute and not dry_run %}
            {{ log('Dropping table/view with command: ' ~ drop_query, info=True) }}
            {% do run_query(drop_query) %}    
        {% else %}
            {{ log(drop_query, info=True) }}
        {% endif %}
    {% endfor %}
  
{% endmacro %}