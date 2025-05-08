{%- macro concat_columns(columns, delim = ', ') %}
    {%- for column in columns -%}
        {{ column }} {% if not loop.last %} || '{{ delim }}' || {% endif %}
    {%- endfor -%}
{% endmacro %}


{% macro drop_old_relations(dryrun=False) %}

{% if execute %}

{# находим все модели, seed, snapshot проекта dbt #}

{% set current_models = [] %}

{% for node in graph.nodes.values() | selectattr("resource_type", "in", ["model", "snapshot", "seed"]) %}
    {% do current_models.append(node.name) %}
{% endfor %}

{# формирование скрипта удаления всез таблиц и вьюх, которым не соответствует ни одна модель, сид и снэпшот #}

{% set cleanup_query %}
WITH MODELS_TO_DROP AS (
    SELECT
        CASE
            WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE'
            WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW'
        END AS RELATION_TYPE,
        CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) as RELATION_NAME
    FROM
        {{ target.database }}.INFORMATION_SCHEMA.TABLES
    WHERE
        TABLE_SCHEMA = '{{ target.schema }}'
        AND UPPER(TABLE_NAME) NOT IN (
            {%- for model in current_models -%}
                '{{ model.upper() }}'
                {%- if not loop.last -%}
                    ,
                {%- endif %}
            {%- endfor -%}
        )
)
SELECT
    'DROP ' || RELATION_TYPE || ' ' || RELATION_NAME || ';' as DROP_COMMANDS
FROM
    MODELS_TO_DROP;
{% endset %}

{% do log(cleanup_query) %}

{% set drop_commands = run_query(cleanup_query).columns[0].values() %}

{# удаление лишних таблиц и вьюх / вывод скрипта удаления #}


{% if drop_commands %}
    {% if dryrun | as_bool == False %}
        {% do log('Executing DROP commands ...', True) %}
    {% else %}
        {% do log('Printing DROP commands ...', True) %}
    {% endif %}

    {% for drop_command in drop_commands %}
        {% do log(drop_command, True) %}
        {% if  dryrun | as_bool == False %}
            {% do run_query(drop_command) %}
        {% endif %}
    {% endfor %}
{% else %}
     {% do log('No relations to clean', True) %}
{% endif %}

{% endif %}

{% endmacro %}


{% macro bookref_to_bigint(bookref) %}
('0x' || {{ bookref }})::bigint
{% endmacro %}


{% macro genarate_renamed_model(model_name) %}
{#% set query %}
    SELECT
        source_field_name,
        target_field_name
    FROM
        {{ ref('mapping_table_fields') }}
    WHERE 
        table_name = '{{model_name}}'
{% endset %}

{% set field_mapping_query_result = run_query(query) %}
{% if execute %}
    {% set source_field_name_list = field_mapping_query_result.columns[0].values() %}
    {% set target_field_name_list = field_mapping_query_result.columns[1].values() %}
{% else %}
    {% set source_field_name_list = [] %}
    {% set target_field_name_list = [] %}
{% endif %}

SELECT
{% set ns = namespace(ind=0) %}
{% for source_name in source_field_name_list -%}
    {{- source_name }} as {{ target_field_name_list[ns.ind] -}}
    {%- set ns.ind = ns.ind + 1 -%}
    {% if not loop.last %},{% endif %}
{% endfor %}
FROM {{ source('demo_src', 'model_name') }#}
select 1
{% endmacro %}