{#- Model names carry their Layer and Source System to be unique in the one project
    (bronze_weather__sao_paulo, stg_silver_ibge__municipios); the table keeps only the part after
    "__", plus "stg_" for staging views. An explicit alias in config() still wins. -#}
{%- macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node.resource_type == 'model' and '__' in node.name -%}
        {%- set table = node.name.split('__')[-1] -%}
        {{ ('stg_' ~ table) if node.name.startswith('stg_') else table }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro -%}
