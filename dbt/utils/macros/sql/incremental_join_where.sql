{%- macro incremental_join_where(source, unique_key, exclude_columns=[], data_control_columns=true) -%}

{#- Busca colunas da origem -#}
{%- set columns = adapter.get_columns_in_relation(source) | map(attribute='column') | list -%}

{%- if unique_key in exclude_columns -%}
  {%- do exceptions.raise_compiler_error("unique_key não pode estar em exclude_columns") -%}
{%- endif -%}

{%- set exclude_columns = exclude_columns or [] -%}
{%- set selected_columns = [] -%}
{%- for col in columns -%}
  {%- if col not in exclude_columns -%}
    {%- do selected_columns.append(col) -%}
  {%- endif -%}
{%- endfor -%}

{%- set target_columns = [] -%}
{%- if is_incremental() -%}
  {%- set target_columns = adapter.get_columns_in_relation(this) | map(attribute='column') | list -%}
{%- endif -%}

{%- set ignore_cols = [unique_key, 'etl_created_date', 'etl_updated_date'] + exclude_columns -%}
{%- set compare_cols = [] -%}
{%- set missing_cols = [] -%}

{%- for col in selected_columns -%}
  {%- if col not in ignore_cols and (target_columns | length == 0 or col in target_columns) -%}
    {%- do compare_cols.append(col) -%}
  {%- elif col not in ignore_cols and target_columns | length > 0 and col not in target_columns -%}
    {%- do missing_cols.append(col) -%}
  {%- endif -%}
{%- endfor -%}

select
  {% for col in selected_columns %}
    a.{{ col }},
  {%- endfor -%}
  {%- if data_control_columns %}
    {% if is_incremental() %}
      current_timestamp() as etl_created_date,
      current_timestamp() as etl_updated_date
    {% else %}
      current_timestamp() as etl_created_date,
      cast(null as timestamp) as etl_updated_date
    {% endif %}
  {% endif %}
from {{ source }} a

{%- if is_incremental() %}
left join {{ this }} z
  on a.{{ unique_key }} = z.{{ unique_key }}
where (
  z.{{ unique_key }} is null
  {%- if missing_cols | length > 0 %}
  or (
  {%- for col in missing_cols %}
    a.{{ col }} is not null
    {%- if not loop.last %} or {% endif -%}
  {% endfor %}
  )
  {% endif -%}
  {%- if compare_cols | length > 0 %}
  or (
  {%- for col in compare_cols %}
    not (a.{{ col }} <=> z.{{ col }})
    {%- if not loop.last %} or {% endif -%}
  {% endfor %}
  )
  {% endif -%}
)
{% endif -%}
{%- endmacro -%}
