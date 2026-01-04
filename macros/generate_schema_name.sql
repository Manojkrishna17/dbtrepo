{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}

        {{ target.schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}   {# Use ONLY the custom schema name — NO prefix #}

    {%- endif -%}

{%- endmacro %}