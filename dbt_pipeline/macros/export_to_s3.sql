{% macro export_to_s3(model_name, s3_prefix) %}
  {% set bucket = env_var('AWS_BUCKET_NAME') %}
  {% set s3_path = 's3://' ~ bucket ~ '/' ~ s3_prefix ~ '/' ~ model_name ~ '.parquet' %}
  {% set export_query %}
    COPY (SELECT * FROM {{ model_name }}) TO '{{ s3_path }}' (FORMAT PARQUET);
  {% endset %}
  {% do run_query(export_query) %}
{% endmacro %}
