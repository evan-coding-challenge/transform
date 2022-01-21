{% snapshot snapshot_weather_codes %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='Code',
          check_cols=['Code'],
        )
    }}

    select * from {{ ref('weather_codes') }}

{% endsnapshot %}
