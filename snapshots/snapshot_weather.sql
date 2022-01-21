{% snapshot snapshot_weather %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='STATION',
          check_cols=['STATION', 'DATE'],
        )
    }}

    select * from {{ ref('weather') }}

{% endsnapshot %}
