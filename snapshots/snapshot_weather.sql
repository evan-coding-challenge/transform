{% snapshot snapshot_weather %}

{{
    config (
      -- target_database='analytics',
      target_schema = 'snapshots',

      strategy='check',

      unique_key= 'station', -- concat('station', 'date'),
      check_cols = ['station', 'date']

    )
}}

select * from {{ ref('weather') }}

{% endsnapshot %}
