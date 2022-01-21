{% snapshot snapshot_power_generation_02 %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='Facility_Name',
          check_cols=['Facility_Name', 'Address_Location'],
        )
    }}

    select * from {{ ref('power_generation_02') }}

{% endsnapshot %}
