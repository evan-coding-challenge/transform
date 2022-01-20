with dim_date as (

    select * from {{ ref('dim_date') }}

)
,
dim_facility as (

    select * from {{ ref('dim_facility') }}

)
,
dim_weather_code as (

    select * from {{ ref('dim_weather_code') }}

)
,
dim_weather_station as (

    select * from {{ ref('dim_weather_station') }}
    
)
,
fact_power_generation as (

    select * from {{ ref('fact_power_generation') }}

)
,
fact_weather_events as (

    select * from {{ ref('fact_weather_events') }}

)
,
final as (

    select 

        dim_date.date_id,

        -- Facts
        fact_power_generation.facility_id,
        {{ dbt_utils.star(from=ref('fact_power_generation'), except=["date_id", "facility_id"]) }},

        fact_weather_events.weather_station_id,
        {{ dbt_utils.star(from=ref('fact_weather_events'), except=["date_id", "weather_station_id"]) }},

        -- Dims
        {{ dbt_utils.star(from=ref('dim_facility'), except=["facility_id"]) }},
        {{ dbt_utils.star(from=ref('dim_weather_station'), except=["weather_station_id"]) }}

    from dim_date 

    inner join fact_power_generation
    on dim_date.date_id = fact_power_generation.date_id

    left join fact_weather_events
    on dim_date.date_id = fact_weather_events.date_id

    left join dim_facility
    on fact_power_generation.facility_id = dim_facility.facility_id
    
    left join dim_weather_station 
    on fact_weather_events.weather_station_id = dim_weather_station.weather_station_id

)

select * from final 

