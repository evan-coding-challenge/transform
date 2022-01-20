with weather_station as (

    select * from {{ ref('stg_weather_events') }} 

)
,
final as (

    select 

        {{ dbt_utils.surrogate_key(['weather_station']) }} as weather_station_id,
        weather_station,
        weather_station_name

    from weather_station 
    where is_current_version = 1

)

select * from final 
