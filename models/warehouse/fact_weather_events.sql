with weather_events as (

    select * from {{ ref('stg_weather_events') }}

)
,
final as (

    select 

        {{ dbt_utils.surrogate_key(['weather_station', 'weather_date_year_month']) }} as sk_fact_weather_events,

        {{ dbt_utils.surrogate_key(['weather_station']) }} as weather_station_id,
        weather_date_year_month as date_id,

        avg(average_temperature) as average_temperature,
        avg(max_temperature) as average_max_temperature,
        avg(min_temperature) as average_min_temperature,
        avg(temperature_at_observation_time) as average_temperature_at_observation_time,
        avg(average_wind_speed) as average_wind_speed_average,
        avg(peak_gust_time) as average_peak_gust_time,
        sum(total_wind_movement) as total_wind_movement_total,
        avg(highest_instantaneous_wind_speed) as average_highest_instantaneous_wind_speed,
        avg(fastest_5_second_wind_speed) as average_fastest_5_second_wind_speed,
        avg(direction_of_fastest_5_second_wind) as average_direction_of_fastest_5_second_wind,
        avg(fastest_2_minute_wind_speed) as average_fastest_2_minute_wind_speed,
        avg(direction_of_fastest_2_minute_wind) as average_direction_of_fastest_2_minute_wind,
        sum(precipitation) as total_precipitation,
        sum(precipitation_total) as total_precipitation_total,
        sum(days_included_in_precipitation_total) as total_days_included_in_precipitation_total,
        sum(evaporation) as total_evaporation,
        min(daily_min_water_temperature) as minimum_daily_min_water_temperature,
        max(daily_max_water_temperature) as maximum_daily_max_water_temperature,
        sum(snowfall) as total_snowfall,
        sum(snow_depth) as total_snow_depth,
        sum(water_equivalent_of_snow_on_ground) as total_water_equivalent_of_snow_on_ground,
        sum(water_equivalent_of_snowfall) as total_water_equivalent_of_snowfall,

        sum(fog_or_ice_fog_or_freezing_fog_flag) as total_instances_of_fog_or_ice_fog_or_freezing_fog,
        sum(heavy_fog_or_heaving_freezing_fog_flag) as total_instances_of_heavy_fog_or_heaving_freezing_fog,
        sum(thunder_flag) as total_instances_of_thunder,
        sum(ice_pellets_or_sleet_or_snow_pellets_or_small_hail_flag) as total_instances_of_ice_pellets_or_sleet_or_snow_pellets_or_small_hail_flag,
        sum(hail_flag) as total_instances_of_hail,
        sum(glaze_or_rime_flag) as total_instances_of_glaze_or_rime,
        sum(dust_or_volcanic_ash_or_blowing_dust_or_blowing_sand_or_blowing_obstruction_flag) as total_instances_of_dust_or_volcanic_ash_or_blowing_dust_or_blowing_sand_or_blowing_obstruction,
        sum(smoke_or_haze_flag) as total_instances_of_smoke_or_haze,
        sum(blowing_or_drifting_snow_flag) as total_instances_of_blowing_or_drifting_snow,
        sum(tornado_or_waterspout_or_funnel_cloud_flag) as total_instances_of_tornado_or_waterspout_or_funnel_cloud,
        sum(high_or_damaging_winds_flag) as total_instances_of_high_or_damaging_winds

    from weather_events 

    {{ dbt_utils.group_by(n=3) }}



)

select * from final 

