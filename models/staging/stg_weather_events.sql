with weather as (

    select * from {{ ref('snapshot_weather') }}

)
,
final as (

    select 

        ---- Station Info ----
        STATION::varchar(256) as weather_station,
        NAME::varchar(256) as weather_station_name,
        DATE::date as weather_date,
        concat(year(weather_date), '-', month(weather_date), '-01')::date as weather_date_year_month,

        ---- Temperature ----
        TAVG::number(10,0) as average_temperature,
        TMAX::number(10,0) as max_temperature,
        TMIN::number(10,0) as min_temperature,
        TOBS::number(10,0) as temperature_at_observation_time,

        ---- Wind ----
        AWND::number(10,2) as average_wind_speed,
        
        case 
            when len(PGTM)=1 then concat('000', PGTM)
            when len(PGTM)=2 then concat('00', PGTM)
            when len(PGTM)=3 then concat('0', PGTM)
            when len(PGTM)= 4 then PGTM 
            else null
        end::number(4,0) as peak_gust_time,


        WDMV::number(10,2) as total_wind_movement,
        WSFI::number(10,2) as highest_instantaneous_wind_speed,

        WSF5::number(10,2) as fastest_5_second_wind_speed,
        WDF5::number(10,0) as direction_of_fastest_5_second_wind,
        
        WSF2::number(10,2) as fastest_2_minute_wind_speed,
        WDF2::number(10,0) as direction_of_fastest_2_minute_wind,

        ---- Precipitation ----
        PRCP::number(10,2) as precipitation,
        MDPR::number(10,2) as precipitation_total,
        DAPR::number(10,0) as days_included_in_precipitation_total,

        EVAP::number(10,2) as evaporation,

        ---- Water Temperature ----
        MNPN::number(10,0) as daily_min_water_temperature,
        MXPN::number(10,0) as daily_max_water_temperature,

        ---- Snow ----
        SNOW::number(10,2) as snowfall,
        SNWD::number(10,2) as snow_depth,
        WESD::number(10,2) as water_equivalent_of_snow_on_ground,
        WESF::number(10,2) as water_equivalent_of_snowfall,

        ---- Weather Event Flags ---- 
        coalesce(WT01, 0) as fog_or_ice_fog_or_freezing_fog_flag,
        coalesce(WT02, 0) as heavy_fog_or_heaving_freezing_fog_flag,
        coalesce(WT03, 0) as thunder_flag,
        coalesce(WT04, 0) as ice_pellets_or_sleet_or_snow_pellets_or_small_hail_flag,
        coalesce(WT05, 0) as hail_flag,
        coalesce(WT06, 0) as glaze_or_rime_flag,
        coalesce(WT07, 0) as dust_or_volcanic_ash_or_blowing_dust_or_blowing_sand_or_blowing_obstruction_flag,
        coalesce(WT08, 0) as smoke_or_haze_flag,
        coalesce(WT09, 0) as blowing_or_drifting_snow_flag,
        coalesce(WT10, 0) as tornado_or_waterspout_or_funnel_cloud_flag,
        coalesce(WT11, 0) as high_or_damaging_winds_flag,

        row_number() over (partition by weather_station order by weather_date asc) as is_current_version

    from weather 

)

select * from final 

