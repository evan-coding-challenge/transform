with weather_code as (

    select * from {{ ref('stg_weather_codes') }}

)
,
final as (

    select 

        {{ dbt_utils.surrogate_key(['weather_code']) }} as weather_code_id,
        weather_code,
        weather_code_name

    from weather_code 

)

select * from final 

