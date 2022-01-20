with weather_codes as (

    select * from {{ ref('weather_codes') }}

)
,
final as (

    select 
        code::varchar(256) as weather_code,
        full_name::varchar(256) as weather_code_name

    from weather_codes

)

select * from final 

