with power_generation as (

    select * from {{ ref('power_generation_01') }}
    union 
    select * from {{ ref('power_generation_02') }}
    union 
    select * from {{ ref('power_generation_03') }}
    union 
    select * from {{ ref('power_generation_04') }}

)
,
final as (

    select 
        Facility_Name::varchar(256) as facility_name,
        KW_Rating::number(10,2) as facility_kw_rating,
        KWH_Generated::number(10,0) as facility_kwh_generated,
        substring(Date, 0, 10)::date as power_generation_date,
        concat(substring(power_generation_date, 0, 4), '-', substring(power_generation_date, 6, 2), '-01')::date as power_generation_date_year_month,

        Generated::number(10,0) as facility_generated,
        Address_Location::varchar(256) as facility_location,
        row_number() over (partition by facility_name order by Date asc) as is_current_version

    from power_generation


)

select * from final 

