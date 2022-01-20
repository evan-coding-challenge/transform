with power_generation as (

    select * from {{ ref('stg_power_generation') }}

)
,
calculated as (

    select 

        {{ dbt_utils.surrogate_key(['facility_name', 'power_generation_date_year_month']) }} as sk_fact_power_generation,

        {{ dbt_utils.surrogate_key(['facility_name']) }} as facility_id,

        power_generation_date_year_month as date_id,

        facility_kw_rating,
        facility_kwh_generated,
        facility_generated,

        case 
            when lower(facility_name) like '%fire station 41%' then true 
            else false 
        end as faulty_meters_flag,

        case 
            when lower(facility_name) like '%fire station 41%' then 0.25
            else 1
        end as faulty_meters_value,

        (facility_kw_rating * faulty_meters_value) as max_kw_rating,
        (facility_kwh_generated * faulty_meters_value) as max_kwh_generated,
        (facility_generated * faulty_meters_value) as max_generated,

        (facility_kw_rating * -1 * faulty_meters_value) as min_kw_rating,
        (facility_kwh_generated * -1 * faulty_meters_value) as min_kwh_generated,
        (facility_generated * -1 * faulty_meters_value) as min_generated


    from power_generation 
    where is_current_version = 1
)
,
final as (

    select 

        sk_fact_power_generation,
        facility_id,
        date_id,
        faulty_meters_flag,
        faulty_meters_value,

        avg(facility_kw_rating) as average_facility_kw_rating,
        sum(facility_kwh_generated) as total_facility_kwh_generated,
        sum(facility_generated) as total_facility_generated,

        avg(max_kw_rating) as average_max_kw_rating,
        sum(max_kwh_generated) as total_max_kwh_generated,
        sum(max_generated) as total_max_generated,

        avg(min_kw_rating) as average_min_kw_rating,
        sum(min_kw_rating) as average_min_kwh_generated,
        sum(min_kwh_generated) as average_min_generated

    from calculated 
    {{ dbt_utils.group_by(n=5) }}


)

select * from final 

