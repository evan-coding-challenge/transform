with facility as (

    select * from {{ ref('stg_power_generation') }}

)
,
final as (

    select 

        {{ dbt_utils.surrogate_key(['facility_name']) }} as facility_id,

        facility_name,
        facility_kw_rating,
        facility_location
    
    from facility 
    where is_current_version = 1


)

select * from final 
