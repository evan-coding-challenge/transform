with date_spine as (

  {{ dbt_utils.date_spine(
      start_date="to_date('11/01/2009', 'mm/dd/yyyy')",
      datepart="day",
      end_date="dateadd(year, 10, current_date)"
     )
  }}

)
,
calculated as (

    SELECT  
      date_day,
      date_day                                                                                as date_id,

      DAYNAME(date_day)                                                                       as day_name,

      DATE_PART('month', date_day)                                                            as month_actual,
      DATE_PART('year', date_day)                                                             as year_actual,
      DATE_PART(quarter, date_day)                                                            as quarter_actual,

      DATE_PART(dayofweek, date_day) + 1                                                      as day_of_week,
      CASE WHEN day_name = 'Sun' THEN date_day
        ELSE DATEADD('day', -1, DATE_TRUNC('week', date_day)) END                             as first_day_of_week,

      CASE WHEN day_name = 'Sun' THEN WEEK(date_day) + 1
        ELSE WEEK(date_day) END                                                               as week_of_year_temp, --remove this column

      CASE WHEN day_name = 'Sun' AND LEAD(week_of_year_temp) OVER (ORDER BY date_day) = '1'
        THEN '1'
        ELSE week_of_year_temp END                                                            as week_of_year,

      DATE_PART('day', date_day)                                                              as day_of_month,

      ROW_NUMBER() OVER (PARTITION BY year_actual, quarter_actual ORDER BY date_day)          as day_of_quarter,
      ROW_NUMBER() OVER (PARTITION BY year_actual ORDER BY date_day)                          as day_of_year,

      CASE WHEN month_actual < 2
        THEN year_actual
        ELSE (year_actual+1) END                                                              as fiscal_year,
      CASE WHEN month_actual < 2 THEN '4'
        WHEN month_actual < 5 THEN '1'
        WHEN month_actual < 8 THEN '2'
        WHEN month_actual < 11 THEN '3'
        ELSE '4' END                                                                          as fiscal_quarter,

      ROW_NUMBER() OVER (PARTITION BY fiscal_year, fiscal_quarter ORDER BY date_day)          as day_of_fiscal_quarter,
      ROW_NUMBER() OVER (PARTITION BY fiscal_year ORDER BY date_day)                          as day_of_fiscal_year,

      TO_CHAR(date_day, 'MMMM')                                                               as month_name,

      TRUNC(date_day, 'Month')                                                                as first_day_of_month,
      LAST_VALUE(date_day) OVER (PARTITION BY year_actual, month_actual ORDER BY date_day)    as last_day_of_month,

      FIRST_VALUE(date_day) OVER (PARTITION BY year_actual ORDER BY date_day)                 as first_day_of_year,
      LAST_VALUE(date_day) OVER (PARTITION BY year_actual ORDER BY date_day)                  as last_day_of_year,

      FIRST_VALUE(date_day) OVER (PARTITION BY year_actual, quarter_actual ORDER BY date_day) as first_day_of_quarter,
      LAST_VALUE(date_day) OVER (PARTITION BY year_actual, quarter_actual ORDER BY date_day)  as last_day_of_quarter,

      FIRST_VALUE(date_day) OVER (PARTITION BY fiscal_year, fiscal_quarter ORDER BY date_day) as first_day_of_fiscal_quarter,
      LAST_VALUE(date_day) OVER (PARTITION BY fiscal_year, fiscal_quarter ORDER BY date_day)  as last_day_of_fiscal_quarter,

      FIRST_VALUE(date_day) OVER (PARTITION BY fiscal_year ORDER BY date_day)                 as first_day_of_fiscal_year,
      LAST_VALUE(date_day) OVER (PARTITION BY fiscal_year ORDER BY date_day)                  as last_day_of_fiscal_year,

      DATEDIFF('week', first_day_of_fiscal_year, date_day) +1                              as week_of_fiscal_year,

      CASE WHEN EXTRACT('month', date_day) = 1 THEN 12
        ELSE EXTRACT('month', date_day) - 1 END                                               as month_of_fiscal_year,

      LAST_VALUE(date_day) OVER (PARTITION BY first_day_of_week ORDER BY date_day)            as last_day_of_week,

      (year_actual || '-Q' || EXTRACT(QUARTER FROM date_day))                                 as quarter_name,

      (fiscal_year || '-' || DECODE(fiscal_quarter,
        1, 'Q1',
        2, 'Q2',
        3, 'Q3',
        4, 'Q4'))                                                                             as fiscal_quarter_name,  
      ('FY' || SUBSTR(fiscal_quarter_name, 3, 7))                                             as fiscal_quarter_name_fy,
      DENSE_RANK() OVER (ORDER BY fiscal_quarter_name)                                        as fiscal_quarter_number_absolute,
      fiscal_year || '-' || MONTHNAME(date_day)                                               as fiscal_month_name,
      ('FY' || SUBSTR(fiscal_month_name, 3, 8))                                               as fiscal_month_name_fy,

      (CASE WHEN MONTH(date_day) = 1 AND DAYOFMONTH(date_day) = 1 THEN 'New Year''s Day'
        WHEN MONTH(date_day) = 12 AND DAYOFMONTH(date_day) = 25 THEN 'Christmas Day'
        WHEN MONTH(date_day) = 12 AND DAYOFMONTH(date_day) = 26 THEN 'Boxing Day'
        ELSE NULL END)::VARCHAR                                                               as holiday_desc,
      (CASE WHEN HOLIDAY_DESC IS NULL THEN 0
        ELSE 1 END)::BOOLEAN                                                                  as is_holiday,
      DATE_TRUNC('month', last_day_of_fiscal_quarter)                                         as last_month_of_fiscal_quarter,
      IFF(DATE_TRUNC('month', last_day_of_fiscal_quarter) = date_day, TRUE, FALSE)         as is_first_day_of_last_month_of_fiscal_quarter,
      DATE_TRUNC('month', last_day_of_fiscal_year)                                            as last_month_of_fiscal_year,
      IFF(DATE_TRUNC('month', last_day_of_fiscal_year) = date_day, TRUE, FALSE)            as is_first_day_of_last_month_of_fiscal_year

    from date_spine

)
,
final as (

    select  

        date_id,
        date_day,
        day_name,
        month_actual,
        year_actual,
        quarter_actual,
        day_of_week,
        first_day_of_week,
        week_of_year,
        day_of_month,
        day_of_quarter,
        day_of_year,
        fiscal_year,
        fiscal_quarter,
        day_of_fiscal_quarter,
        day_of_fiscal_year,
        month_name,
        first_day_of_month,
        last_day_of_month,
        first_day_of_year,
        last_day_of_year,
        first_day_of_quarter,
        last_day_of_quarter,
        first_day_of_fiscal_quarter,
        last_day_of_fiscal_quarter,
        first_day_of_fiscal_year,
        last_day_of_fiscal_year,
        week_of_fiscal_year,
        month_of_fiscal_year,
        last_day_of_week,
        quarter_name,
        fiscal_quarter_name,
        fiscal_quarter_name_fy,
        fiscal_quarter_number_absolute,
        fiscal_month_name,
        fiscal_month_name_fy,
        holiday_desc,
        is_holiday,
        last_month_of_fiscal_quarter,
        is_first_day_of_last_month_of_fiscal_quarter,
        last_month_of_fiscal_year,
        is_first_day_of_last_month_of_fiscal_year

    from calculated
    order by date_day

)

select * from final 
