
    
    

with dbt_test__target as (

  select trip_id as unique_field
  from `data-curriculum-478510`.`nyc_taxi_silver`.`fct_trips`
  where trip_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


