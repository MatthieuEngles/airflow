
    
    

with dbt_test__target as (

  select rate_code_id as unique_field
  from `data-curriculum-478510`.`nyc_taxi_silver`.`dim_rate_code`
  where rate_code_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


