
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_datetime
from `data-curriculum-478510`.`nyc_taxi_silver`.`fct_trips`
where pickup_datetime is null



  
  
      
    ) dbt_internal_test