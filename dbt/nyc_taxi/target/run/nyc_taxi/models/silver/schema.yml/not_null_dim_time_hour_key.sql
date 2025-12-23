
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour_key
from `data-curriculum-478510`.`nyc_taxi_silver`.`dim_time`
where hour_key is null



  
  
      
    ) dbt_internal_test