
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select vendor_id
from `data-curriculum-478510`.`nyc_taxi_silver`.`dim_vendor`
where vendor_id is null



  
  
      
    ) dbt_internal_test