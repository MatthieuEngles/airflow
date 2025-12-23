
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_key
from `data-curriculum-478510`.`nyc_taxi_silver`.`dim_date`
where date_key is null



  
  
      
    ) dbt_internal_test