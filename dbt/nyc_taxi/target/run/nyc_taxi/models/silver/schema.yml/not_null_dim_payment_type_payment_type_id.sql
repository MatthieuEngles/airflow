
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_type_id
from `data-curriculum-478510`.`nyc_taxi_silver`.`dim_payment_type`
where payment_type_id is null



  
  
      
    ) dbt_internal_test