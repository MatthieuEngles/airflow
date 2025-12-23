
  
    

    create or replace table `data-curriculum-478510`.`nyc_taxi_silver`.`dim_rate_code`
      
    
    

    
    OPTIONS()
    as (
      

/*
    Dimension table for rate codes
*/

SELECT
    rate_code_id,
    CASE rate_code_id
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Unknown'
    END AS rate_code_name
FROM (
    SELECT DISTINCT rate_code_id
    FROM `data-curriculum-478510`.`nyc_taxi_silver`.`stg_yellow_taxi`
    WHERE rate_code_id IS NOT NULL
)
    );
  