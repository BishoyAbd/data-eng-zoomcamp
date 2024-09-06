
  create view "ny_taxi"."public"."yellow_taxi_summary__dbt_tmp"
    
    
  as (
    -- models/yellow_taxi_summary.sql
SELECT
  passenger_count,
  SUM(total_amount) AS total_fare,
  COUNT(*) AS total_rides
FROM
  yellow_taxi_data
GROUP BY
  passenger_count
  );