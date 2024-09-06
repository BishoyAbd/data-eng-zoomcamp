-- models/yellow_taxi_summary.sql
SELECT
  passenger_count,
  SUM(total_amount) AS total_fare,
  COUNT(*) AS total_rides
FROM
  yellow_taxi_data
GROUP BY
  passenger_count