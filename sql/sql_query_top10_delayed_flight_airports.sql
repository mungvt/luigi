SELECT
  airport_code,
  airport_name,
  SUM(statistics_flight_delayed) AS delayed_flights
FROM
  airline
GROUP BY
  airport_code,
  airport_name
ORDER BY
  delayed_flights DESC
LIMIT 10;
