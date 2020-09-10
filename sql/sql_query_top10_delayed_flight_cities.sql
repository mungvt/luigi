SELECT
  airport_city,
  SUM(statistics_flight_delayed) AS delayed_flights
FROM
  airline
GROUP BY
  airport_city
ORDER BY
  delayed_flights DESC
LIMIT 10;
