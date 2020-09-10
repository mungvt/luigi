SELECT
  carrier_name,
  SUM(statistics_flight_delayed) AS delayed_flights
FROM
  airline
GROUP BY
  carrier_name
ORDER BY
  delayed_flights DESC
LIMIT 10;
