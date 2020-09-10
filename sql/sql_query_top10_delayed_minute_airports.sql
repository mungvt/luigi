SELECT
  airport_code,
  airport_name,
  SUM(statistics__minr_delays__carrier) AS minutes_delay
FROM
  airline
GROUP BY
  airport_code,
  airport_name
ORDER BY
  minutes_delay DESC
LIMIT 10;
