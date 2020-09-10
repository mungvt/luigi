SELECT
  carrier_topmins.carrier_name,
  carrier_airport_mins.airport_name,
  carrier_topmins.top_mins
FROM (
  SELECT
    carrier_name,
    MAX(sum_mins) AS top_mins
  FROM (
    SELECT
      carrier_name,
      SUM(statistics__minr_delays__carrier) AS sum_mins
    FROM
      airline
    WHERE
      carrier_name IN (
        SELECT
          carrier
        FROM top10_delayed_flight_carriers
      )
    GROUP BY
      airport_name,
      carrier_name
    ORDER BY sum_mins DESC
  ) AS carrier_mins
  GROUP BY carrier_name
) AS carrier_topmins
JOIN (
  SELECT
    carrier_name,
    airport_name,
    SUM(statistics__minr_delays__carrier) AS sum_mins
  FROM
    airline
  WHERE
    carrier_name IN (
      SELECT
        carrier
      FROM
         top10_delayed_flight_carriers
    )
  GROUP BY
    airport_name,
    carrier_name
  ORDER BY
    sum_mins DESC
) AS carrier_airport_mins
ON
  carrier_topmins.top_mins = carrier_airport_mins.sum_mins
ORDER BY
  carrier_topmins.top_mins DESC;
