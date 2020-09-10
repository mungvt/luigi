SELECT
  cities_top_mins.airport_city,
  cities_airport_mins.airport_name,
  cities_top_mins.topmins
FROM (
  SELECT
    airport_city,
    MAX(sum_mins) AS topmins
  FROM (
    SELECT
      airport_city,
      SUM(statistics__minr_delays__carrier) AS sum_mins
    FROM
      airline
    WHERE
      airport_city IN (
      SELECT
        city
      FROM
        top10_delayed_flight_cities
      )
    GROUP BY
      airport_name,
      airport_city
    ORDER BY
      sum_mins DESC
  ) AS cities_topmins
  GROUP BY
    airport_city
) AS cities_top_mins
JOIN (
  SELECT
    airport_name,
    airport_city,
    SUM(statistics__minr_delays__carrier) AS summins
  FROM
    airline
  WHERE
    airport_city IN (
      SELECT
        city
      FROM
        top10_delayed_flight_cities
    )
  GROUP BY
    airport_name,
    airport_city
  ORDER BY
    summins DESC
) AS cities_airport_mins
ON
  cities_top_mins.topmins = cities_airport_mins.summins
ORDER BY
  cities_top_mins.topmins DESC;
