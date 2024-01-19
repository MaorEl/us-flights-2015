# Get % of Cancelled flights by airline, origin airport and destination airport

SELECT
  airline,
  origin_airport,
  destination_airport,
  SUM(cancelled) AS total_cancelled,
  COUNT(*) AS total_flights,
  CAST(SUM(cancelled) AS FLOAT) * 100.0 / CAST(COUNT(*) AS FLOAT) AS cancelled_percentage
FROM
  flights
GROUP BY
  airline, origin_airport, destination_airport;


# Calculate average delay in minutes
SELECT
  AVG(departure_delay) AS avg_departure_delay,
  AVG(arrival_delay) AS avg_arrival_delay,
  AVG(airline_delay) AS avg_airline_delay,
  AVG(security_delay) AS avg_security_delay,
  AVG(weather_delay) AS avg_weather_delay
    FROM flights;

    # Calculate average delay in minutes in case of having a delay
SELECT
  AVG(arrival_delay) AS avg_arrival_delay
    FROM flights
    WHERE arrival_delay > 0 ALLOW FILTERING;

    SELECT
  AVG(weather_delay) AS avg_weather_delay
    FROM flights
    WHERE weather_delay > 0 ALLOW FILTERING;

    SELECT
  AVG(security_delay) AS avg_security_delay
    FROM flights
    WHERE security_delay > 0 ALLOW FILTERING;

    SELECT
  AVG(airline_delay) AS avg_airline_delay
    FROM flights
    WHERE airline_delay > 0 ALLOW FILTERING;

    # Average delay by airline, origin_airport, destination_airport,
SELECT
airline, origin_airport, destination_airport,
  AVG(airline_delay) AS avg_airline_delay
    FROM flights
    WHERE airline_delay > 0
    GROUP BY airline, origin_airport, destination_airport ALLOW FILTERING;

     # Get the average delay for each partition
SELECT
  origin_airport,
  destination_airport,
  airline,
  AVG(departure_delay) + AVG(arrival_delay) + AVG(weather_delay) + AVG(airline_delay) + AVG(security_delay) as avg_total_delay
FROM
  flights
GROUP BY
  airline, origin_airport, destination_airport ALLOW FILTERING;


# Average delay of Delta air lines
SELECT   AVG(departure_delay) + AVG(arrival_delay) + AVG(weather_delay) + AVG(airline_delay) + AVG(security_delay) as avg_total_delay
FROM flights
WHERE airline = 'DL' ALLOW FILTERING;

# Average delay of origin airport BWI
SELECT   AVG(departure_delay) + AVG(arrival_delay) + AVG(weather_delay) + AVG(airline_delay) + AVG(security_delay) as avg_total_delay
FROM flights
WHERE origin_airport = 'BWI' ALLOW FILTERING;

# Average delay of destination airport BWI
SELECT   AVG(departure_delay) + AVG(arrival_delay) + AVG(weather_delay) + AVG(airline_delay) + AVG(security_delay) as avg_total_delay
FROM flights
WHERE destination_airport = 'BWI' ALLOW FILTERING;


