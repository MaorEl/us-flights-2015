# Creating a keyspace for the flights data

CREATE KEYSPACE IF NOT EXISTS flights
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

# Create airlines table

CREATE TABLE IF NOT EXISTS airlines (
    airline_code TEXT PRIMARY KEY,
    airline_name TEXT
);

# Create airports table

CREATE TABLE IF NOT EXISTS airports (
    airport_code TEXT PRIMARY KEY,
    airport_name TEXT,
    city TEXT,
    country TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    state TEXT,
);

# Create flights table

CREATE TABLE IF NOT EXISTS flights (
    id TEXT,
    flight_date DATE,
    day_of_week INT,
    airline TEXT,
    flight_number INT,
    tail_number TEXT,
    origin_airport TEXT,
    destination_airport TEXT,
    departure_delay FLOAT,
    elapsed_time FLOAT,
    air_time FLOAT,
    distance INT,
    arrival_delay FLOAT,
    diverted INT,
    cancelled INT,
    cancellation_reason TEXT,
    air_system_delay FLOAT,
    security_delay FLOAT,
    airline_delay FLOAT,
    late_aircraft_delay FLOAT,
    weather_delay FLOAT,
    PRIMARY KEY ((airline, origin_airport, destination_airport), flight_date, id)
) WITH CLUSTERING ORDER BY (flight_date DESC);

