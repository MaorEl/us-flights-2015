import logging
import uuid

import pandas as pd

from cassandra_client import CassandraClient

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)


def ingest_airlines():
    airlines_file_path = 'dataset/airlines.csv'
    airlines_df = pd.read_csv(airlines_file_path, delimiter=',')
    cassandra_client = CassandraClient()
    airlines_columns = airlines_df.columns.tolist()
    cassandra_client.insert_query(airlines_df, 'airlines', airlines_columns)
    return airlines_df


def ingest_airports():
    airports_file_path = 'dataset/airports.csv'
    airports_df = pd.read_csv(airports_file_path, delimiter=',')
    cassandra_client = CassandraClient()
    airports_columns = airports_df.columns.tolist()
    cassandra_client.insert_query(airports_df, 'airports', airports_columns)
    return airports_df


def cut_random(df, amount):
    shuffled_df = df.sample(n=amount)
    return shuffled_df


def ingest_flights(airports_df, airlines_df):
    flights_file_path = 'dataset/flights.csv'
    should_clean_df = True
    flights_df = pd.read_csv(flights_file_path, delimiter=',')
    flights_df_subset = cut_random(flights_df, 110000)

    if should_clean_df:
        flights_df_subset = clean_flights_df(flights_df_subset, airports_df, airlines_df)

    print(flights_df_subset)
    cassandra_client = CassandraClient()
    flight_columns = flights_df_subset.columns.tolist()
    logging.info("Saving clean  Flights DF")
    flights_df_subset.to_csv('dataset/flights_small_and_clean.csv', index=False)
    logging.info("Saved clean Flights DF")
    print(flight_columns)
    cassandra_client.insert_query(flights_df_subset, 'flights', flight_columns)


def clean_flights_df(flights_df, airports_df, airlines_df):
    logging.info('Start cleaning Flights DF')
    flights_df.columns = flights_df.columns.str.lower()
    flights_df['flight_date'] = pd.to_datetime(flights_df[['year', 'month', 'day']])
    flights_df = flights_df.drop(['year', 'month', 'day'], axis=1)
    flights_df = flights_df.fillna('$$$')
    flights_df['flight_date'] = pd.to_datetime(flights_df['flight_date']).apply(convert_date)
    flights_df['arrival_delay'] = flights_df['arrival_delay'].apply(convert_float_to_int)
    flights_df = flights_df[flights_df['origin_airport'].isin(airports_df['airport_code'])]
    flights_df = flights_df[flights_df['destination_airport'].isin(airports_df['airport_code'])]
    flights_df = flights_df[flights_df['airline'].isin(airlines_df['airline_code'])]

    flights_df = delete_time_columns(flights_df, 'wheels_off')
    flights_df = delete_time_columns(flights_df, 'wheels_on')
    flights_df = delete_time_columns(flights_df, 'scheduled_departure')
    flights_df = delete_time_columns(flights_df, 'scheduled_time')
    flights_df = delete_time_columns(flights_df, 'scheduled_arrival')
    flights_df = delete_time_columns(flights_df, 'departure_time')
    flights_df = delete_time_columns(flights_df, 'arrival_time')
    flights_df = delete_time_columns(flights_df, 'departure_delay')
    flights_df = delete_time_columns(flights_df, 'taxi_out')
    flights_df = delete_time_columns(flights_df, 'taxi_in')
    flights_df['id'] = [str(uuid.uuid4()) for _ in range(len(flights_df))]
    logging.info("Finished cleaning Flights DF")
    return flights_df


def convert_date(timestamp):
    date_part = timestamp.date()
    formatted_date = date_part.strftime('%Y-%m-%d')
    return formatted_date


def delete_time_columns(flights_df, column_name):
    flights_df = flights_df.drop(column_name, axis=1)
    return flights_df


def convert_float_to_int(value):
    try:
        if value != '$$$':
            value_int = int(value)
            return value_int
        else:
            return value
    except ValueError as e:
        print(f"Error converting {value}: {e}")
        return value


def ingest():
    airlines_df = ingest_airlines()
    airports_df = ingest_airports()
    ingest_flights(airports_df, airlines_df)


if __name__ == "__main__":
    ingest()
