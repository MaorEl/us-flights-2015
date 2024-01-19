import pandas as pd
import matplotlib.pyplot as plt

from cassandra_client import CassandraClient


def add_states_column(cassandra_client, top_cancelled_routes_df):
    airports_df = cassandra_client.execute_query("SELECT * from airports;")
    top_cancelled_routes_df = top_cancelled_routes_df.merge(airports_df[['airport_code', 'state']], left_on='origin_airport',
                                  right_on='airport_code', how='left')
    # Merge for origin airport
    top_cancelled_routes_df = top_cancelled_routes_df.merge(airports_df[['airport_code', 'state']], left_on='origin_airport',
                                  right_on='airport_code', how='left')
    top_cancelled_routes_df.rename(columns={'state_x': 'origin_state'}, inplace=True)
    top_cancelled_routes_df.drop(columns=['airport_code_x', 'airport_code_y', 'state_y'], inplace=True)

    # Merge for destination airport
    top_cancelled_routes_df = top_cancelled_routes_df.merge(airports_df[['airport_code', 'state']], left_on='destination_airport',
                                  right_on='airport_code', how='left')
    top_cancelled_routes_df.rename(columns={'state': 'destination_state'}, inplace=True)
    top_cancelled_routes_df.drop(columns=['airport_code'], inplace=True)

    top_cancelled_routes_df['cancellation_rate'] = top_cancelled_routes_df['total_cancelled'] / \
                                                   top_cancelled_routes_df['total_flights']

    # Sum total_cancelled and total_flights for each origin state
    sum_by_origin_state = top_cancelled_routes_df.groupby('origin_state').agg(
        {'total_cancelled': 'sum', 'total_flights': 'sum'}).reset_index()

    # Sum total_cancelled and total_flights for each destination state
    sum_by_destination_state = top_cancelled_routes_df.groupby('destination_state').agg(
        {'total_cancelled': 'sum', 'total_flights': 'sum'}).reset_index()

    # Calculate average cancellation rate for each origin state
    sum_by_origin_state['avg_cancellation_rate_origin_state'] = sum_by_origin_state['total_cancelled'] / \
                                                                sum_by_origin_state['total_flights']

    # Calculate average cancellation rate for each destination state
    sum_by_destination_state['avg_cancellation_rate_destination_state'] = sum_by_destination_state['total_cancelled'] / \
                                                                          sum_by_destination_state['total_flights']

    sum_by_origin_state = sum_by_origin_state.sort_values(by='avg_cancellation_rate_origin_state', ascending=False)
    sum_by_destination_state = sum_by_destination_state.sort_values(by='avg_cancellation_rate_destination_state',
                                                                    ascending=False)

    return sum_by_origin_state, sum_by_destination_state


def show_graph_of_sum_by_origin_state(sum_by_origin_state):
    top_10_states = sum_by_origin_state.sort_values(by='avg_cancellation_rate_origin_state', ascending=False).head(10)

    # Create a bar chart for the top 10 states
    plt.figure(figsize=(10, 6))
    plt.bar(top_10_states['origin_state'], top_10_states['avg_cancellation_rate_origin_state'], color='red')
    plt.xlabel('Origin State')
    plt.ylabel('Average Cancellation Rate')
    plt.title('Top 10 States by Cancellation Rate')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

    pass


def show_graph_of_sum_by_destination_state(sum_by_destination_state):
    top_10_states = sum_by_destination_state.sort_values(by='avg_cancellation_rate_destination_state', ascending=False).head(10)

    # Create a bar chart for the top 10 states
    plt.figure(figsize=(10, 6))
    plt.bar(top_10_states['destination_state'], top_10_states['avg_cancellation_rate_destination_state'], color='red')
    plt.xlabel('Destination State')
    plt.ylabel('Average Cancellation Rate')
    plt.title('Top 10 States by Cancellation Rate')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

    pass


def show_graph_of_average_delay_by_partition(average_delay_by_partition):
    # Sort the DataFrame by avg_airline_delay in descending order
    top_partitions = average_delay_by_partition.head(10)

    # Create a bar chart for the top partitions
    plt.figure(figsize=(12, 8))
    bars = plt.bar(range(len(top_partitions)), top_partitions['avg_airline_delay'], color='green')
    plt.xlabel('Partition')
    plt.ylabel('Average Airline Delay')
    plt.title(f'Top {10} Partitions by Average Delay')
    plt.xticks(range(len(top_partitions)), top_partitions['airline'] + ' ' + top_partitions['origin_airport'] + ' to ' + top_partitions['destination_airport'], rotation=45, ha='right')

    # Add data labels above the bars for better clarity
    for bar, label in zip(bars, top_partitions['avg_airline_delay']):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 10, f'{label:.2f}', ha='center', va='bottom')

    plt.tight_layout()
    plt.show()
    pass


def analyze():
    pd.set_option('display.max_columns', None)
    cassandra_client = CassandraClient()
    print(get_average_delays(cassandra_client))
    top_cancelled_routes = get_top_by_percentage_of_cancelled_flights_by_partition_key(cassandra_client, 100, 5)
    print(top_cancelled_routes)
    top_cancelled_routes_by_states = add_states_column(cassandra_client, top_cancelled_routes)
    show_graph_of_sum_by_origin_state(top_cancelled_routes_by_states[0])
    show_graph_of_sum_by_destination_state(top_cancelled_routes_by_states[1])
    print(top_cancelled_routes_by_states)
    average_delay_by_partition = get_average_delay_by_partition(cassandra_client)
    show_graph_of_average_delay_by_partition(average_delay_by_partition)

    print(average_delay_by_partition)
    pass


def get_top_by_percentage_of_cancelled_flights_by_partition_key(cassandra_client, top: int, min_flights: int):
    results_df = cassandra_client.execute_query("""SELECT
  airline,
  origin_airport,
  destination_airport,
  SUM(cancelled) AS total_cancelled,
  COUNT(*) AS total_flights,
  CAST(SUM(cancelled) AS FLOAT) * 100.0 / CAST(COUNT(*) AS FLOAT) AS cancelled_percentage
FROM
  flights
GROUP BY
  airline, origin_airport, destination_airport;""")

    results_df = results_df[results_df['total_flights'] > min_flights]
    sorted_df = results_df.sort_values(by='cancelled_percentage', ascending=False)
    top_10_cancelled = sorted_df.head(top)
    return top_10_cancelled


def get_average_delays(cassandra_client):

    query = """SELECT
  AVG(departure_delay) AS avg_departure_delay,
  AVG(arrival_delay) AS avg_arrival_delay,
  AVG(airline_delay) AS avg_airline_delay,
  AVG(security_delay) AS avg_security_delay,
  AVG(weather_delay) AS avg_weather_delay
    FROM flights;"""
    results_df = cassandra_client.execute_query(query)
    return results_df


def get_average_delay_by_partition(cassandra_client):
    query = """SELECT
airline, origin_airport, destination_airport,
  AVG(airline_delay) AS avg_airline_delay
    FROM flights
    WHERE airline_delay > 0
    GROUP BY airline, origin_airport, destination_airport ALLOW FILTERING;"""
    results_df = cassandra_client.execute_query(query)
    results_df = results_df.sort_values(by='avg_airline_delay', ascending=False)
    return results_df



if __name__ == "__main__":
    analyze()
