import pandas
import pandas as pd
from cassandra.cluster import Cluster
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)


class CassandraClient:
    def __init__(self) -> None:
        logging.info(f'Connecting to cassandra cluster with client')
        super().__init__()
        self.cluster = Cluster(['localhost'], port=9042)
        logging.debug(f'Using flights Keyspace')
        self.session = self.cluster.connect('flights')

    def __del__(self):
        self.cluster.shutdown()

    def insert_query(self, df: pandas.DataFrame, table_name, table_columns):
        for row in df.itertuples():
            values = [getattr(row, column) for column in table_columns]
            values = [value for value in values if value != '$$$']
            filtered_table_columns = [column for column in table_columns if getattr(row, column) != '$$$']
            filtered_table_columns_str = ', '.join(filtered_table_columns)
            num_columns = len(filtered_table_columns)
            string_placeholders = ', '.join(['%s' for _ in range(num_columns)])
            query = f"""
                        INSERT INTO {table_name} ({filtered_table_columns_str})
                        VALUES ({string_placeholders})
                        """
            logging.info(f'Executing the following query: {query}', *values)
            self.session.execute(
                query,
                values
            )

    def execute_query(self, query: str):
        try:
            response = self.session.execute(query, timeout=120)
        except Exception as e:
            logging.error(f"Failed to run query: {query}. Error: {e}")
            response = None
        return pd.DataFrame(response)
