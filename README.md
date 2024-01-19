### US Flights 2015

1. `create.cql` - contains Cassandra Keyspace and tables creation scripts
2. `ingest.py` - ingesting the data from the csv data set into the CQL DB tables
3. `cassandra_client.py` - Flights Cassandra client which allow inserting queries smartly or to execute any script into our Flights keyspace
4. `data_analyze.cql` - Data analayze select queries that can be executed directly in the cqlsh
5. `analyze_data.py` - Executing queries from `data_analyze.cql` file, and using Pandas & Matplotlib in order to achieve insights
