-- Create keyspace for ScyllaDB tests
CREATE KEYSPACE IF NOT EXISTS @KEYSPACE
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

