DROP TABLE IF EXISTS @KEYSPACE.datetime2;

CREATE TABLE @KEYSPACE.datetime2 (
        id int PRIMARY KEY,
        value timestamp
);

INSERT INTO @KEYSPACE.datetime2 (id, value) VALUES (1, '2015-10-22 11:01:02');
