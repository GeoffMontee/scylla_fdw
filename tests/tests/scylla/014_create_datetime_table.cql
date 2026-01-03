DROP TABLE IF EXISTS @KEYSPACE.datetime;

CREATE TABLE @KEYSPACE.datetime (
        id int PRIMARY KEY,
        value timestamp
);

INSERT INTO @KEYSPACE.datetime (id, value) VALUES (1, '2015-10-22 11:01:02');
