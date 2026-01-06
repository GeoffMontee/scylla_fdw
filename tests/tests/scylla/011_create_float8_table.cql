DROP TABLE IF EXISTS @KEYSPACE.float8;

CREATE TABLE @KEYSPACE.float8 (
        id int PRIMARY KEY,
        value double
);

INSERT INTO @KEYSPACE.float8 (id, value) VALUES (1, 1000.01);
