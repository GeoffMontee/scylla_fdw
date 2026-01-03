DROP TABLE IF EXISTS @KEYSPACE.int_max;

CREATE TABLE @KEYSPACE.int_max (
        id int PRIMARY KEY,
        value int
);

INSERT INTO @KEYSPACE.int_max (id, value) VALUES (1, 2147483647);
