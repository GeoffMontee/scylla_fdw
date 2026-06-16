DROP TABLE IF EXISTS @KEYSPACE.int_min;

CREATE TABLE @KEYSPACE.int_min (
        id int PRIMARY KEY,
        value int
);

INSERT INTO @KEYSPACE.int_min (id, value) VALUES (1, -2147483648);
