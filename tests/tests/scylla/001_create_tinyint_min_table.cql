-- ScyllaDB uses tinyint (8-bit signed integer, -128 to 127)
DROP TABLE IF EXISTS @KEYSPACE.tinyint_min;

CREATE TABLE @KEYSPACE.tinyint_min (
        id int PRIMARY KEY,
        value tinyint
);

INSERT INTO @KEYSPACE.tinyint_min (id, value) VALUES (1, -128);

