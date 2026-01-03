DROP TABLE IF EXISTS @KEYSPACE.bigint_min;

CREATE TABLE @KEYSPACE.bigint_min (
        id int PRIMARY KEY,
        value bigint
);

INSERT INTO @KEYSPACE.bigint_min (id, value) VALUES (1, -9223372036854775808);
