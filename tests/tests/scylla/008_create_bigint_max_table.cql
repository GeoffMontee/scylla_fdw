DROP TABLE IF EXISTS @KEYSPACE.bigint_max;

CREATE TABLE @KEYSPACE.bigint_max (
        id int PRIMARY KEY,
        value bigint
);

INSERT INTO @KEYSPACE.bigint_max (id, value) VALUES (1, 9223372036854775807);
