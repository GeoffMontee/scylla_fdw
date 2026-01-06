DROP TABLE IF EXISTS @KEYSPACE.smallint_min;

CREATE TABLE @KEYSPACE.smallint_min (
        id int PRIMARY KEY,
        value smallint
);

INSERT INTO @KEYSPACE.smallint_min (id, value) VALUES (1, -32768);
