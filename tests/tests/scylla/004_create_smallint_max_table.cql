DROP TABLE IF EXISTS @KEYSPACE.smallint_max;

CREATE TABLE @KEYSPACE.smallint_max (
        id int PRIMARY KEY,
        value smallint
);

INSERT INTO @KEYSPACE.smallint_max (id, value) VALUES (1, 32767);
