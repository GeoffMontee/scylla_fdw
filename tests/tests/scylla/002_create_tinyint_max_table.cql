-- ScyllaDB tinyint max is 127
DROP TABLE IF EXISTS @KEYSPACE.tinyint_max;

CREATE TABLE @KEYSPACE.tinyint_max (
        id int PRIMARY KEY,
        value tinyint
);

INSERT INTO @KEYSPACE.tinyint_max (id, value) VALUES (1, 127);
