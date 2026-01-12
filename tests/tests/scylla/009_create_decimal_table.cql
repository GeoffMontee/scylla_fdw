DROP TABLE IF EXISTS @KEYSPACE.decimal;

CREATE TABLE @KEYSPACE.decimal (
        id int PRIMARY KEY,
        value decimal
);

INSERT INTO @KEYSPACE.decimal (id, value) VALUES (1, 1000.01);
