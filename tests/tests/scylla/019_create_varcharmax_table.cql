DROP TABLE IF EXISTS @KEYSPACE.varcharmax;

CREATE TABLE @KEYSPACE.varcharmax (
        id int PRIMARY KEY,
        value text
);

INSERT INTO @KEYSPACE.varcharmax (id, value) VALUES (1, 'this is a string');
