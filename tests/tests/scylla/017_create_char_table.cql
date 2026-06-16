DROP TABLE IF EXISTS @KEYSPACE.char;

CREATE TABLE @KEYSPACE.char (
        id int PRIMARY KEY,
        value text
);

INSERT INTO @KEYSPACE.char (id, value) VALUES (1, 'this is a string');
