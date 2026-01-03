DROP TABLE IF EXISTS @KEYSPACE.varchar;

CREATE TABLE @KEYSPACE.varchar (
        id int PRIMARY KEY,
        value text
);

INSERT INTO @KEYSPACE.varchar (id, value) VALUES (1, 'this is a string');
