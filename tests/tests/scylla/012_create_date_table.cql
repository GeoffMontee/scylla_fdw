DROP TABLE IF EXISTS @KEYSPACE.date;

CREATE TABLE @KEYSPACE.date (
        id int PRIMARY KEY,
        value date
);

INSERT INTO @KEYSPACE.date (id, value) VALUES (1, '2015-10-22');
