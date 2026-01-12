DROP TABLE IF EXISTS @KEYSPACE.datetimeoffset;

CREATE TABLE @KEYSPACE.datetimeoffset (
        id int PRIMARY KEY,
        value timestamp
);

INSERT INTO @KEYSPACE.datetimeoffset (id, value) VALUES (1, '2015-10-22 11:01:02');
