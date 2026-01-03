DROP TABLE IF EXISTS @KEYSPACE.time;

CREATE TABLE @KEYSPACE.time (
        id int PRIMARY KEY,
        value time
);

INSERT INTO @KEYSPACE.time (id, value) VALUES (1, '11:01:02');
