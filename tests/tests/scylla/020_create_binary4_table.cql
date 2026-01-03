DROP TABLE IF EXISTS @KEYSPACE.binary4;

CREATE TABLE @KEYSPACE.binary4 (
        id int PRIMARY KEY,
        value blob
);

INSERT INTO @KEYSPACE.binary4 (id, value) VALUES (1, 0x01020304);
