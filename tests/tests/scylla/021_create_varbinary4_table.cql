DROP TABLE IF EXISTS @KEYSPACE.varbinary4;

CREATE TABLE @KEYSPACE.varbinary4 (
        id int PRIMARY KEY,
        value blob
);

INSERT INTO @KEYSPACE.varbinary4 (id, value) VALUES (1, 0x01020304);
