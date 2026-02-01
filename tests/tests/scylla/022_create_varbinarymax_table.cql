DROP TABLE IF EXISTS @KEYSPACE.varbinarymax;

CREATE TABLE @KEYSPACE.varbinarymax (
        id int PRIMARY KEY,
        value blob
);

INSERT INTO @KEYSPACE.varbinarymax (id, value) VALUES (1, 0x01020304);
