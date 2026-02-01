DROP TABLE IF EXISTS @KEYSPACE.float4;

CREATE TABLE @KEYSPACE.float4 (
        id int PRIMARY KEY,
        value float
);

INSERT INTO @KEYSPACE.float4 (id, value) VALUES (1, 1000.01);
