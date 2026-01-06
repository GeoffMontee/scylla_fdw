DROP FOREIGN TABLE IF EXISTS @PSCHEMANAME.float4;

CREATE FOREIGN TABLE @PSCHEMANAME.float4 (
        id int,
        value real
)
        SERVER scylla_svr
        OPTIONS (table '@MSCHEMANAME.float4', row_estimate_method 'showplan_all');

SELECT * FROM @PSCHEMANAME.float4;
