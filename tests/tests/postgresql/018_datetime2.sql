DROP FOREIGN TABLE IF EXISTS @PSCHEMANAME.datetime2;

CREATE FOREIGN TABLE @PSCHEMANAME.datetime2 (
        id int,
        value timestamp without time zone
)
        SERVER scylla_svr
        OPTIONS (table '@MSCHEMANAME.datetime2', row_estimate_method 'showplan_all');

SELECT * FROM @PSCHEMANAME.datetime2;
