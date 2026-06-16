DROP FOREIGN TABLE IF EXISTS @PSCHEMANAME.time;

CREATE FOREIGN TABLE @PSCHEMANAME.time (
        id int,
        value time without time zone
)
        SERVER scylla_svr
        OPTIONS (table '@MSCHEMANAME.time', row_estimate_method 'showplan_all');

SELECT * FROM @PSCHEMANAME.time;
