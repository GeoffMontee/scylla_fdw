DROP FOREIGN TABLE IF EXISTS @PSCHEMANAME.datetime;

CREATE FOREIGN TABLE @PSCHEMANAME.datetime (
        id int,
        value timestamp without time zone
)
        SERVER scylla_svr
        OPTIONS (table '@MSCHEMANAME.datetime', row_estimate_method 'showplan_all');

SELECT * FROM @PSCHEMANAME.datetime;
