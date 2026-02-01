DROP FOREIGN TABLE IF EXISTS @PSCHEMANAME.money_max;

CREATE FOREIGN TABLE @PSCHEMANAME.money_max (
        id int,
        value money
)
        SERVER scylla_svr
        OPTIONS (table '@MSCHEMANAME.money_max', row_estimate_method 'showplan_all');

SELECT * FROM @PSCHEMANAME.money_max;
