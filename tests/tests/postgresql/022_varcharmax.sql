DROP FOREIGN TABLE IF EXISTS @PSCHEMANAME.varcharmax;

CREATE FOREIGN TABLE @PSCHEMANAME.varcharmax (
        id int,
        value varchar
)
        SERVER scylla_svr
        OPTIONS (table '@MSCHEMANAME.varcharmax', row_estimate_method 'showplan_all');

SELECT * FROM @PSCHEMANAME.varcharmax;
