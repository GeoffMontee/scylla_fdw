DROP FOREIGN TABLE IF EXISTS @PSCHEMANAME.varbinary4;

CREATE FOREIGN TABLE @PSCHEMANAME.varbinary4 (
        id int,
        value bytea
)
        SERVER scylla_svr
        OPTIONS (table '@MSCHEMANAME.varbinary4', row_estimate_method 'showplan_all');

SELECT * FROM @PSCHEMANAME.varbinary4;
