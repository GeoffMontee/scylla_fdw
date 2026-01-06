DROP SERVER IF EXISTS scylla_svr CASCADE;

CREATE SERVER scylla_svr
       FOREIGN DATA WRAPPER scylla_fdw
       OPTIONS (host '@SHOST', port '@SPORT');
