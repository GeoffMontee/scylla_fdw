#!/usr/bin/env python
from lib.messages import print_error, print_info, print_ok, print_report
from lib.messages import print_usage_error, print_warning
from lib.tests import run_tests
from optparse import OptionParser
from os import path

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except:
    print_error(
        "scylla-driver library not available, please install it before usage!")
    print_error("Install with: pip install scylla-driver")
    exit(1)


def parse_options():
    """Parse and validate options. Returns a dict with all the options."""
    usage = "%prog <arguments>"
    description = ('Run ScyllaDB tests from CQL files')
    parser = OptionParser(usage=usage, description=description)
    parser.add_option('--host', action='store',
                      help='ScyllaDB host/contact point')
    parser.add_option('--port', action='store', default='9042',
                      help='ScyllaDB native transport port (default: 9042)')
    parser.add_option('--keyspace', action='store',
                      help='Keyspace name to use for tests')
    parser.add_option('--username', action='store',
                      help='Username to connect (optional)')
    parser.add_option('--password', action='store',
                      help='Password to connect (optional)')
    parser.add_option('--ssl', action='store_true', default=False,
                      help='Use SSL/TLS connection')
    (options, args) = parser.parse_args()
    # Check for test parameters
    if (options.host is None or
        options.port is None or
            options.keyspace is None):
        print_error("Insufficient parameters, check help (-h)")
        exit(4)
    else:
        return(options)


def main():
    try:
        args = parse_options()
    except Exception as e:
        print_usage_error(path.basename(__file__), e)
        exit(2)
    try:
        # Set up authentication if provided
        auth_provider = None
        if args.username and args.password:
            auth_provider = PlainTextAuthProvider(
                username=args.username,
                password=args.password
            )
        
        # Create cluster connection
        cluster = Cluster(
            contact_points=[args.host],
            port=int(args.port),
            auth_provider=auth_provider
        )
        
        # Connect to cluster
        session = cluster.connect()
        
        # Wrap session in a simple connection object for compatibility
        class ScyllaConnection:
            def __init__(self, session, cluster):
                self.session = session
                self.cluster = cluster
                self.notices = []
            
            def cursor(self):
                return ScyllaCursor(self.session)
            
            def commit(self):
                # ScyllaDB/Cassandra is auto-commit
                pass
            
            def rollback(self):
                # ScyllaDB/Cassandra doesn't support transactions
                pass
        
        class ScyllaCursor:
            def __init__(self, session):
                self.session = session
                self.result = None
            
            def execute(self, query, params=None):
                self.result = self.session.execute(query, params)
                return self.result
            
            def fetchone(self):
                if self.result:
                    return self.result.one()
                return None
            
            def fetchall(self):
                if self.result:
                    return list(self.result)
                return []
            
            def close(self):
                pass
        
        conn = ScyllaConnection(session, cluster)
        
        replaces = {'@KEYSPACE': args.keyspace}
        tests = run_tests('tests/scylla/*.cql', conn, replaces, 'scylla')
        print_report(tests['total'], tests['ok'], tests['errors'])
        
        # Cleanup
        cluster.shutdown()
        
        if tests['errors'] != 0:
            exit(5)
        else:
            exit(0)
    except Exception as e:
        print_error(e)
        exit(3)

if __name__ == "__main__":
    main()
