#!/usr/bin/python
try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import sql
except ImportError:
    postgresqldb_found = False
else:
    postgresqldb_found = True

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils._text import to_native
from ansible.module_utils.pycompat24 import get_exception

import ast
import traceback

from ansible.module_utils.connection import *
from ansible.module_utils.table import *

# Needed to have pycharm autocompletition working
try:
    from module_utils.connection import *
except:
    pass

DOCUMENTATION = '''
---
module: postgresql_query

short_description: execute a query in a PostGreSQL database and return the results

version_added: "2.3"

description:
    - "execute a query in a PostGreSQL database and return the results"

options:
    database:
        description:
            - Name of the database to connect to.
        default: postgres
    login_host:
        description:
            - Host running the database.
        default: localhost
    login_password:
        description:
            - The password used to authenticate with.
    login_unix_socket:
        description:
            - Path to a Unix domain socket for local connections.
    login_user:
        description:
            - The username used to authenticate with.
    port:
        description:
            - Database port to connect to.
        default: 5432
    query:
        description:
            - Query to execute
        required: true
    parameters:
        description:
            - |
                Parameters of the query as list (if positional parameters are used in query)
                or as dictionary (if named parameters are used).
                Psycopg2 syntax is required for parameters.
                See: http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries

extends_documentation_fragment:
    - Postgresql

notes:
   - This module uses I(psycopg2), a Python PostgreSQL database adapter. You must ensure that psycopg2 is installed on
     the host before using this module. If the remote host is the PostgreSQL server (which is the default case),
     then PostgreSQL must also be installed on the remote host.
     For Ubuntu-based systems, install the C(postgresql), C(libpq-dev), and C(python-psycopg2) packages
     on the remote host before using this module.

requirements: [ psycopg2 ]

author:
    - Denis Gasparin (@rtshome)
'''

EXAMPLES = '''
---
# Fetch all tables in my_app database
- postgresql_query:
    database: my_app
    query: "SELECT * FROM pg_tables"

# Fetch table info for table "pg_statistic"
- postgresql_query:
    database: my_app
    query: "SELECT * FROM pg_tables WHERE tablename = %(table_name)s"
        table_name: pg_statistic
  register: query_results
'''

RETURN = '''
executed_query:
    description: the body of the last query sent to the backend (including bound arguments) as bytes string
rows:
    description: list of rows. Each row is a dict indexed using the column name
rowCount:
    description: number of rows returned by the query
'''


def run_module():
    module_args = dict(
        login_user=dict(default="postgres"),
        login_password=dict(default="", no_log=True),
        login_host=dict(default=""),
        login_unix_socket=dict(default=""),
        database=dict(default="postgres"),
        port=dict(default="5432"),
        query=dict(required=True),
        parameters=dict(default=[])
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=False
    )

    database = module.params["database"]
    parameters = ast.literal_eval(module.params["parameters"])

    if not postgresqldb_found:
        module.fail_json(msg="the python psycopg2 module is required")

    cursor = None
    try:
        cursor = connect(database, prepare_connection_params(module.params))
        cursor.connection.autocommit = False
        cursor.execute(module.params["query"], parameters)

        module.exit_json(
            changed=True,
            executed_query=cursor.query,
            rows=cursor.fetchall(),
            rowCount=cursor.rowcount
        )

    except psycopg2.ProgrammingError:
        e = get_exception()
        module.fail_json(msg="database error: the query did not produce any resultset, %s" % to_native(e))
    except psycopg2.DatabaseError:
        e = get_exception()
        module.fail_json(msg="database error: %s" % to_native(e), exception=traceback.format_exc())
    except TypeError:
        e = get_exception()
        module.fail_json(msg="parameters error: %s" % to_native(e))
    finally:
        if cursor:
            cursor.connection.rollback()

if __name__ == '__main__':
    run_module()
