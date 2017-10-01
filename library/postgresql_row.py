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
# noinspection PyBroadException
try:
    from module_utils.connection import *
except:
    pass


DOCUMENTATION = '''
---
module: postgresql_row

short_description: |
                    ensure that there are exactly n rows returned by query.
                    If not execute the command query to add the missing rows

version_added: "2.3"

description:
    - "ensure that there are exactly n rows returned by query. If not execute the command query to add the missing rows"

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
    schema:
        description:
            - Schema where the table is defined
        default: public
    table:
        description:
            - The table to check for row presence/absence
        required: true
    row:
        description:
            - Dictionary with the fields of the row
        required: true
    state:
        description:
            - The row state
        choices:
            - present
            - absent

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
# Ensure row with fields key="environment" and value="production" is present in db
- postgresql_row:
    database: my_app_config
    table: app_config
    row:
        key: environment
        value: production
    state:
        present
'''

RETURN = '''
executed_query:
    description: the body of the last query sent to the backend (including bound arguments) as bytes string
executed_command:
    description: the body of the command executed to insert the missing rows including bound arguments
'''


def run_module():
    module_args = dict(
        login_user=dict(default="postgres"),
        login_password=dict(default="", no_log=True),
        login_host=dict(default=""),
        login_unix_socket=dict(default=""),
        database=dict(default="postgres"),
        port=dict(default="5432"),
        schema=dict(default="public"),
        table=dict(required=True),
        row=dict(required=True),
        state=dict(default="present"),
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    database = module.params["database"]
    schema = module.params["schema"]
    table = module.params["table"]
    row_columns = ast.literal_eval(module.params["row"])
    state = module.params["state"]

    if not postgresqldb_found:
        module.fail_json(msg="the python psycopg2 module is required")

    cursor = None
    try:
        cursor = connect(database, prepare_connection_params(module.params))
        cursor.connection.autocommit = False
        sql_identifiers = {
            'schema': sql.Identifier(schema),
            'table': sql.Identifier(table)
        }
        sql_where = []
        sql_insert_columns = []
        sql_parameters = []

        col_id = 0
        for c, v in row_columns.iteritems():
            sql_identifiers['col_%d' % col_id] = sql.Identifier(c)
            sql_where.append('{%s} = %%s' % ('col_%d' % col_id))
            sql_insert_columns.append('{%s}' % ('col_%d' % col_id))
            sql_parameters.append(v)
            col_id += 1

        cursor.execute("LOCK {schema}.{table}".format(schema=schema, table=table))

        cursor.execute(
            sql.SQL(
                "SELECT COUNT(*) FROM {schema}.{table} WHERE " + " AND ".join(sql_where)
            ).format(**sql_identifiers),
            sql_parameters
        )
        executed_query = cursor.query
        row_count = cursor.fetchone()['count']

        if row_count > 1:
            raise psycopg2.ProgrammingError('More than 1 one returned by selection query %s' % executed_query)

        changed = False
        if state == 'present' and row_count != 1:
            changed = True
        if state == 'absent' and row_count == 1:
            changed = True

        if module.check_mode or not changed:
            cursor.connection.rollback()
            module.exit_json(
                changed=changed,
                executed_query=executed_query
            )

        if state == 'present':
            cursor.execute(
                sql.SQL(
                    'INSERT INTO {schema}.{table} (' + ', '.join(sql_insert_columns) + ') ' +
                    'VALUES (' + ', '.join(['%s'] * len(sql_parameters)) + ')'
                ).format(**sql_identifiers),
                sql_parameters
            )
            executed_cmd = cursor.query
        else:
            cursor.execute(
                sql.SQL(
                    'DELETE FROM {schema}.{table} WHERE ' + ' AND '.join(sql_where)
                ).format(**sql_identifiers),
                sql_parameters
            )
            executed_cmd = cursor.query

        cursor.connection.commit()

        module.exit_json(
            changed=changed,
            executed_query=executed_query,
            executed_command=executed_cmd,
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
