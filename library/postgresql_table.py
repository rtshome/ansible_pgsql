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
    from module_utils.table import *
except:
    pass

DOCUMENTATION = '''
---
module: postgresql_table

short_description: Add or remove a table in a PostGreSQL database

version_added: "2.3"

description:
    - "Add or remove a table in a PostGreSQL database"

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
    name:
        description:
            - Name of the table
        required: true
    schema:
        description:
            - Schema of the table
    owner:
        description:
            - Owner of the table
    port:
        description:
            - Database port to connect to.
        default: 5432
    state:
        description:
            - The table state
        default: present
        choices:
            - present
            - absent
    columns:
        description:
            - List of objects with name, type and null
        required: true
    primary_key:
        description:
            - List with column names composing the primary key

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
# Create a table in my_app database named "config" with two columns, key and value. Key is the primary key
- postgresql_table:
    database: my_app
    name: config
    state: present
    columns:
      - {
        name: key,
        type: text,
        null: False
      }
      - {
        name: value,
        type: text,
        null: False
      }
    primary_key:
      - key

# Ensure that the config table is not present
- postgresql_table:
    database: my_app
    name: config
    state: absent

'''

RETURN = '''
table:
    description: Name of the table
    type: str
schema:
    description: Schema of the table
    type: str
owner:
    description: Owner of the table
    type: str
differences:
    description: Dictionary containing the differences between the previous table and the updated one
columns:
    description: List containing the columns of the created table
logs:
    description: List with logs of the operations done by the module
'''


def run_module():
    module_args = dict(
        login_user=dict(default="postgres"),
        login_password=dict(default="", no_log=True),
        login_host=dict(default=""),
        login_unix_socket=dict(default=""),
        port=dict(default="5432"),
        name=dict(required=True),
        schema=dict(default="public"),
        owner=dict(default=""),
        database=dict(default="postgres"),
        state=dict(default="present", choices=["absent", "present"]),
        columns=dict(default=[]),
        primary_key=dict(default=[])
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    columns = ast.literal_eval(module.params["columns"])
    database = module.params["database"]
    name = module.params["name"]
    owner = module.params["owner"]
    primary_key = ast.literal_eval(module.params["primary_key"])
    schema = module.params["schema"]
    state = module.params["state"]

    if not postgresqldb_found:
        module.fail_json(msg="the python psycopg2 module is required")

    idx = 1
    for col in columns:
        if 'name' not in col.keys():
            module.fail_json(msg="Missing name in column definition number %d" % idx)

        if 'type' not in col.keys():
            module.fail_json(msg="Missing type in column definition number %d" % idx)

        if 'null' in col.keys() and col['null'] not in [True, False]:
            module.fail_json(msg="Column [%s] null key should be a boolean value" % col['name'], a=col)

        idx += 1

    if state == "present" and len(columns) == 0:
        module.fail_json(msg="No columns given for table [%s.%s]" % (schema, name))

    cursor = None
    try:
        cursor = connect(database, prepare_connection_params(module.params))
        diff = {}
        table_checks = table_matches(cursor, schema, name, owner, columns, primary_key, diff)
        logs = []

        if module.check_mode:
            if state == "absent":
                changed = diff['exists']
            else:
                # state == "present"
                changed = not table_checks

            module.exit_json(
                changed=changed,
                table=name,
                schema=schema,
                owner=owner,
                differences=diff,
                columns=columns
            )

        cursor.connection.autocommit = False
        changed = False

        if state == "absent" and diff['exists']:
            cursor.execute(
                sql.SQL("DROP TABLE {schema}.{name}").format(
                    schema=sql.Identifier(schema),
                    name=sql.Identifier(name)
                )
            )
            logs.append("drop table")
            changed = True

        if state == "present":
            if not diff['exists']:
                cursor.execute(
                    sql.SQL("CREATE TABLE {schema}.{name} (__dummy__field__ TEXT)").format(
                        schema=sql.Identifier(schema),
                        name=sql.Identifier(name)
                    )
                )
                logs.append("exists")
                changed = True
            else:
                cursor.execute(
                    sql.SQL("ALTER TABLE {schema}.{name} ADD COLUMN __dummy__field__ TEXT").format(
                        schema=sql.Identifier(schema),
                        name=sql.Identifier(name)
                    )
                )

            if diff['owner']:
                changed = True
                cursor.execute(
                    sql.SQL("ALTER TABLE {schema}.{name} OWNER TO {owner}").format(
                        schema=sql.Identifier(schema),
                        name=sql.Identifier(name),
                        owner=sql.Identifier(owner)
                    )
                )

            for col_to_drop, col_status in diff['existing_columns'].iteritems():
                if col_status is not True:
                    cursor.execute(
                        sql.SQL("ALTER TABLE {schema}.{name} DROP COLUMN {col}").format(
                            schema=sql.Identifier(schema),
                            name=sql.Identifier(name),
                            col=sql.Identifier(col_to_drop)
                        )
                    )
                    logs.append("drop " + col_to_drop)
                    changed = True

            for col in columns:
                col_status = diff['playbook_columns'][col['name']]
                if col_status is not True:
                    cursor.execute(
                        sql.SQL(
                            "ALTER TABLE {schema}.{name} ADD COLUMN {col} %s %s" %
                            (col['type'], 'NOT NULL' if 'null' in col.keys() and col['null'] is False else '')
                        ).format(
                            schema=sql.Identifier(schema),
                            name=sql.Identifier(name),
                            col=sql.Identifier(col['name'])
                        )
                    )
                    logs.append("add " + col['name'])
                    changed = True

            if diff['primary_key'] is not True:
                changed = diff['primary_key'] is None
                cursor.execute(
                    sql.SQL("ALTER TABLE {schema}.{name} DROP CONSTRAINT IF EXISTS {pkname}").format(
                        schema=sql.Identifier(schema),
                        name=sql.Identifier(name),
                        pkname=sql.Identifier(name + "_pkey")
                    )
                )

                if len(primary_key) > 0:
                    changed = True
                    _pk = map(lambda c: sql.Identifier(c), primary_key)
                    cursor.execute(
                        sql.SQL("ALTER TABLE {schema}.{name} ADD PRIMARY KEY ({pkey})").format(
                            schema=sql.Identifier(schema),
                            name=sql.Identifier(name),
                            pkey=sql.SQL(', ').join(_pk)
                        )
                    )
                    logs.append("add primary key")

            cursor.execute(
                sql.SQL("ALTER TABLE {schema}.{name} DROP COLUMN IF EXISTS __dummy__field__").format(
                        schema=sql.Identifier(schema),
                        name=sql.Identifier(name)
                )
            )

        cursor.connection.commit()

        module.exit_json(
            changed=changed,
            table=name,
            schema=schema,
            owner=owner,
            differences=diff,
            columns=columns,
            logs=logs
        )

    except psycopg2.DatabaseError:
        if cursor:
            cursor.connection.rollback()
        e = get_exception()
        module.fail_json(msg="database error: %s" % to_native(e), exception=traceback.format_exc())

if __name__ == '__main__':
    run_module()
