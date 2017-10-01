def _table_exists_query():
    return """
        SELECT c.oid, n.nspname as "Schema",
          c.relname as "Name",
          pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE
          n.nspname ~ ('^(' || %s || ')$')
          AND c.relname ~ ('^(' || %s || ')$')
          AND c.relkind = 'r'
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1,2;
        """


def _table_columns_definition(cursor, table_oid):
    cursor.execute(
        """
        SELECT a.attname,
          pg_catalog.format_type(a.atttypid, a.atttypmod),
          (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
           FROM pg_catalog.pg_attrdef d
           WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
          a.attnotnull, a.attnum,
          (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
           WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
          NULL AS indexdef,
          NULL AS attfdwoptions
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum;
        """,
        (table_oid,)
    )
    return cursor.fetchall()


def _normalize_column_types(t):
    return t.lower()


def _compare_column(db_column, playbook_columns, diff):
    result = True
    column_found = False
    same_type = False
    same_null = False
    for c in playbook_columns:
        if db_column['attname'] == c['name']:
            column_found = True
            if db_column['format_type'] == _normalize_column_types(c['type']):
                same_type = True
            if 'null' in c.keys() and c['null'] is False and db_column['attnotnull']:
                same_null = True
            elif ('null' not in c.keys() or c['null'] is True) and not db_column['attnotnull']:
                same_null = True

    diff['found'] = column_found
    diff['type'] = same_type
    diff['null'] = same_null

    return result and column_found and same_type and same_null


def _get_primary_key(cursor, table_oid):
    cursor.execute(
        """
        SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
          pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
        FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
          LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
        WHERE c.oid = %s AND c.oid = i.indrelid AND i.indexrelid = c2.oid AND i.indisprimary = TRUE
        ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
        """,
        (table_oid,)
    )
    if cursor.rowcount != 1:
        return False
    return cursor.fetchone()['pg_get_constraintdef']


def _build_primary_key_def(columns):
    return 'PRIMARY KEY (' + ', '.join(columns) + ')'


def table_exists(cursor, schema, name):
    cursor.execute(_table_exists_query(), (schema, name))
    return cursor.rowcount == 1


def table_matches(cursor, schema, name, owner, columns, primary_key, diff):
    diff['exists'] = None
    diff['owner'] = None
    diff['playbook_columns'] = {}
    diff['existing_columns'] = {}
    diff['logs'] = {}
    diff['primary_key'] = None
    for c in columns:
        diff['playbook_columns'][c['name']] = None

    if not table_exists(cursor, schema, name):
        diff['exists'] = False
        return False
    diff['exists'] = True

    cursor.execute(_table_exists_query(), (schema, name))
    r = cursor.fetchone()
    table_oid = r['oid']

    diff['owner'] = r['Owner'] != owner and len(owner) > 0

    result = True
    for r in _table_columns_definition(cursor, table_oid):
        diff['existing_columns'][r['attname']] = None
        col_diff = {}
        col_comparison = _compare_column(r, columns, col_diff)
        if not col_comparison:
            if col_diff['found']:
                diff['existing_columns'][r['attname']] = False
                diff['playbook_columns'][r['attname']] = False
                diff['logs'][r['attname']] = col_diff
        else:
            diff['existing_columns'][r['attname']] = True
            diff['playbook_columns'][r['attname']] = True
        result = result and col_comparison

    current_primary_key = _get_primary_key(cursor, table_oid)
    if current_primary_key is False and len(primary_key) > 0:
        diff['primary_key'] = False
        result = False
    elif current_primary_key is not False and len(primary_key) == 0:
        diff['primary_key'] = None
        result = False
    elif current_primary_key != _build_primary_key_def(primary_key):
        diff['primary_key'] = False
        result = False
    else:
        diff['primary_key'] = True

    return result

