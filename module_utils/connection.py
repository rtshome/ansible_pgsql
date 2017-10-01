import psycopg2
import psycopg2.extras


def prepare_connection_params(params):
    params_map = {
        "login_host":"host",
        "login_user":"user",
        "login_password":"password",
        "port":"port"
    }
    kw = dict((params_map[k], v) for (k, v) in params.items() if k in params_map and v != '')

    # If a login_unix_socket is specified, incorporate it here.
    is_localhost = "host" not in kw or kw["host"] == "" or kw["host"] == "localhost"
    if is_localhost and params["login_unix_socket"] != "":
        kw["host"] = params["login_unix_socket"]

    return kw


def connect(database, params):
    db_connection = psycopg2.connect(database=database, **params)
    # Enable autocommit so we can create databases
    if psycopg2.__version__ >= '2.4.2':
        db_connection.autocommit = True
    else:
        db_connection.set_isolation_level(psycopg2
                                          .extensions
                                          .ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cursor
