pgsql
=========

Provides four new ansible modules for Postgresql:
  - pgsql_table: ensure that a table is present (or absent) in database
  - pgsql_row: ensure that a row is present (or absent) in a table
  - pgsql_query: execute an arbitrary query in database and return results
  - pgsql_command: execute an arbitrary query in database


Requirements
------------

It requires psycopg2 installed as per Ansible's PostgreSQL modules: http://docs.ansible.com/ansible/latest/list_of_database_modules.html#postgresql

Role Variables
--------------

No variables are defined by the module

Dependencies
------------



Example Playbook
----------------

Including an example of how to use your role (for instance, with variables passed in as parameters) is always nice for users too:

    - hosts: servers
      tasks:
        - postgresql_table:
            database: acme
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
        
        - postgresql_row:
            database: acme
            table: config
            row:
              key: env
              value: production

        - postgresql_query:
            database: acme
            query: "SELECT * FROM config WHERE env = %(env)s
            parameters:
              env: production 

        - postgresql_command:
            database: acme
            command: "TRUNCATE logs"
      roles:
         - { role: pgsql }

License
-------

BSD

Author Information
------------------

Denis Gasparin <denis@gasparin.net>
http://www.gasparin.net
