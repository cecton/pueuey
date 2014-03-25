import os

from conn_adapter import ConnAdapter


Root = os.path.dirname(__file__)
SqlFunctions = os.path.join(Root, 'sql', 'ddl.sql')
CreateTable = os.path.join(Root, 'sql', 'create_table.sql')
DropSqlFunctions = os.path.join(Root, 'sql', 'drop_ddl.sql')

def create(connection=None):
    conn_adapter = ConnAdapter(connection)
    conn_adapter.execute(open(CreateTable).read())
    conn_adapter.execute(open(SqlFunctions).read())
    if connection is None:
        conn_adapter.disconnect()

def drop(connection=None):
    conn_adapter = ConnAdapter(connection)
    conn_adapter.execute(open(DropSqlFunctions).read())
    if connection is None:
        conn_adapter.disconnect()
