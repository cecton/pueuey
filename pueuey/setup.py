import os

Root = os.path.dirname(__file__)
CreateTable = open(os.path.join(Root, 'sql', 'create_table.sql'), 'rb').read()
DropSqlFunctions = open(os.path.join(Root, 'sql', 'drop_ddl.sql'), 'rb').read()

def create(conn, table, columns, close=False):
    conn.execute(CreateTable % {'table': table, 'columns': columns})
    if close:
        conn.close()

def drop(conn, table, close=False):
    conn.execute(DropSqlFunctions % {'table': table})
    if close:
        conn.close()
