import os

Root = os.path.dirname(__file__)
SqlFunctions = os.path.join(Root, 'sql', 'ddl.sql')
CreateTable = os.path.join(Root, 'sql', 'create_table.sql')
DropSqlFunctions = os.path.join(Root, 'sql', 'drop_ddl.sql')

def create(conn, close=False):
    conn.execute(open(CreateTable).read())
    conn.execute(open(SqlFunctions).read())
    if close:
        conn.close()

def drop(conn, close=False):
    conn.execute(open(DropSqlFunctions).read())
    if close:
        conn.close()
