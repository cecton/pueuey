import os

Root = os.path.dirname(__file__)
SqlFunctions = os.path.join(Root, 'sql', 'ddl.sql')
CreateTable = os.path.join(Root, 'sql', 'create_table.sql')
DropSqlFunctions = os.path.join(Root, 'sql', 'drop_ddl.sql')

def create(conn, table, columns, close=False):
    conn.execute(open(CreateTable).read()
        % {'table': table, 'columns': columns})
    conn.execute(open(SqlFunctions).read()
        % {'table': table})
    if close:
        conn.close()

def drop(conn, table, close=False):
    conn.execute(open(DropSqlFunctions).read()
        % {'table': table})
    if close:
        conn.close()
