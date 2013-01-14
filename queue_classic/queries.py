import os

__all__ = ['Queries']


class Queries(object):
    def __init__(self, conn, table):
        self.conn = conn
        self.table = table

    def insert(self, q_name, row, chan=None):
        #TODO: convert dict values who are actual dicts to JSON?
        row = dict(row, q_name=q_name)
        curs = self.conn.cursor()
        curs.execute(
            'INSERT INTO "%s" (%s)' % (self.table, ",".join(row.keys()))
            +" VALUES ("+",".join('%s' for i in row.values())+")"
            +" RETURNING id", row.values())
        if chan: self.conn.notify(chan)
        return tuple(curs)[0][0]

    def lock_head(self, q_name, top_bound=None, columns=None):
        if top_bound is None:
            top_bound = os.environ.get('QC_TOP_BOUND', 9)
        if columns is None:
            columns = '*'
        curs = self.conn.execute(
"SELECT %s FROM lock_head_%s" % (
    (", ".join('"%s"' % c for c in columns) if hasattr(columns, '__iter__')
        else columns),
    self.table)
+"(%s, %s)", [q_name, top_bound])
        return curs.fetchone() or None

    def count(self, q_name=None):
        if q_name:
            r = self.conn.execute('SELECT COUNT(*) FROM "%s"' % self.table)
        else:
            r = self.conn.execute('SELECT COUNT(*) FROM "%s"' % self.table
                                  +" WHERE q_name = %s", q_name)
        return int(r["count"])

    def delete(self, id):
        return self.conn.execute('DELETE FROM "%s" WHERE id = %d' % (self.table, id))

    def delete_all(self, q_name=None):
        if q_name:
            return self.conn.execute('DELETE FROM "%s"' % self.table)
        else:
            return self.conn.execute('DELETE FROM "%s"' % self.table
                                     +" WHERE q_name = %s", q_name)
