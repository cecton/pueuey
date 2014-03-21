DROP FUNCTION IF EXISTS lock_head_%(table)s(tname varchar);
DROP FUNCTION IF EXISTS lock_head_%(table)s(q_name varchar, top_boundary integer);
DROP TABLE IF EXISTS %(table)s;
