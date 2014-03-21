do $$ begin

CREATE TABLE %(table)s (
  id bigserial PRIMARY KEY,
  q_name text not null check (length(q_name) > 0),
  %(columns)s,
  locked_at timestamptz,
  created_at timestamptz default now()
);

--NOTE: not compatible with pueuey
-- If json type is available, use it for the args column.
--perform * from pg_type where typname = 'json';
--if found then
--  alter table %(table)s alter column args type json using (args::json);
--end if;

end $$ language plpgsql;

create function queue_classic_notify() returns trigger as $$ begin
  perform pg_notify(new.q_name, '');
  return null;
end $$ language plpgsql;

create trigger queue_classic_notify
after insert on %(table)s
for each row
execute procedure queue_classic_notify();

CREATE INDEX idx_qc_on_name_only_unlocked ON %(table)s (q_name, id) WHERE locked_at IS NULL;
