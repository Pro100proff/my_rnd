create table if not exists archive_indexes (
  index_id varchar primary key,
  active_directory varchar not null,
  tmp_directory varchar not null,
  target_file_size bigint not null,
  dir_created_at timestamp not null default now(),
  last_merge_at timestamp,
  total_bytes_merged bigint default 0
);

create table if not exists merge_batches (
  batch_id varchar primary key,
  index_id varchar not null,
  files text[] not null,
  status varchar not null,
  bytes_written bigint,
  created_at timestamp not null,
  completed_at timestamp
);

create table if not exists rotation_rules (
  index_id varchar not null,
  name varchar not null,
  type varchar not null,
  params jsonb not null,
  execution_order int not null,
  enabled boolean not null default true
);

create table if not exists deferred_deletes (
  id serial primary key,
  path varchar not null,
  delete_after timestamp not null,
  status varchar not null,
  error_message varchar,
  created_at timestamp not null,
  completed_at timestamp
);

insert into archive_indexes(index_id, active_directory, tmp_directory, target_file_size)
values ('archive-logs-tenant-42', '/data/archive-logs-tenant-42/active', '/tmp/archive-logs-tenant-42', 536870912)
on conflict do nothing;

insert into rotation_rules(index_id, name, type, params, execution_order, enabled) values
('archive-logs-tenant-42', 'drop_debug_logs', 'content_filter', '{"condition":"level = ''DEBUG''", "older_than_days":7}', 1, true),
('archive-logs-tenant-42', 'sample_info', 'sampling', '{"condition":"level = ''INFO''", "older_than_days":30, "keep_ratio":0.1}', 2, true),
('archive-logs-tenant-42', 'max_age_90_days', 'max_age_days', '{"days":90}', 3, true),
('archive-logs-tenant-42', 'max_size_5tb', 'max_total_size', '{"max_gb":5000}', 4, true),
('archive-logs-tenant-42', 'switch_dir', 'directory_switch', '{"switch_every_days":30, "grace_minutes":30}', 5, true)
on conflict do nothing;
