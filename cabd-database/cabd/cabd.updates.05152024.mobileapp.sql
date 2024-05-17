
alter table cabd.feature_types add column community_data_table varchar;
alter table cabd.feature_types add column community_data_photo_fields varchar[];

update cabd.feature_types set 
  community_data_table = 'stream_crossings.stream_crossings_community_staging',
  community_data_photo_fields = '{"photo1", "photo2", "photo3"}' 
where type = 'stream_crossings';

update cabd.feature_types set 
  community_data_table = 'dams.dams_community_staging',
  community_data_photo_fields = '{"photo1", "photo2", "photo3"}' 
where type = 'dams';

create table cabd.community_data_raw(
  id uuid not null default uuid_generate_v4(),
  uploaded_datetime timestamp with time zone not null,
  data varchar not null,
  status varchar(16) not null default 'NEW',
  status_message varchar,
  warnings varchar[],
  primary key (id),
  CONSTRAINT status_value_ch CHECK (status IN ('NEW', 'PROCESSING', 'DONE', 'DONE_WARN'))
);

create table cabd.community_contact(
  user_id uuid not null default uuid_generate_v4(),
  username varchar not null,
  primary key (user_id)
);

create table stream_crossings.stream_crossings_community_staging(
  id uuid not null default uuid_generate_v4(),
  cabd_id uuid not null,
  user_id uuid not null references cabd.community_contact(user_id),
  uploaded_datetime timestamp with time zone not null,
  data jsonb,
  status varchar(16) not null default 'NEW',
  primary key (id),
  CONSTRAINT status_value_ch CHECK (status IN ('NEW', 'REVIEWED'))
);

create table dams.dams_community_staging(
  id uuid not null default uuid_generate_v4(),
  cabd_id uuid not null,
  user_id uuid not null references cabd.community_contact(user_id),
  uploaded_datetime timestamp with time zone not null,
  data jsonb,
  status varchar(16) not null default 'NEW',
  primary key (id),
  CONSTRAINT status_value_ch CHECK (status IN ('NEW', 'REVIEWED'))
);

