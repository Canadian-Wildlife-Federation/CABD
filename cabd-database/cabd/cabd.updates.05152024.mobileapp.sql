
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
  CONSTRAINT status_value_ch CHECK (status IN ('NEW', 'REJECTED', 'REVIEWED'))
);

create table dams.dams_community_staging(
  id uuid not null default uuid_generate_v4(),
  cabd_id uuid not null,
  user_id uuid not null references cabd.community_contact(user_id),
  uploaded_datetime timestamp with time zone not null,
  data jsonb,
  status varchar(16) not null default 'NEW',
  primary key (id),
  CONSTRAINT status_value_ch CHECK (status IN ('NEW', 'REJECTED',  'REVIEWED'))
);


alter table cabd.community_data_raw owner to cabd;
alter table stream_crossings.stream_crossings_community_staging owner to cabd;
alter table dams.dams_community_staging owner to cabd;
alter table cabd.community_contact owner to cabd;



--
-- support updates pending from the community staging table
-- as new community tables are added for other
-- feature types this will need to be updated as well
--
CREATE OR REPLACE VIEW cabd.updates_pending
AS 
SELECT cabd_id FROM cabd.dam_updates
union ALL
 SELECT cabd_id FROM cabd.fishway_updates
union ALL
 SELECT cabd_id FROM cabd.waterfall_updates
union ALL 
 select cabd_id from dams.dams_community_staging WHERE status = 'NEW'
union ALL 
 select cabd_id from stream_crossings.stream_crossings_community_staging WHERE status = 'NEW'


-- support rejected status for community features
alter table dams.dams_community_staging drop CONSTRAINT status_value_ch;
alter table dams.dams_community_staging add CONSTRAINT status_value_ch CHECK (status IN ('NEW', 'REJECTED', 'REVIEWED'));

alter table stream_crossings.stream_crossings_community_staging drop CONSTRAINT status_value_ch; 
alter table stream_crossings.stream_crossings_community_staging add CONSTRAINT status_value_ch CHECK (status IN ('NEW', 'REJECTED', 'REVIEWED'));
 
 
 
 -- ** bug fixes ** --
 
alter table cabd.vector_tile_cache alter column key type varchar(128);


CREATE OR REPLACE VIEW cabd.updates_pending
AS SELECT DISTINCT dam_updates.cabd_id
   FROM cabd.dam_updates
UNION ALL
 SELECT DISTINCT fishway_updates.cabd_id
   FROM cabd.fishway_updates
UNION ALL
 SELECT DISTINCT waterfall_updates.cabd_id
   FROM cabd.waterfall_updates
UNION ALL
 SELECT DISTINCT dams_community_staging.cabd_id
   FROM dams.dams_community_staging
  WHERE dams_community_staging.status::text = 'NEW'::text
UNION ALL
 SELECT DISTINCT stream_crossings_community_staging.cabd_id
   FROM stream_crossings.stream_crossings_community_staging
  WHERE stream_crossings_community_staging.status::text = 'NEW'::text;