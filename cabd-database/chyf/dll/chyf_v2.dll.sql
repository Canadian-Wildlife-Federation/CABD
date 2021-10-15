drop schema if exists chyf2 cascade;
create schema chyf2;

create table chyf2.aoi(
  id uuid not null primary key,
  short_name varchar unique,
  full_name varchar,  
  geometry geometry(POLYGON, 4617)
);

create table chyf2.shoreline(
  id uuid not null primary key,
  aoi_id uuid not null references chyf2.aoi(id),
  geometry geometry(LINESTRING, 4617) not null
);
create index shoreline_aoi_id_idx on chyf2.shoreline(aoi_id);

create table chyf2.names(
  name_id uuid not null primary key,
  name_en varchar,
  name_fr varchar,
  cgndb_id uuid
);

create table chyf2.terminal_point(
  id uuid not null primary key,
  aoi_id uuid not null references chyf2.aoi(id),
  flow_direction smallint not null check (flow_direction in (1,2)),  
  geometry geometry(POINT, 4617) not null
);
create index terminal_point_aoi_id_idx on chyf2.terminal_point(aoi_id);

create table chyf2.ecatchment(
  id uuid not null primary key,
  ec_type smallint not null check (ec_type in(1,2,3,4,5)),
  ec_subtype smallint,
  area double precision not null,
  aoi_id uuid not null references chyf2.aoi(id),
  name_id uuid references chyf2.names(name_id),
  is_reservoir boolean,
  geometry geometry(POLYGON, 4617) not null
);
create index ecatchment_aoi_id_idx on chyf2.ecatchment (aoi_id);
  
create table chyf2.nexus(
  id uuid not null primary key,
  nexus_type smallint not null check (nexus_type in (1,2,3,4,5,6,7,99)),
  bank_ecatchment_id uuid references chyf2.ecatchment(id),
  geometry geometry(POINT, 4617)
);

create table chyf2.eflowpath(
  id uuid not null primary key,
  ef_type smallint not null check (ef_type in (1,2,3,4)),
  ef_subtype smallint check (ef_subtype in (10, 20, 99)),
  rank smallint not null,
  length double precision not null,
  name_id uuid references chyf2.names(name_id),
  aoi_id uuid references chyf2.aoi(id),
  from_nexus_id uuid references chyf2.nexus(id),
  to_nexus_id uuid references chyf2.nexus(id),
  ecatchment_id uuid references chyf2.ecatchment(id),
  geometry geometry(LINESTRING, 4617) not null
);
create index eflowpath_aoi_id_idx on chyf2.eflowpath(aoi_id);
create index eflowpath_from_nexus_id_idx on chyf2.eflowpath(from_nexus_id);
create index eflowpath_to_nexus_id_idx on chyf2.eflowpath(to_nexus_id);


create table chyf2.eflowpath_attributes(
  id uuid not null primary key references chyf2.eflowpath(id),
  strahler_order integer,
  horton_order integer,
  hack_order integer
);


create table chyf2.ecatchment_attributes(
  id uuid not null primary key references chyf2.ecatchment(id)
);

CREATE TABLE chyf2.vector_tile_cache (
	"key" varchar(32) NOT NULL,
	tile bytea NULL,
	CONSTRAINT vector_tile_cache_pkey PRIMARY KEY (key)
);
