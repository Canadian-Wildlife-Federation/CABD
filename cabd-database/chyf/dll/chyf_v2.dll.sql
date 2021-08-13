drop schema if exists chyf cascade;
create schema chyf;

create table chyf.aoi(
  id uuid not null primary key,
  short_name varchar unique,
  full_name varchar,  
  geometry geometry(POLYGON, 4617)
);

create table chyf.shoreline(
  id uuid not null primary key,
  aoi_id uuid not null references chyf.aoi(id),
  geometry geometry(LINESTRING, 4617) not null
);

create table chyf.names(
  id uuid not null primary key,
  name_en varchar,
  name_fr varchar,
  cgndb_id uuid
);

create table chyf.terminal_point(
  id uuid not null primary key,
  aoi_id uuid not null references chyf.aoi(id),
  flow_direction smallint not null check (flow_direction in (1,2)),  
  geometry geometry(POINT, 4617) not null
);

create table chyf.ecatchment(
  id uuid not null primary key,
  ec_type smallint not null check (ec_type in(1,2,3,4,5)),
  ec_subtype smallint,
  area double precision not null,
  aoi_id uuid not null references chyf.aoi(id),
  name_id uuid references chyf.names(id),
  geometry geometry(POLYGON, 4617) not null
);
  
create table chyf.nexus(
  id uuid not null primary key,
  nexus_type smallint not null check (nexus_type in (1,2,3,4,5,6,7,99)),
  bank_ecatchment_id uuid references chyf.ecatchment(id),
  geometry geometry(POINT, 4617)
);

create table chyf.eflowpath(
  id uuid not null primary key,
  ef_type smallint not null check (ef_type in (1,2,3,4)),
  ef_subtype smallint,
  rank smallint not null,
  length double precision not null,
  name_id uuid references chyf.names(id),
  aoi_id uuid references chyf.aoi(id),
  from_nexus_id uuid references chyf.nexus(id),
  to_nexus_id uuid references chyf.nexus(id),
  ecatchment_id uuid references chyf.ecatchment(id),
  geometry geometry(LINESTRING, 4617) not null
);


create table chyf.eflowpath_attributes(
  id uuid not null primary key references chyf.eflowpath(id),
  strahler_order integer,
  horton_order integer,
  hack_order integer
);


CREATE TABLE chyf.vector_tile_cache (
	"key" varchar(32) NOT NULL,
	tile bytea NULL,
	CONSTRAINT vector_tile_cache_pkey PRIMARY KEY (key)
);
