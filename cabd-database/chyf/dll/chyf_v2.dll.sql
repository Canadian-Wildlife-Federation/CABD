--create extension postgis;
--create extension "uuid-ossp";

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
  nid varchar(32),
  ec_type smallint not null,
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
  nexus_type smallint not null ,
  bank_ecatchment_id uuid references chyf2.ecatchment(id),
  geometry geometry(POINT, 4617)
);

create table chyf2.eflowpath(
  id uuid not null primary key,
  nid varchar(32),
  ef_type smallint not null,
  ef_subtype smallint,
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
	last_accessed timestamp not null  default now(),
	CONSTRAINT vector_tile_cache_pkey PRIMARY KEY (key)
);


-- create code tables
CREATE TABLE chyf2.ef_type_codes(
	code smallint NOT NULL primary key, 
	name varchar(128) NOT NULL,
	description text NULL
);

CREATE TABLE chyf2.ef_subtype_codes(
	code smallint NOT NULL primary key, 
	name varchar(128) NOT NULL,
	description text NULL
);

CREATE TABLE chyf2.nexus_type_codes(
	code smallint NOT NULL primary key, 
	name varchar(128) NOT NULL,
	description text NULL
);

CREATE TABLE chyf2.ec_type_codes(
	code smallint NOT NULL primary key, 
	name varchar(128) NOT NULL,
	description text NULL
);
CREATE TABLE chyf2.ec_subtype_codes(
	code smallint NOT NULL primary key, 
	name varchar(128) NOT NULL,
	description text NULL
);

--populate code tables
INSERT INTO chyf2.ef_type_codes(code, name, description) VALUES
 (1, 'Reach', 'If the elementary flowpath represents an observed or inferred single-line flow. Includes Observed and Constructed for networkFlowType from the NHN, as well as flow through dams and potentially other types not present in the NHN.'),
 (2, 'Bank', 'Skeleton segment that connects a bank catchment to the flow network. This has no equivalent in the NHN.'),
 (3, 'Skeleton', 'A skeleton segment in a lake or double-line river that connects at both ends to other skeleton segments or to a reach flowpath. These are referred to as Inferred for networkFlowType in the NHN.'),
 (4, 'Infrastructure', 'An elementary flowpath representing flow contained in a conduit, such as a storm drain or a sanitary sewer. Conduits may be buried, at ground level, or elevated. This is assigned manually during preprocessing.');

INSERT INTO chyf2.ef_subtype_codes(code, name, description) VALUES
 (10, 'Observed', 'Reach flowpath subtype representing an observed stream flowpath. It is equivalent to Observed for networkFlowType for a Water Linear Flow in the NHN.'),
 (20, 'Inferred', 'Reach flowpath subtype representing a flowpath that appears to traverse the land but was not visible when mapped. It is equivalent to Constructed for networkFlowType for a Water Linear Flow in the NHN. Flow through dams that are assigned a Constructed value will require manual review.'),
 (99, 'Unspecified', 'Unknown, impossible to determine, or unrelated to the other existing subtypes. It is equivalent to Unknown or None for networkFlowType for a Water Linear Flow in the NHN.');

INSERT INTO chyf2.nexus_type_codes(code, name, description) VALUES
 (1, 'Headwater', 'Headwater flowpath start point. The start of an elementary flowpath, for which the start point does not intersect other flowpaths, lake boundaries, or double-line river boundaries.'),
 (2, 'Terminal Isolated', 'Terminal flowpath endpoint. The endpoint of an elementary flowpath that is not connected to any other flowpath at the endpoint. These are true sinks or the endpoint of a flowpath across a terminal lake.'),
 (3, 'Terminal Boundary', 'Terminal flowpath endpoint. The endpoint of an elementary flowpath that is not connected to any other flowpath at the endpoint. These represent the limit of what was visible when the hydrography was compiled and would generally flow into other water if additional data was included.'),
 (4, 'Flowpath', 'Flowpath hydro node. The junction of a single-line river with another single-line river or with the boundary of a double-line river or lake. These are defined by the endpoints of observed flowpaths.'),
 (5, 'Water', 'Water hydro node. The junction of a double-line river or lake with another double-line river or lake, as represented by the intersection of the incoming and outgoing flowpaths at that location.'),
 (6, 'Bank', 'Bank hydro node. A type of hydro node that is arbitrarily placed on the boundary of adjacent catchments to show flow or potential flow from one to the other, where no observed flowpaths exist to convey that information, and where the upper catchment is land based and the lower is water based (lake or double-line river).'),
 (7, 'Inferred', 'Inferred junction. These are required for the purpose of connectivity only. They are not hydro nodes.'),
 (99, 'Unknown', 'Unknown or impossible to determine');

INSERT INTO chyf2.ec_type_codes(code, name, description) VALUES
 (1, 'Reach', 'An elementary catchment that contains a section of a single-line stream. The contained stream will bisect the catchment, except in the case of headwater stream segments or terminal stream segments.'),
 (2, 'Bank', 'An elementary catchment that is adjacent to a waterbody and that drains directly into it. It does not contain a waterbody. For example, if two streams drain into a lake, the remnant area between the catchments for the two streams also drains into the lake; it defines a bank catchment.'),
 (3, 'Empty', 'An elementary catchment consisting of internally drained land that does not touch a waterbody. In 2D the linear ring defining its boundary does not surround any waterbodies.'),
 (4, 'Water', 'An elementary catchment with polygonal geometry representing (i) watercourses such as rivers that are sufficiently large to have been mapped with polygonal geometry, or (ii), static or tidal waterbodies such as lakes, estuaries, nearshore zones, and the ocean.'),
 (5, 'Built-up Area', 'Urban or industrial area, or area under construction. Such areas where constructed features may involve significant infrastructure developments that include conduits for the transfer of freshwater or waste water. 2D topologic rules will not apply in and around these areas, especially as related to infrastructure flowpaths, which in 2D may cross one another and not intersect. Also, such flowpaths will generally not drain the immediate area through which they pass. This is assigned manually during preprocessing where information on the extent of built-up areas is available.');

 
INSERT INTO chyf2.ec_subtype_codes(code, name, description) VALUES
 (10, 'Lake', 'A body of surface water. Equivalent to waterDefinition Lake and waterDefinition Reservoir in the NHN.'),
 (11, 'Great Lake', 'One of the Great Lakes of North America or other very large lakes.'),
 (12, 'Wastewater Pond', 'A pond or lagoon designed to contain wastewater for treatment. Equivalent to waterDefinition Liquid Waste in the NHN.'),
 (20, 'Estuary', 'A body of surface water at a freshwater - ocean water interface, typically characterized by tidal waters. Equivalent to waterDefinition Tidal River in the NHN'),
 (30, 'Nearshore Zone', 'The zone extending from the shore of a large waterbody (freshwater or the ocean), based on an arbitrary distance, bathymetry or littoral characteristics.'),
 (40, 'River', 'A body of surface water characterized by its flow. Equivalent to waterDefinition Watercourse in the NHN.'),
 (41, 'Canal', 'A body of surface water, participating in a hydrographic network, special due to its artificial origin. Ditches are also included here. Equivalent to waterDefinition Canal and Ditch in the NHN.'),
 (50, 'Buried Infrastructure', 'A body of subsurface flow contained in a conduit, such as a storm drain, a sanitary sewer, a flow through a dam or an industrial complex. Includes waterDefinition Conduit in the NHN.'),
 (90, 'Ocean', 'A large body of saline water that composes much of the earth’s hydrosphere and that is not situated inland. [After Wikipedia and Princeton] Seas and bays that extend to the open ocean are classed as ocean. The Caribbean and Mediterranean Seas and the Bay of Bengal are considered as ocean, whereas the Caspian Sea in Asia is a lake. Subdivisions of oceans are considered as water catchments.'),
 (99, 'Unknown', 'The subtype is not known or does not correspond to one of the known types listed above. Equivalent to waterDefinition None in the NHN.');
 
ALTER TABLE chyf2.eflowpath ADD FOREIGN KEY (ef_type) REFERENCES chyf2.ef_type_codes;
ALTER TABLE chyf2.eflowpath ADD FOREIGN KEY (ef_subtype) REFERENCES chyf2.ef_subtype_codes;
ALTER TABLE chyf2.ecatchment ADD FOREIGN KEY (ec_type) REFERENCES chyf2.ec_type_codes;
ALTER TABLE chyf2.ecatchment ADD FOREIGN KEY (ec_subtype) REFERENCES chyf2.ec_subtype_codes;
ALTER TABLE chyf2.nexus ADD FOREIGN KEY (nexus_type) REFERENCES chyf2.nexus_type_codes;


--geometry indexes
create index ecatchment_geometry_idx on chyf2.ecatchment using gist(geometry);
create index eflowpath_geometry_idx on chyf2.eflowpath using gist(geometry);
create index shoreline_geometry_idx on chyf2.shoreline using gist(geometry);
create index nexus_geometry_idx on chyf2.nexus using gist(geometry);

alter schema chyf2 owner to chyf;
ALTER TABLE chyf2.aoi OWNER TO chyf;
alter table chyf2.ec_subtype_codes OWNER TO chyf;
alter table chyf2.ec_type_codes OWNER TO chyf;
alter table chyf2.ecatchment OWNER TO chyf;
alter table chyf2.ecatchment_attributes OWNER TO chyf;
alter table chyf2.ef_subtype_codes OWNER TO chyf;
alter table chyf2.ef_type_codes OWNER TO chyf;
alter table chyf2.eflowpath OWNER TO chyf;
alter table chyf2.eflowpath_attributes OWNER TO chyf;
alter table chyf2.names OWNER TO chyf;
alter table chyf2.nexus OWNER TO chyf;
alter table chyf2.nexus_type_codes OWNER TO chyf;
alter table chyf2.shoreline OWNER TO chyf;
alter table chyf2.terminal_point OWNER TO chyf;
alter table chyf2.vector_tile_cache OWNER TO chyf;
	