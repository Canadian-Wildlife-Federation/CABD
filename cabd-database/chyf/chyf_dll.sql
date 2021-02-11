CREATE SCHEMA chyf;

CREATE TABLE chyf.working_limit (
	region_id varchar(23) not null,
	geometry geometry(POLYGON, 4326) not NULL,
	primary key (region_id)
);
CREATE INDEX working_limit_geometry_idx ON chyf.working_limit USING gist (geometry);

CREATE TABLE chyf.elementary_catchment (
	region_id varchar(32) not null,
	area float8 NOT NULL,
	d2w2d_mean float8 NULL,
	d2w2d_max float8 NULL,
	elv_min float8 NULL,
	elv_max float8 NULL,
	elv_mean float8 NULL,
	slope_min float8 NULL,
	slope_max float8 NULL,
	slope_mean float8 NULL,
	north_pct float8 NULL,
	south_pct float8 NULL,
	east_pct float8 NULL,
	west_pct float8 NULL,
	flat_pct float8 NULL,
	geometry geometry(POLYGON, 4326) NULL
);
CREATE INDEX elementary_catchment_geometry_idx ON chyf.elementary_catchment USING gist (geometry);
ALTER TABLE chyf.elementary_catchment ADD FOREIGN KEY (region_id) references chyf.working_limit(region_id);


CREATE TABLE chyf.flowpath (
	region_id varchar(32) not null,
	"type" varchar NOT NULL,
	"rank" varchar NOT NULL,
	length float8 NOT NULL,
	"name" varchar NULL,
	nameid varchar NULL,
	geometry geometry(LINESTRING, 4326) NOT NULL,
	CONSTRAINT flowpath_check CHECK (((type)::text = ANY ((ARRAY['Observed'::character varying, 'Constructed'::character varying, 'Bank'::character varying, 'Inferred'::character varying])::text[]))),
	CONSTRAINT flowpath_check_1 CHECK (((rank)::text = ANY ((ARRAY['Primary'::character varying, 'Secondary'::character varying])::text[])))
);
CREATE INDEX flowpath_geometry_idx ON chyf.flowpath USING gist (geometry);
ALTER TABLE chyf.flowpath ADD FOREIGN KEY (region_id) references chyf.working_limit(region_id);

CREATE TABLE chyf.waterbody (
	region_id varchar(32) not null,
	area float8 NOT NULL,
	definition int4 NOT NULL, -- Classifies the waterbody into subtype.  Valid values (4-Lake, 9-Pond, 6-River, 1-Canal, -1-Unknown)
	geometry geometry(POLYGON, 4326) NOT NULL,
	CONSTRAINT waterbody_check CHECK ((definition = ANY (ARRAY[4, 9, 6, 1, -1])))
);
CREATE INDEX waterbody_geometry_idx ON chyf.waterbody USING gist (geometry);
ALTER TABLE chyf.waterbody ADD FOREIGN KEY (region_id)references chyf.working_limit(region_id);

COMMENT ON COLUMN chyf.waterbody.definition IS 'Classifies the waterbody into subtype.  Valid values (4-Lake, 9-Pond, 6-River, 1-Canal, -1-Unknown)';

drop table chyf.vector_tile_cache;
create table chyf.vector_tile_cache(key varchar(32), tile bytea, primary key(key));


