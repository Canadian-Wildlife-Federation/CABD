create schema waterfalls;

CREATE TABLE waterfalls.waterfall_complete_level_codes (
	code int2 NOT NULL, -- Code referencing the level of information available for the barrier structure.
	"name" varchar(32) NULL, -- The level of information available for the barrier structure.
	description text NULL,
	CONSTRAINT waterfall_complele_level_codes_pk PRIMARY KEY (code)
);
COMMENT ON TABLE waterfalls.waterfall_complete_level_codes IS 'Reference table for the level of information available for the waterfall barrier structure.';
COMMENT ON COLUMN waterfalls.waterfall_complete_level_codes.code IS 'Code referencing the level of information available for the barrier structure.';
COMMENT ON COLUMN waterfalls.waterfall_complete_level_codes."name" IS 'The level of information available for the barrier structure.';



CREATE TABLE waterfalls.waterfalls (
	cabd_id uuid NOT NULL, -- Unique identifier for each barrier point.
	fall_name_en varchar(512) NULL, -- English given or known name of the waterfall.
	fall_name_fr varchar(512) NULL, -- French given or known name of the waterfall.
	waterbody_name_en varchar(512) NULL, -- English name of waterbody in which the waterfall is recorded.
	waterbody_name_fr varchar(512) NULL, -- French name of waterbody in which the waterfall is recorded.
	watershed_group_code varchar(32) NULL,
	province_territory_code varchar(2) NOT NULL,
	nearest_municipality varchar(512) NULL, -- Name of nearest municipality (i.e., a city or town that has corporate status and local government).
	fall_height_m float4 NULL, -- Height of the waterfall in meters.
	capture_date date NULL, -- The capture date for a structure as documented in the original dataset, if provided.
	last_update date NULL, -- Most recent date of the data source used to create, revise or confirm the waterfall record.
	"comments" text NULL, -- Unstructured comments about the waterfall.
	data_source_id varchar(256) NULL, -- The unique id assigned to the dam record in the original data source.
	data_source varchar(256) NULL, -- The original data source from which the dam record was obtained.
	complete_level_code int2 NULL, -- Level of information available for the barrier structure.
	snapped_point geometry(POINT, 4326) NULL,
	original_point geometry(POINT, 4326) NULL,
	CONSTRAINT waterfalls_pk PRIMARY KEY (cabd_id)
);
CREATE INDEX waterfalls_point_idx ON waterfalls.waterfalls USING gist (snapped_point);
COMMENT ON TABLE waterfalls.waterfalls IS 'Contains spatial point representation of waterfalls and their attributes.';
COMMENT ON COLUMN waterfalls.waterfalls.cabd_id IS 'Unique identifier for each barrier point.';
COMMENT ON COLUMN waterfalls.waterfalls.fall_name_en IS 'English given or known name of the waterfall.';
COMMENT ON COLUMN waterfalls.waterfalls.fall_name_fr IS 'French given or known name of the waterfall.';
COMMENT ON COLUMN waterfalls.waterfalls.waterbody_name_en IS 'English name of waterbody in which the waterfall is recorded.';
COMMENT ON COLUMN waterfalls.waterfalls.waterbody_name_fr IS 'French name of waterbody in which the waterfall is recorded.';
COMMENT ON COLUMN waterfalls.waterfalls.nearest_municipality IS 'Name of nearest municipality (i.e., a city or town that has corporate status and local government).';
COMMENT ON COLUMN waterfalls.waterfalls.fall_height_m IS 'Height of the waterfall in meters.';
COMMENT ON COLUMN waterfalls.waterfalls.capture_date IS 'The capture date for a structure as documented in the original dataset, if provided.';
COMMENT ON COLUMN waterfalls.waterfalls.last_update IS 'Most recent date of the data source used to create, revise or confirm the waterfall record.';
COMMENT ON COLUMN waterfalls.waterfalls."comments" IS 'Unstructured comments about the waterfall.';
COMMENT ON COLUMN waterfalls.waterfalls.data_source_id IS 'The unique id assigned to the dam record in the original data source.';
COMMENT ON COLUMN waterfalls.waterfalls.data_source IS 'The original data source from which the dam record was obtained.';
COMMENT ON COLUMN waterfalls.waterfalls.complete_level_code IS 'Level of information available for the barrier structure.';

ALTER TABLE waterfalls.waterfalls ADD CONSTRAINT waterfalls_fk_1 FOREIGN KEY (province_territory_code) REFERENCES cabd.province_territory_codes(code);
ALTER TABLE waterfalls.waterfalls ADD CONSTRAINT waterfalls_fk_2 FOREIGN KEY (complete_level_code) REFERENCES waterfalls.waterfall_complete_level_codes(code);
ALTER TABLE waterfalls.waterfalls ADD CONSTRAINT waterfalls_fk_3 FOREIGN KEY (watershed_group_code) REFERENCES cabd.watershed_groups(code);


INSERT INTO waterfalls.waterfall_complete_level_codes (code,"name",description) VALUES
	 (1,'Location Only',' Only location is known '),
	 (2,'Complete Unknown Barrier','Location and height is known - unknown if structure is a barrier to fish '),
	 (3,'Complete Confirmed Barrier','Location and height is known - confirmed to be a barrier to fish  ');
	 

	 
CREATE OR REPLACE VIEW cabd.waterfalls_view
AS SELECT w.cabd_id,
    'waterfalls'::text AS feature_type,
    st_y(w.snapped_point) as latitude,
    st_x(w.snapped_point) as longitude,
    w.fall_name_en,
    w.fall_name_fr,
    w.waterbody_name_en,
    w.waterbody_name_fr,
    w.watershed_group_code,
    wg.name AS watershed_group_name,
    w.province_territory_code,
    pt.name AS province_territory,
    w.nearest_municipality,
    w.fall_height_m,
    w.capture_date,
    w.last_update,
    w.comments,
    w.complete_level_code,
    cl.name AS complete_level,
    w.data_source_id,
    w.data_source,
    w.snapped_point
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = w.watershed_group_code::text;
     
     

DELETE FROM cabd.feature_types where type = 'waterfalls';
DELETE FROM cabd.feature_type_metadata where view_name = 'cabd.waterfalls_view';

INSERT INTO cabd.feature_types ("type",data_view) VALUES
	 ('waterfalls','cabd.waterfalls_view');
	 
INSERT INTO cabd.feature_type_metadata (view_name,field_name,"name",description,is_link) VALUES
	 ('cabd.waterfalls_view','fall_name_en','Fall Name (English)','English given or known name of the waterfall.',false),
	 ('cabd.waterfalls_view','latitude','Latitude','Latitude of point location of the waterfall in decimal degrees; the point location is only an approximation of the actual waterfall location.',false),
	 ('cabd.waterfalls_view','longitude','Longitude','Longitude of point location of the waterfall in decimal degrees; the point location is only an approximation of the actual waterfall location.',false),
	 ('cabd.waterfalls_view','fall_name_fr','Fall Name (French)','French given or known name of the waterfall.',false),
	 ('cabd.waterfalls_view','waterbody_name_en','Waterbody Name (English)','English name of waterbody in which the waterfall is recorded.',false),
	 ('cabd.waterfalls_view','waterbody_name_fr','Waterbody Name (French)','French name of waterbody in which the waterfall is recorded.',false),
	 ('cabd.waterfalls_view','watershed_group_code','Watershed Group Code',NULL,false),
	 ('cabd.waterfalls_view','watershed_group_name','Watershed Group Name',NULL,false),
	 ('cabd.waterfalls_view','province_territory_code','Province/Territory Code',NULL,false),
	 ('cabd.waterfalls_view','province_territory','Province/Territory Name',NULL,false),
	 ('cabd.waterfalls_view','fall_height_m','Fall Height (m)','Height of the waterfall in meters.',false),
	 ('cabd.waterfalls_view','capture_date','Capture Date','The capture date for a structure as documented in the original dataset, if provided.',false),
	 ('cabd.waterfalls_view','last_update','Last Update Date','Most recent date of the data source used to create, revise or confirm the waterfall record.',false),
	 ('cabd.waterfalls_view','comments','Comments','Unstructured comments about the waterfall.',false),
	 ('cabd.waterfalls_view','complete_level_code','Completeness Level Code',NULL,false),
	 ('cabd.waterfalls_view','complete_level','Completeness Level Name','Level of information available for the barrier structure.',false),
	 ('cabd.waterfalls_view','data_source_id','Data Source Identifier',NULL,false),
	 ('cabd.waterfalls_view','data_source','Data Source',NULL,false),
	 ('cabd.waterfalls_view','point','Location',NULL,false),
	 ('cabd.waterfalls_view','nearest_municipality','Nearest Municipality','Name of nearest municipality (i.e., a city or town that has corporate status and local government).',false),
	 ('cabd.waterfalls_view','feature_type','Feature Type',NULL,false),
	 ('cabd.waterfalls_view','cabd_id','Barrier Identifier','Unique identifier for waterfall',false);     