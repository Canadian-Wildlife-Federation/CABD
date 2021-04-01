drop schema if exists fishways cascade;
create schema fishways;

CREATE TABLE fishways.entrance_position_codes (
	code int2 NOT NULL,
	name varchar(32) NOT NULL,
	description text NULL,
	CONSTRAINT fishpass_entrance_position_codes_pk PRIMARY KEY (code)
);


CREATE TABLE fishways.entrance_location_codes (
	code int2 NOT NULL,
	"name" varchar(32) NOT NULL,
	description text NULL,
	CONSTRAINT fishhpass_entrance_type_codes_pk PRIMARY KEY (code)
);

CREATE TABLE fishways.fishway_complete_level_codes (
	code int2 NOT NULL,
	"name" varchar(32) NULL,
	description text NULL,
	CONSTRAINT fishway_complele_level_codes_pk PRIMARY KEY (code)
);


CREATE TABLE fishways.fishways (
	cabd_id uuid NOT NULL,
	dam_id uuid NULL,
	dam_name_en varchar(512) NULL,
	dam_name_fr varchar(512) NULL,	
	waterbody_name_en varchar(512) NULL,
	waterbody_name_fr varchar(512) NULL,	
	river_name_en varchar(512) NULL,
	river_name_fr varchar(512) NULL,	
	watershed_group_code varchar(32) NULL,
	nhn_workunit_id varchar(7) NULL,	
	province_territory_code varchar(2) NULL,
	nearest_municipality varchar(512) NULL,	
	fishpass_type_code int2 NOT NULL,
	monitoring_equipment text NULL,
	architect text NULL,
	contracted_by text NULL,
	constructed_by text NULL,
	plans_held_by text NULL,
	purpose text NULL,
	designed_on_biology bool NULL,
	length_m float4 NULL,
	elevation_m float4 NULL,
	inclination_pct float4 NULL,
	depth_m float4 NULL,	
	entrance_location_code int2 NULL,
	entrance_position_code int2 NULL,	
	modified boolean NULL,
	modification_year int2 NULL,
	modification_purpose text NULL,
	year_constructed int2 NULL,
	operated_by text NULL,
	operation_period text NULL,
	has_evaluating_studies boolean NULL,
	nature_of_evaluation_studies text NULL,	
	engineering_notes text NULL,
	operating_notes text NULL,
	mean_fishway_velocity_ms float4 NULL,
	max_fishway_velocity_ms float4 NULL,
	estimate_of_attraction_pct float4 NULL,
	estimate_of_passage_success_pct float4 NULL,
	fishway_reference_id VARCHAR(256) NULL,
	data_source_id varchar(256) null,
	data_source varchar(256) null,
	complete_level_code int2 null,
	original_point geometry(POINT, 4326) NULL,
	snapped_point geometry(POINT, 4326) NULL,
	CONSTRAINT fishpass_pk PRIMARY KEY (cabd_id),
	CONSTRAINT fishpass_fk FOREIGN KEY (fishpass_type_code) REFERENCES cabd.upstream_passage_type_codes(code),
	CONSTRAINT fishpass_fk_1 FOREIGN KEY (province_territory_code) REFERENCES cabd.province_territory_codes(code),
	CONSTRAINT fishpass_fk_3 FOREIGN KEY (entrance_location_code) REFERENCES fishways.entrance_location_codes(code),
	CONSTRAINT fishpass_fk_4 FOREIGN KEY (entrance_position_code) REFERENCES fishways.entrance_position_codes(code),
	CONSTRAINT fishways_fk FOREIGN KEY (dam_id) REFERENCES dams.dams_medium_large(cabd_id),
	CONSTRAINT fishways_fk_7 FOREIGN KEY (watershed_group_code) REFERENCES cabd.watershed_groups(code),
	CONSTRAINT fishways_fk_8 FOREIGN KEY (complete_level_code) REFERENCES fishways.fishway_complete_level_codes(code)
);


CREATE TABLE fishways.species_mapping (
	fishway_id uuid NOT NULL,
	species_id uuid NOT NULL,
	known_to_use bool NOT NULL,
	CONSTRAINT fishpass_species_pk PRIMARY KEY (fishway_id, species_id),
	CONSTRAINT fishpass_species_fk FOREIGN KEY (fishway_id) REFERENCES fishways.fishways(cabd_id),
	CONSTRAINT fishpass_species_fk_1 FOREIGN KEY (species_id) REFERENCES cabd.fish_species(id)
);


INSERT INTO fishways.fishway_complete_level_codes (code,"name",description) VALUES
	 (1,'Minimal',NULL),
	 (2,'Moderate',NULL),
	 (3,'Complete',NULL);
	 
INSERT INTO fishways.entrance_position_codes (code,"name",description) VALUES
	 (1,'Bottom',NULL),
	 (2,'Surface',NULL),
	 (3,'Bottom and Surface',NULL),
	 (4,'Mid-column',NULL);

INSERT INTO fishways.entrance_location_codes (code,"name",description) VALUES
	 (1,'Midstream',NULL),
	 (2,'Bank',NULL);
	  
CREATE OR REPLACE VIEW cabd.fishways_view
AS SELECT d.cabd_id,
    'fishways'::text AS feature_type,
    st_y(d.snapped_point) AS latitude,
    st_x(d.snapped_point) AS longitude,
    d.dam_id,
    d.dam_name_en,
    d.dam_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.river_name_en,
    d.river_name_fr,
    d.watershed_group_code,
    wg.name AS watershed_group_name,
    d.nhn_workunit_id,
    d.province_territory_code,
    pt.name AS province_territory,
    d.nearest_municipality,	
    d.fishpass_type_code,
    tc.name as fishpass_type,
    d.monitoring_equipment,
    d.architect,
    d.contracted_by,
    d.constructed_by,
    d.plans_held_by,
    d.purpose,
    d.designed_on_biology,
    d.length_m,
    d.elevation_m,
    d.inclination_pct,
    d.depth_m,
    d.entrance_location_code,
    elc.name as entrance_location,
    d.entrance_position_code,	
    epc as entrance_position, 
    d.modified,
    d.modification_year,
    d.modification_purpose,
    d.year_constructed,
    d.operated_by,
    d.operation_period,
    d.has_evaluating_studies,
    d.nature_of_evaluation_studies,
    d.engineering_notes,
    d.operating_notes,
    d.mean_fishway_velocity_ms,
    d.max_fishway_velocity_ms,
    d.estimate_of_attraction_pct,
    d.estimate_of_passage_success_pct,
    d.fishway_reference_id,
    d.data_source_id,
    d.data_source,
    d.complete_level_code,
    cl.name as complete_level,
    sp1.species AS known_use,
    sp2.species AS known_notuse,
    d.snapped_point as geometry
FROM fishways.fishways d
     LEFT JOIN cabd.province_territory_codes pt ON pt.code = d.province_territory_code
     LEFT JOIN cabd.watershed_groups wg ON wg.code = d.watershed_group_code
     LEFT JOIN cabd.upstream_passage_type_codes tc ON tc.code = d.fishpass_type_code
     LEFT JOIN fishways.entrance_location_codes elc on elc.code = d.entrance_location_code
     LEFT JOIN fishways.entrance_position_codes epc on epc.code = d.entrance_position_code
     LEFT JOIN fishways.fishway_complete_level_codes cl on cl.code = d.complete_level_code  
     LEFT JOIN ( SELECT a.fishway_id,
            array_agg(b.name) AS species
           FROM fishways.species_mapping a
             JOIN cabd.fish_species b ON a.species_id = b.id
          WHERE a.known_to_use
          GROUP BY a.fishway_id) sp1 ON sp1.fishway_id = d.cabd_id
     LEFT JOIN ( SELECT a.fishway_id,
            array_agg(b.name) AS species
           FROM fishways.species_mapping a
             JOIN cabd.fish_species b ON a.species_id = b.id
          WHERE NOT a.known_to_use
          GROUP BY a.fishway_id) sp2 ON sp2.fishway_id = d.cabd_id;


grant all privileges on cabd.fishways_view to cabd;

DELETE FROM cabd.feature_types where type = 'fishways';
DELETE FROM cabd.feature_type_metadata where view_name = 'cabd.fishways_view';

INSERT INTO cabd.feature_types ("type",data_view) VALUES
	 ('fishways','cabd.fishways_view');
	

INSERT INTO cabd.feature_type_metadata (view_name,field_name,"name",description,is_link, data_type, vw_simple_order, vw_all_order) VALUES
('cabd.fishways_view','cabd_id','System Identifier',NULL,false, 'uuid', null, 1),
('cabd.fishways_view','latitude','Latitude',NULL,false, 'double', 1, 2),
('cabd.fishways_view','longitude','Longitude',NULL,false, 'double', 2, 3),
('cabd.fishways_view','dam_id','Dam Identifier',NULL,true, 'url', 4, 4),
('cabd.fishways_view','dam_name_en','Dam Name (English)',NULL,false, 'varchar(512)', 3, 5),
('cabd.fishways_view','dam_name_fr','Dam Name (French)',NULL,false, 'varchar(512)', null, 6),
('cabd.fishways_view','river_name_en','River/Steam Name (English)',NULL,false, 'varchar(512)', 5, 7),
('cabd.fishways_view','river_name_fr','River/Stream Name (French)',NULL,false, 'varchar(512)', null, 8),
('cabd.fishways_view','waterbody_name_en','Waterbody Name (English)',NULL,false, 'varchar(512)', 6, 9),
('cabd.fishways_view','waterbody_name_fr','Waterbody Name (French)',NULL,false, 'varchar(512)', null, 10),
('cabd.fishways_view','province_territory_code','Province/Territory Code',NULL,false, 'varchar(2)', null, 11),
('cabd.fishways_view','province_territory','Province/Territory Name',NULL,false, 'varchar(32)', 7, 12),
('cabd.fishways_view','fishpass_type_code','Fishway Type Code',NULL,false, 'integer', null, 13),
('cabd.fishways_view','fishpass_type','Fishway Type',NULL,false, 'varchar(32)', 8, 14),
('cabd.fishways_view','monitoring_equipment','Monitoring Equipment',NULL,false, 'text', 9, 15),
('cabd.fishways_view','length_m','Length (m)',NULL,false, 'double', 10, 16),
('cabd.fishways_view','elevation_m','Elevation (m)',NULL,false, 'double', 11, 17),
('cabd.fishways_view','inclination_pct','Inclination (%)',NULL,false, 'double', 12, 18),
('cabd.fishways_view','depth_m','Mean channel depth (m)',NULL,false, 'double', 13, 19),
('cabd.fishways_view','entrance_location_code','Entrance Location Code',NULL,false, 'integer', null, 20),
('cabd.fishways_view','entrance_location','Entrance Location',NULL,false, 'varchar(32)', 14, 21),
('cabd.fishways_view','entrance_position_code','Entrance Position Code',NULL,false, 'integer', null, 22),
('cabd.fishways_view','entrance_position','Entrance Position',NULL,false, 'varchar(32)', 15, 23),
('cabd.fishways_view','mean_fishway_velocity_ms','Average Velocity of Water Flow (ms)',NULL,false, 'double', 16, 24),
('cabd.fishways_view','max_fishway_velocity_ms','Maximum Velocity of Water Flow(ms)',NULL,false, 'double', 17, 25),
('cabd.fishways_view','estimate_of_attraction_pct','Attraction Estimate (%)',NULL,false, 'double', 18, 26),
('cabd.fishways_view','estimate_of_passage_success_pct','Transit Success Estimate(%)',NULL,false, 'double', 19, 27),
('cabd.fishways_view','fishway_reference_id','Reference Identifier',NULL,false, 'varchar(256)', 20, 28),
('cabd.fishways_view','known_use','Species Known To Use',NULL,false, 'array(varchar)', 21, 29),
('cabd.fishways_view','known_notuse','Species Known To Not Use',NULL,false, 'array(varchar)', 22, 30),
('cabd.fishways_view','has_evaluating_studies','Has Evaluating Studies',NULL,false, 'boolean', 23, 31),
('cabd.fishways_view','nature_of_evaluation_studies','Nature of Evaluation Studies',NULL,false, 'text', 24, 32),
('cabd.fishways_view','architect','Architect',NULL,false, 'text', null, 33),
('cabd.fishways_view','contracted_by','Contracted By',NULL,false, 'text', null, 34),
('cabd.fishways_view','constructed_by','Constructed By',NULL,false, 'text', null, 35),
('cabd.fishways_view','plans_held_by','Plans Held By',NULL,false, 'text', null, 36),
('cabd.fishways_view','purpose','Purpose of Fishway',NULL,false, 'text', null, 37),
('cabd.fishways_view','designed_on_biology','Designed Based on Biology',NULL,false, 'boolean', null, 38),
('cabd.fishways_view','modified','Is Modified',NULL,false, 'boolean', null, 39),
('cabd.fishways_view','modification_year','Modification Year',NULL,false, 'integer', null, 40),
('cabd.fishways_view','modification_purpose','Modification Purpose',NULL,false, 'text', null, 41),
('cabd.fishways_view','year_constructed','Year Constructed',NULL,false, 'integer', null, 42),
('cabd.fishways_view','operation_period','Operation Period',NULL,false, 'text', null, 43),
('cabd.fishways_view','engineering_notes','Engineering Nodes',NULL,false, 'text', null, 44),
('cabd.fishways_view','operating_notes','Operation Nodes',NULL,false, 'text', null, 45),
('cabd.fishways_view','data_source_id','Data Source Identifier',NULL,false, 'varchar(256)', 25, 46),
('cabd.fishways_view','data_source','Data Source',NULL,false, 'varchar(256)', 26, 47),
('cabd.fishways_view','complete_level_code','Completeness Level Code',NULL,false, 'integer', null, 48),
('cabd.fishways_view','complete_level','Completeness Level',NULL,false, 'varchar(32)', 27, 49),
('cabd.fishways_view','watershed_group_code','Watershed Group Code',NULL,false, 'varchar(32)', null, 50),
('cabd.fishways_view','watershed_group_name','Watershed Group Name',NULL,false, 'varchar(512)', null, 51),
('cabd.fishways_view','nhn_workunit_id','NHN Working Unit',NULL,false, 'varchar(7)', null, 52),
('cabd.fishways_view','nearest_municipality','Nearest Municipality',NULL,false, 'varchar(512)', null, 53),
('cabd.fishways_view','feature_type','Feature Type',NULL,false, 'text', null, 54),
('cabd.fishways_view','geometry','Location',NULL,false, 'geometry', null, null);
	 
	 
