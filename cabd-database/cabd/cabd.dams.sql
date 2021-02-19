create schema dams;


CREATE TABLE dams.condition_codes (
	code int2 NOT NULL, -- Code referencing the physical condition of the dam.
	"name" varchar(32) NOT NULL, -- The physical condition of the dam.
	description text NULL,
	CONSTRAINT dam_conditions_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.condition_codes IS 'Reference table for the physical condition of the dam.';
COMMENT ON COLUMN dams.condition_codes.code IS 'Code referencing the physical condition of the dam.';
COMMENT ON COLUMN dams.condition_codes."name" IS 'The physical condition of the dam.';


CREATE TABLE dams.construction_type_codes (
	code int2 NOT NULL, -- Code referencing the dam structure type based on construction material/design.
	"name" varchar(32) NOT NULL, -- Dam structure type based on construction material/design.
	description text NULL,
	CONSTRAINT dam_construction_types_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.construction_type_codes IS 'Reference table for dam construction types.';
COMMENT ON COLUMN dams.construction_type_codes.code IS 'Code referencing the dam structure type based on construction material/design.';
COMMENT ON COLUMN dams.construction_type_codes."name" IS 'Dam structure type based on construction material/design.';



CREATE TABLE dams.dam_complete_level_codes (
	code int2 NOT NULL, -- Code referencing the level of information available for the barrier structure.
	"name" varchar(32) NOT NULL, -- The level of information available for the barrier structure.
	description text NULL,
	CONSTRAINT data_complete_level_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.dam_complete_level_codes IS 'Reference table for the level of information available for the dam barrier structure.';
COMMENT ON COLUMN dams.dam_complete_level_codes.code IS 'Code referencing the level of information available for the barrier structure.';
COMMENT ON COLUMN dams.dam_complete_level_codes."name" IS 'The level of information available for the barrier structure.';


CREATE TABLE dams.dam_use_codes (
	code int2 NOT NULL, -- Code referencing the primary use of the dam and associated facilities.
	"name" varchar(32) NULL, -- The primary intended use of the dam and associated facilities.
	description text NULL,
	CONSTRAINT dam_uses_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.dam_use_codes IS 'Reference table for the primary intended use of the dam and associated facilities.';
COMMENT ON COLUMN dams.dam_use_codes.code IS 'Code referencing the primary use of the dam and associated facilities.';
COMMENT ON COLUMN dams.dam_use_codes."name" IS 'The primary intended use of the dam and associated facilities.';


CREATE TABLE dams.downstream_passage_route_codes (
	code int2 NOT NULL, -- Code referencing the downstream fish passage routes.
	"name" varchar(32) NOT NULL, -- Types of downstream fish passage routes.
	description text NULL,
	CONSTRAINT downstream_passage_routes_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.downstream_passage_route_codes IS 'Reference table for downstream fish passage routes associated with barrier structures.';
COMMENT ON COLUMN dams.downstream_passage_route_codes.code IS 'Code referencing the downstream fish passage routes.';
COMMENT ON COLUMN dams.downstream_passage_route_codes."name" IS 'Types of downstream fish passage routes.';



CREATE TABLE dams.function_codes (
	code int2 NOT NULL, -- Code referencing the function of the dam.
	"name" varchar(64) NOT NULL, -- The function of the dam.
	description text NULL,
	CONSTRAINT dam_function_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.function_codes IS 'Reference table for the function of the dam.';
COMMENT ON COLUMN dams.function_codes.code IS 'Code referencing the function of the dam.';
COMMENT ON COLUMN dams.function_codes."name" IS 'The function of the dam.';



CREATE TABLE dams.lake_control_codes (
	code int2 NOT NULL, -- Control structure code
	"name" varchar(32) NOT NULL, -- Control structure name
	description text NULL,
	CONSTRAINT lake_control_codes_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.lake_control_codes IS 'Code identifying whether a reservoir has been built at the location of an existing natural lake using a lake control structure';
COMMENT ON COLUMN dams.lake_control_codes.code IS 'Control structure code';
COMMENT ON COLUMN dams.lake_control_codes."name" IS 'Control structure name';


CREATE TABLE dams.operating_status_codes (
	code int2 NOT NULL, -- Code referencing the operating status of the dam.
	"name" varchar(32) NULL, -- The status of the dam indicating continued active operation or the type of ‘end-of-life’ action completed if operations have ceased; validity based on date from the ‘LAST_UPDATE’ field.
	description text NULL,
	CONSTRAINT dam_operating_status_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.operating_status_codes IS 'Reference table for the operating status of the dam.';
COMMENT ON COLUMN dams.operating_status_codes.code IS 'Code referencing the operating status of the dam.';
COMMENT ON COLUMN dams.operating_status_codes."name" IS 'The status of the dam indicating continued active operation or the type of ‘end-of-life’ action completed if operations have ceased; validity based on date from the ‘LAST_UPDATE’ field.';

CREATE TABLE dams.size_codes (
	code int2 NOT NULL, -- Code referencing the size classification of the dam.
	"name" varchar(32) NOT NULL, -- The size classification of the dam based on the height of the dam in meters (dam_hgt_m).
	description text NULL,
	CONSTRAINT dam_size_class_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.size_codes IS 'Reference table for the size classification of the dam based on the height of the dam in meters.';
COMMENT ON COLUMN dams.size_codes.code IS 'Code referencing the size classification of the dam.';
COMMENT ON COLUMN dams.size_codes."name" IS 'The size classification of the dam based on the height of the dam in meters (dam_hgt_m).';



CREATE TABLE dams.spillway_type_codes (
	code int2 NOT NULL, -- Code referencing spillway type.
	"name" varchar(32) NOT NULL, -- The type of spillway associated with the dam structure.
	description text NULL,
	CONSTRAINT dam_spillway_types_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.spillway_type_codes IS 'Reference table for the type of spillway associated with the dam structure.';
COMMENT ON COLUMN dams.spillway_type_codes.code IS 'Code referencing spillway type.';
COMMENT ON COLUMN dams.spillway_type_codes."name" IS 'The type of spillway associated with the dam structure.';


CREATE TABLE dams.turbine_type_codes (
	code int2 NOT NULL, -- Code referencing turbine type.
	"name" varchar(32) NOT NULL, -- The type of turbine in the dam structure.
	description text NULL,
	CONSTRAINT dam_turbine_types_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.turbine_type_codes IS 'Reference table for turbine types.';
COMMENT ON COLUMN dams.turbine_type_codes.code IS 'Code referencing turbine type.';
COMMENT ON COLUMN dams.turbine_type_codes."name" IS 'The type of turbine in the dam structure.';



CREATE TABLE dams.use_codes (
	code int2 NOT NULL, -- Code referencing the amount of use
	"name" varchar(32) NOT NULL, -- Amount of use.
	description text NULL,
	CONSTRAINT use_codes_pk PRIMARY KEY (code)
);
COMMENT ON TABLE dams.use_codes IS 'Reference table for use fields of barriers. The extent to which specific use field is a planned use.';
COMMENT ON COLUMN dams.use_codes.code IS 'Code referencing the amount of use';
COMMENT ON COLUMN dams.use_codes."name" IS 'Amount of use.';



CREATE TABLE dams.dams_medium_large (
	cabd_id uuid NOT NULL, -- Unique identifier for each barrier point.
	dam_name_en varchar(512) NULL, -- Given or known name of the dam structure, English.
	dam_name_fr varchar(512) NULL, -- Given or known name of the dam structure, French.
	waterbody_name_en varchar(512) NULL, -- Name of waterbody in which the dam is recorded, English.
	waterbody_name_fr varchar(512) NULL, -- Name of waterbody in which the dam is recorded, French.
	reservoir_name_en varchar(512) NULL, -- Name of the reservoir or controlled lake (i.e., impounded waterbody), English.
	reservoir_name_fr varchar(512) NULL, -- Name of the reservoir or controlled lake (i.e., impounded waterbody), French.
	watershed_group_code varchar(32) NULL,
	nhn_workunit_id varchar(7) NULL,
	province_territory_code varchar(2) NOT NULL,
	nearest_municipality varchar(512) NULL, -- Name of nearest municipality.
	"owner" varchar(512) NULL, -- Person, company, organization, government unit, public utility, corporation or other entity which either holds a water license to operate a dam or retains the legal property title on the dam site.
	ownership_type_code int2 NULL,
	province_reg_body varchar(512) NULL, -- The public or government agency responsible for regulation, licensing, compliance and enforcement at the provincial level.
	federal_reg_body varchar(512) NULL, -- The public or government agency responsible for regulation, licensing, compliance and enforcement at the federal level.
	province_compliance_status varchar(64) NULL, -- The status of regulatory compliance with provincial licensing body.
	federal_compliance_status varchar(64) NULL, -- The status of regulatory compliance with the federal licensing body.
	operating_note text NULL, -- Unstructured comments on important operation considerations for the dam structure.
	operating_status_code int2 NULL,
	use_code int2 NULL,
	use_irrigation_code int2 NULL, -- Indicates the dam is used for irrigation purposes, and the extent to which irrigation is a planned use (i.e., main use, major use, or secondary use).
	use_electricity_code int2 NULL, -- Indicates the dam is used for hydroelectric energy production, and the extent to which hydroelectric production is a planned use (i.e., main use, major use, or secondary use).
	use_supply_code int2 NULL, -- Indicates the dam is used for water supply purposes, and the extent to which water supply is a planned use (i.e., main use, major use, or secondary use).
	use_floodcontrol_code int2 NULL, -- Indicates the dam is used for flood control purposes, and the extent to which flood control is a planned use (i.e., main use, major use, or secondary use).
	use_recreation_code int2 NULL, -- Indicates the dam is used for recreation purposes, and the extent to which recreation is a planned use (i.e., main use, major use, or secondary use).
	use_navigation_code int2 NULL, -- Indicates the dam is used for navigation, and the extent to which navigation is a planned use (i.e., main use, major use, or secondary use).
	use_fish_code int2 NULL, -- Indicates the dam is used for fisheries purposes, and the extent to which fisheries are a planned use (i.e., main use, major use, or secondary use).
	use_pollution_code int2 NULL, -- Indicates the dam is used for pollution control purposes, and the extent to which pollution control is a planned use (i.e., main use, major use, or secondary use).
	use_invasivespecies_code int2 NULL, -- Indicates the dam is used in control invasive species and the extent to which invasive species control is a planned use (i.e., Main use, major use, or secondary use).
	use_other_code int2 NULL, -- Indicates the dam is used for “other” purposes, and the extent to which it is a planned use (i.e., Main use, major use, or secondary use).
	lake_control_code int2 NULL, -- Indicates whether a reservoir has been built at the location of an existing natural lake using a lake control structure; currently this column only contains limited entries; “Yes” = lake control structure raises original lake level; “Enlarged” = lake control structure enlarged the original lake surface area; “Maybe” = not sure, but data seems to indicate a lake control structure.
	construction_year numeric NULL, -- Year in which the dam was built (not further specified: year of construction; year of completion; year of commissioning; year of refurbishment/update; etc.)
	removed_year numeric NULL, -- Year in which the dam was decommissioned, removed, replaced, subsumed, or destroyed.
	assess_schedule varchar(100) NULL, -- Frequency at which the dam structure is assessed by ownership party or regulatory body.
	expected_life int2 NULL, -- Number of years the dam structure is expected to last.
	maintenance_last date NULL, -- Date of last maintenance or renovation work performed on the barrier.
	maintenance_next date NULL, -- Date of the next scheduled maintenance or renovation work performed on the barrier.
	function_code int2 NULL,
	condition_code int2 NULL,
	construction_type_code int2 NULL,
	height_m float4 NULL, -- Reported height of the dam in meters. Depending on the data source this could variably be height of dam wall, crest height, or head height.
	length_m float4 NULL, -- Length of the crest of the dam from one bank (or abutment) to the other in meters.
	size_class_code int2 NULL, -- The size classification of the dam based on the height of the dam in meters (dam_hgt_m).
	spillway_capacity float8 NULL, -- The designed capacity of the spillway in m3 per second.
	spillway_type_code int2 NULL,
	reservoir_present bool NULL, -- Indicates if a reservoir is present due to the creation of the dam.
	reservoir_area_skm float4 NULL, -- Representative surface area of reservoir in square kilometers.
	reservoir_depth_m float4 NULL, -- Average depth of reservoir in meters.
	storage_capacity_mcm float8 NULL, -- Storage capacity of reservoir in million cubic meters.
	avg_rate_of_discharge_ls float8 NULL, -- Average rate of discharge at dam location in liters per second.
	degree_of_regulation_pc float4 NULL, -- Degree of Regulation (DOR) in percent; equivalent to “residence time” of water in the reservoir; calculated as ratio between storage capacity (‘Cap_mcm’) and total annual flow (derived from ‘Dis_avg_ls’); values capped at 10,000 indicate exceedingly high values, which may be due to inconsistencies in the data and/or incorrect allocation to the river network and the associated discharges.
	provincial_flow_req float8 NULL, -- Legislated flow requirements for the dam structure in cubic meters per second regulated by the provincial licensing body.
	federal_flow_req float8 NULL, -- Minimum flow recommendations for the dam structure in cubic meters per second. Based on assessments by Fisheries and Oceans Canada for the protection of fish and fish habitat.
	catchment_area_skm float8 NULL, -- Area of upstream catchment draining into the reservoir in square kilometers. The area of upstream catchment is defined by “Elementary Catchment” units in the National Hydrography Network.
	hydro_peaking_system bool NULL, -- Indicates if the dam employs a hydro peaking system.
	generating_capacity_mwh float8 NULL, -- The amount of electricity the hydroelectric facility can produce in megawatt hours.
	turbine_number int2 NULL, -- The number of turbines in the dam structure.
	turbine_type_code int2 NULL,
	up_passage_type_code int2 NULL,
	down_passage_route_code int2 NULL,
	capture_date date NULL, -- The capture date for a structure as documented in the original dataset, if provided.
	last_update date NULL, -- Most recent date of the data source used to create, revise or confirm the dam record.
	data_source_id varchar(256) NULL, -- The unique id assigned to the dam record in the original data source.
	data_source varchar(256) NULL, -- The original data source from which the dam record was obtained.
	"comments" text NULL, -- Unstructured comments about the dam.
	complete_level_code int2 NULL,
	original_point geometry(POINT, 4326) NULL,
	snapped_point geometry(POINT, 4326) NULL,
	CONSTRAINT dams_medium_large_pk PRIMARY KEY (cabd_id)
);
CREATE INDEX dams_medium_large_point_idx ON dams.dams_medium_large USING gist (snapped_point);
COMMENT ON TABLE dams.dams_medium_large IS 'Attributes and description for medium dams (i.e., having a height between 5 and 15 m) and large dams (i.e., having height of 15 m or greater, or a height between 5 m and 15 m that impounds more than 3 million m3).';
COMMENT ON COLUMN dams.dams_medium_large.cabd_id IS 'Unique identifier for each barrier point.';
COMMENT ON COLUMN dams.dams_medium_large.dam_name_en IS 'Given or known name of the dam structure, English.';
COMMENT ON COLUMN dams.dams_medium_large.dam_name_fr IS 'Given or known name of the dam structure, French.';
COMMENT ON COLUMN dams.dams_medium_large.waterbody_name_en IS 'Name of waterbody in which the dam is recorded, English.';
COMMENT ON COLUMN dams.dams_medium_large.waterbody_name_fr IS 'Name of waterbody in which the dam is recorded, French.';
COMMENT ON COLUMN dams.dams_medium_large.reservoir_name_en IS 'Name of the reservoir or controlled lake (i.e., impounded waterbody), English.';
COMMENT ON COLUMN dams.dams_medium_large.reservoir_name_fr IS 'Name of the reservoir or controlled lake (i.e., impounded waterbody), French.';
COMMENT ON COLUMN dams.dams_medium_large.nearest_municipality IS 'Name of nearest municipality.';
COMMENT ON COLUMN dams.dams_medium_large."owner" IS 'Person, company, organization, government unit, public utility, corporation or other entity which either holds a water license to operate a dam or retains the legal property title on the dam site.';
COMMENT ON COLUMN dams.dams_medium_large.province_reg_body IS 'The public or government agency responsible for regulation, licensing, compliance and enforcement at the provincial level.';
COMMENT ON COLUMN dams.dams_medium_large.federal_reg_body IS 'The public or government agency responsible for regulation, licensing, compliance and enforcement at the federal level.';
COMMENT ON COLUMN dams.dams_medium_large.province_compliance_status IS 'The status of regulatory compliance with provincial licensing body.';
COMMENT ON COLUMN dams.dams_medium_large.federal_compliance_status IS 'The status of regulatory compliance with the federal licensing body.';
COMMENT ON COLUMN dams.dams_medium_large.operating_note IS 'Unstructured comments on important operation considerations for the dam structure.';
COMMENT ON COLUMN dams.dams_medium_large.use_irrigation_code IS 'Indicates the dam is used for irrigation purposes, and the extent to which irrigation is a planned use (i.e., main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.use_electricity_code IS 'Indicates the dam is used for hydroelectric energy production, and the extent to which hydroelectric production is a planned use (i.e., main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.use_supply_code IS 'Indicates the dam is used for water supply purposes, and the extent to which water supply is a planned use (i.e., main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.use_floodcontrol_code IS 'Indicates the dam is used for flood control purposes, and the extent to which flood control is a planned use (i.e., main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.use_recreation_code IS 'Indicates the dam is used for recreation purposes, and the extent to which recreation is a planned use (i.e., main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.use_navigation_code IS 'Indicates the dam is used for navigation, and the extent to which navigation is a planned use (i.e., main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.use_fish_code IS 'Indicates the dam is used for fisheries purposes, and the extent to which fisheries are a planned use (i.e., main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.use_pollution_code IS 'Indicates the dam is used for pollution control purposes, and the extent to which pollution control is a planned use (i.e., main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.use_invasivespecies_code IS 'Indicates the dam is used in control invasive species and the extent to which invasive species control is a planned use (i.e., Main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.use_other_code IS 'Indicates the dam is used for “other” purposes, and the extent to which it is a planned use (i.e., Main use, major use, or secondary use).';
COMMENT ON COLUMN dams.dams_medium_large.lake_control_code IS 'Indicates whether a reservoir has been built at the location of an existing natural lake using a lake control structure; currently this column only contains limited entries; “Yes” = lake control structure raises original lake level; “Enlarged” = lake control structure enlarged the original lake surface area; “Maybe” = not sure, but data seems to indicate a lake control structure.';
COMMENT ON COLUMN dams.dams_medium_large.construction_year IS 'Year in which the dam was built (not further specified: year of construction; year of completion; year of commissioning; year of refurbishment/update; etc.)';
COMMENT ON COLUMN dams.dams_medium_large.removed_year IS 'Year in which the dam was decommissioned, removed, replaced, subsumed, or destroyed.';
COMMENT ON COLUMN dams.dams_medium_large.assess_schedule IS 'Frequency at which the dam structure is assessed by ownership party or regulatory body.';
COMMENT ON COLUMN dams.dams_medium_large.expected_life IS 'Number of years the dam structure is expected to last.';
COMMENT ON COLUMN dams.dams_medium_large.maintenance_last IS 'Date of last maintenance or renovation work performed on the barrier.';
COMMENT ON COLUMN dams.dams_medium_large.maintenance_next IS 'Date of the next scheduled maintenance or renovation work performed on the barrier.';
COMMENT ON COLUMN dams.dams_medium_large.height_m IS 'Reported height of the dam in meters. Depending on the data source this could variably be height of dam wall, crest height, or head height.';
COMMENT ON COLUMN dams.dams_medium_large.length_m IS 'Length of the crest of the dam from one bank (or abutment) to the other in meters.';
COMMENT ON COLUMN dams.dams_medium_large.size_class_code IS 'The size classification of the dam based on the height of the dam in meters (dam_hgt_m).';
COMMENT ON COLUMN dams.dams_medium_large.spillway_capacity IS 'The designed capacity of the spillway in m3 per second.';
COMMENT ON COLUMN dams.dams_medium_large.reservoir_present IS 'Indicates if a reservoir is present due to the creation of the dam.';
COMMENT ON COLUMN dams.dams_medium_large.reservoir_area_skm IS 'Representative surface area of reservoir in square kilometers.';
COMMENT ON COLUMN dams.dams_medium_large.reservoir_depth_m IS 'Average depth of reservoir in meters.';
COMMENT ON COLUMN dams.dams_medium_large.storage_capacity_mcm IS 'Storage capacity of reservoir in million cubic meters.';
COMMENT ON COLUMN dams.dams_medium_large.avg_rate_of_discharge_ls IS 'Average rate of discharge at dam location in liters per second.';
COMMENT ON COLUMN dams.dams_medium_large.degree_of_regulation_pc IS 'Degree of Regulation (DOR) in percent; equivalent to “residence time” of water in the reservoir; calculated as ratio between storage capacity (‘Cap_mcm’) and total annual flow (derived from ‘Dis_avg_ls’); values capped at 10,000 indicate exceedingly high values, which may be due to inconsistencies in the data and/or incorrect allocation to the river network and the associated discharges.';
COMMENT ON COLUMN dams.dams_medium_large.provincial_flow_req IS 'Legislated flow requirements for the dam structure in cubic meters per second regulated by the provincial licensing body.';
COMMENT ON COLUMN dams.dams_medium_large.federal_flow_req IS 'Minimum flow recommendations for the dam structure in cubic meters per second. Based on assessments by Fisheries and Oceans Canada for the protection of fish and fish habitat.';
COMMENT ON COLUMN dams.dams_medium_large.catchment_area_skm IS 'Area of upstream catchment draining into the reservoir in square kilometers. The area of upstream catchment is defined by “Elementary Catchment” units in the National Hydrography Network.';
COMMENT ON COLUMN dams.dams_medium_large.hydro_peaking_system IS 'Indicates if the dam employs a hydro peaking system.';
COMMENT ON COLUMN dams.dams_medium_large.generating_capacity_mwh IS 'The amount of electricity the hydroelectric facility can produce in megawatt hours.';
COMMENT ON COLUMN dams.dams_medium_large.turbine_number IS 'The number of turbines in the dam structure.';
COMMENT ON COLUMN dams.dams_medium_large.capture_date IS 'The capture date for a structure as documented in the original dataset, if provided.';
COMMENT ON COLUMN dams.dams_medium_large.last_update IS 'Most recent date of the data source used to create, revise or confirm the dam record.';
COMMENT ON COLUMN dams.dams_medium_large.data_source_id IS 'The unique id assigned to the dam record in the original data source.';
COMMENT ON COLUMN dams.dams_medium_large.data_source IS 'The original data source from which the dam record was obtained.';
COMMENT ON COLUMN dams.dams_medium_large."comments" IS 'Unstructured comments about the dam.';


-- dams.dams_medium_large foreign keys
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk FOREIGN KEY (province_territory_code) REFERENCES cabd.province_territory_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_10 FOREIGN KEY (use_navigation_code) REFERENCES dams.use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_11 FOREIGN KEY (use_fish_code) REFERENCES dams.use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_12 FOREIGN KEY (use_pollution_code) REFERENCES dams.use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_13 FOREIGN KEY (use_invasivespecies_code) REFERENCES dams.use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_14 FOREIGN KEY (use_other_code) REFERENCES dams.use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_15 FOREIGN KEY (condition_code) REFERENCES dams.condition_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_16 FOREIGN KEY (function_code) REFERENCES dams.function_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_17 FOREIGN KEY (construction_type_code) REFERENCES dams.construction_type_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_18 FOREIGN KEY (size_class_code) REFERENCES dams.size_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_19 FOREIGN KEY (spillway_type_code) REFERENCES dams.spillway_type_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_2 FOREIGN KEY (ownership_type_code) REFERENCES cabd.barrier_ownership_type_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_20 FOREIGN KEY (up_passage_type_code) REFERENCES cabd.upstream_passage_type_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_21 FOREIGN KEY (down_passage_route_code) REFERENCES dams.downstream_passage_route_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_22 FOREIGN KEY (turbine_type_code) REFERENCES dams.turbine_type_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_23 FOREIGN KEY (complete_level_code) REFERENCES dams.dam_complete_level_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_24 FOREIGN KEY (lake_control_code) REFERENCES dams.lake_control_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_25 FOREIGN KEY (watershed_group_code) REFERENCES cabd.watershed_groups(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_3 FOREIGN KEY (operating_status_code) REFERENCES dams.operating_status_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_4 FOREIGN KEY (use_code) REFERENCES dams.dam_use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_5 FOREIGN KEY (use_irrigation_code) REFERENCES dams.use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_6 FOREIGN KEY (use_electricity_code) REFERENCES dams.use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_7 FOREIGN KEY (use_supply_code) REFERENCES dams.use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_8 FOREIGN KEY (use_floodcontrol_code) REFERENCES dams.use_codes(code);
ALTER TABLE dams.dams_medium_large ADD CONSTRAINT dams_medium_large_fk_9 FOREIGN KEY (use_recreation_code) REFERENCES dams.use_codes(code);


INSERT INTO dams.operating_status_codes (code,"name",description) VALUES
	 (1,'Abandoned/Orphaned','Structure is not in operation; has no identifiable owner with legal liability.'),
	 (2,'Active','Structure is in operation; functioning as intended.'),
	 (3,'Decommissioned','Structure dismantled/removed.'),
	 (4,'Retired/Closed','Structure was withdrawn from service but remains.'),
	 (5,'Unknown','Operating status of the structure is not known.');
	 
	 
INSERT INTO dams.lake_control_codes (code,"name",description) VALUES
	 (1,'Yes','lake control structure raises original lake level'),
	 (2,'Enlarged','lake control structure enlarged the original lake surface area'),
	 (3,'Maybe','not sure, but data seems to indicate a lake control structure');

INSERT INTO dams.function_codes (code,"name",description) VALUES
	 (1,'Storage','A structure built to store water during times of high flow for a variety of purposes (e.g., water supply, irrigation etc.).'),
	 (2,'Diversion','A structure built to divert all or some of the water from a waterway into a man-made canal or conduit, sometimes referred to as a weir. '),
	 (3,'Detention','A structure built for flood control. Retains excess water in a reservoir to maintain the carrying capacity of the waterway, releasing it gradually at a controlled rate to protect downstream areas from flooding.'),
	 (4,'Debris','A structure built across a waterway to retain debris (e.g., driftwood, gravel, sand etc.).'),
	 (5,'Coffer','An enclosed temporary structure commonly used during construction. Water is pumped from the enclosure to provide a dry worksite. '),
	 (6,'Saddle','An auxiliary structure constructed at low spots along the perimeter of a reservoir to limit its extent, or to allow for an increase in storage capacity.'),
	 (10,'Hydro – Run-of-river','A facility that channels flowing water from a river through a canal or penstock to spin a turbine.'),
	 (11,'Hydro – Tidal','A facility configured to intake and store water during high tide and slowly release it back into the ocean during low tide.'),
	 (12,'Other',NULL),
	 (7,'Hydro – Closed-cycle pumped storage','A facility configured to pump water between two water reservoirs at different elevations; both reservoirs are isolated from a free-flowing water source.'),
	 (8,'Hydro – Conventional storage','A facility that impounds water, which when released, flows through a turbine and generates electricity.'),
	 (9,'Hydro – Open-cycle pumped storage','A facility configured to pump water between two water reservoirs at different elevations; a free-flowing water source is used for either the upper or lower reservoir.');
	 
INSERT INTO dams.downstream_passage_route_codes (code,"name",description) VALUES
	 (1,'Bypass','A fish screen is used to channel juvenile fish downstream of the structure, bypassing the turbine channel.'),
	 (2,'River channel','Fish move downstream of the structure via the natural river channel.'),
	 (3,'Spillway','Fish move downstream of the structure by passing over the spillway.'),
	 (4,'Turbine','Fish move downstream of the structure by passing through the turbine channel.');
	 
INSERT INTO dams.dam_use_codes (code,"name",description) VALUES
	 (1,'Irrigation','Supplies controlled amounts of water to land or crops in needed intervals.'),
	 (2,'Hydroelectricity','Generates electricity by passing water through a hydraulic turbine.'),
	 (3,'Water supply','Water stored for municipal use.'),
	 (4,'Flood control','Reduces the effects of flood waters or high-water levels on downstream areas.'),
	 (5,'Recreation','Reservoir allows for recreational activities (e.g., swimming, boating, fishing etc.).'),
	 (6,'Navigation','Increases water levels on rivers to permit ship to navigate waters that were previously unnavigable.'),
	 (7,'Fisheries','Increasing water levels of the reservoir during spawning season creates suitable spawning habitat for fish, enhancing fish production and fishing success of anglers.'),
	 (8,'Pollution control','Protects the water resource by trapping polluted water in reservoirs or ponds.'),
	 (9,'Invasive species control','Prevents the spread of invasive species upstream or to environmentally sensitive areas within the waterway. '),
	 (10,'Other',NULL);
	 
INSERT INTO dams.dam_complete_level_codes (code,"name",description) VALUES
	 (1,'Unverified','Record is not verified.'),
	 (2,'Minimal','Record is verified but little attribute data is captured.'),
	 (3,'Moderate','Record is verified and most attribute data is captured.'),
	 (4,'Complete','Record is verified and all attribute data is captured.');
	 
INSERT INTO dams.construction_type_codes (code,"name",description) VALUES
	 (1,'Arch','Concrete structure that is curved in the upstream direction.'),
	 (2,'Buttress','Structure with watertight wall supported at intervals on the downstream side by a series of triangle shaped walls; typically, reinforced concrete.'),
	 (3,'Earthfill','Structure composed of successive compacted layers of earth; clay-soil core reduces permeability. Also referred to as earth and embankment dams.'),
	 (4,'Gravity','Structure constructed of concrete and/or masonry which relies on its weight and internal strength for stability.'),
	 (5,'Multiple Arch','A buttress dam composed of a series of arches for the upstream face.'),
	 (6,'Rockfill','Structure composed of dumped and compacted rock fill; permeable with impermeable core or layer on the upstream face.'),
	 (7,'Steel','Structure consisting of a steel framework; inclined struts and steel plates on the upstream face. Supplemented with timber and earthfill to make them water-tight, steel dams are sometimes used as a temporary cofferdam during the construction of the main dam.'),
	 (8,'Timber','Structure built primarily of wood. Commonly used for temporary water diversion in low-head (2-4 m) areas during the construction of the main dam.'),
	 (9,'Unlisted','Construction type is unknown.'),
	 (10,'Other',NULL);
	 
INSERT INTO dams.condition_codes (code,"name",description) VALUES
	 (1,'Good','Structure is fit for its intended purpose; not damaged and capable of agreed standard of performance.'),
	 (2,'Fair','Structure is in average condition, possessing minor defects.'),
	 (3,'Poor','Structure condition is deteriorated and requires maintenance.'),
	 (4,'Unreliable','Structure is not suitable for its intended purpose.');
	 
INSERT INTO dams.turbine_type_codes (code,"name",description) VALUES
	 (1,'Cross-flow','An impulse turbine used in smaller hydroelectric sites with power outputs between 5 and 100 kW. Known also as Banki-Mitchell or Ossberger turbines.'),
	 (2,'Francis','A reaction (propeller) turbine commonly used in medium- or large-scale hydroelectric plants for head heights as low as 2 m and as high as 300 m.'),
	 (3,'Kaplan','A reaction (propeller) turbine with axial-flow and adjustable blades. Most useful for use in cams with large volumes of flow.'),
	 (4,'Pelton','An impulse turbine commonly used in facilities with head height greater than 300 m.');
	 
INSERT INTO dams.spillway_type_codes (code,"name",description) VALUES
	 (1,'Combined','Single spillway that acts as both the principal and emergency spillway.'),
	 (2,'Free','Surplus water falls freely from the crest of the weir; a straight drop spillway or free overfall spillway.'),
	 (3,'Gated','Surplus water is regulated with a gate to prevent downstream flooding.'),
	 (4,'Other',NULL),
	 (5,'None','Structure does not have a spillway.');
	 
INSERT INTO dams.size_codes (code,"name",description) VALUES
	 (1,'Small','A dam having a height less than 5 m.'),
	 (2,'Medium','A dam having a height between 5 and 15 m.'),
	 (3,'Large','A dam having a height of 15 m or greater, or a height between 5 m and 15 m that impounds more than 3 million m3.');
	 
INSERT INTO dams.use_codes (code,"name",description) VALUES
	 (1,'Main',NULL),
	 (2,'Major',NULL),
	 (3,'Secondary',NULL);
	 

	 
CREATE OR REPLACE VIEW cabd.dams_medium_large_view
AS SELECT d.cabd_id,
    'dams_medium_large'::text AS feature_type,
    st_y(d.snapped_point) as latitude,
    st_x(d.snapped_point) as longitude,
    d.dam_name_en,
    d.dam_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.reservoir_name_en,
    d.reservoir_name_fr,
    d.watershed_group_code,
    wg.name AS watershed_group_name,
    d.nhn_workunit_id,
    d.province_territory_code,
    pt.name AS province_territory,
    d.owner,
    d.ownership_type_code,
    ow.name AS ownership_type,
    d.nearest_municipality,
    d.province_reg_body,
    d.federal_reg_body,
    d.province_compliance_status,
    d.federal_compliance_status,
    d.operating_note,
    d.operating_status_code,
    os.name AS operating_status,
    d.use_code,
    duc.name AS dam_use,
    d.use_irrigation_code,
    c1.name AS use_irrigation,
    d.use_electricity_code,
    c2.name AS use_electricity,
    d.use_supply_code,
    c3.name AS use_supply,
    d.use_floodcontrol_code,
    c4.name AS use_floodcontrol,
    d.use_recreation_code,
    c5.name AS use_recreation,
    d.use_navigation_code,
    c6.name AS use_navigation,
    d.use_fish_code,
    c7.name AS use_fish,
    d.use_pollution_code,
    c8.name AS use_pollution,
    d.use_invasivespecies_code,
    c9.name AS use_invasivespecies,
    d.use_other_code,
    c10.name AS use_other,
    d.lake_control_code,
    lk.name AS lake_control,
    d.construction_year,
    d.removed_year,
    d.assess_schedule,
    d.expected_life,
    d.maintenance_last,
    d.maintenance_next,
    d.function_code,
    f.name AS function_name,
    d.condition_code,
    dc.name AS dam_condition,
    d.construction_type_code,
    dct.name AS construction_type,
    d.height_m,
    d.length_m,
    d.size_class_code,
    ds.name AS size_class,
    d.spillway_capacity,
    d.spillway_type_code,
    dsp.name AS spillway_type,
    d.reservoir_present,
    d.reservoir_area_skm,
    d.reservoir_depth_m,
    d.storage_capacity_mcm,
    d.avg_rate_of_discharge_ls,
    d.degree_of_regulation_pc,
    d.provincial_flow_req,
    d.federal_flow_req,
    d.catchment_area_skm,
    d.hydro_peaking_system,
    d.generating_capacity_mwh,
    d.turbine_number,
    d.turbine_type_code,
    dt.name AS turbine_type,
    d.up_passage_type_code,
    up.name AS up_passage_type,
    d.down_passage_route_code,
    down.name AS down_passage_route,
    d.capture_date,
    d.last_update,
    d.data_source_id,
    d.data_source,
    d.comments,
    d.complete_level_code,
    cl.name AS complete_level,
    d.snapped_point AS geometry
   FROM dams.dams_medium_large d
     JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.barrier_ownership_type_codes ow ON ow.code = d.ownership_type_code
     LEFT JOIN dams.operating_status_codes os ON os.code = d.operating_status_code
     LEFT JOIN dams.dam_use_codes duc ON duc.code = d.use_code
     LEFT JOIN dams.use_codes c1 ON c1.code = d.use_irrigation_code
     LEFT JOIN dams.use_codes c2 ON c2.code = d.use_electricity_code
     LEFT JOIN dams.use_codes c3 ON c3.code = d.use_supply_code
     LEFT JOIN dams.use_codes c4 ON c4.code = d.use_floodcontrol_code
     LEFT JOIN dams.use_codes c5 ON c5.code = d.use_recreation_code
     LEFT JOIN dams.use_codes c6 ON c6.code = d.use_navigation_code
     LEFT JOIN dams.use_codes c7 ON c7.code = d.use_fish_code
     LEFT JOIN dams.use_codes c8 ON c8.code = d.use_pollution_code
     LEFT JOIN dams.use_codes c9 ON c9.code = d.use_invasivespecies_code
     LEFT JOIN dams.use_codes c10 ON c10.code = d.use_other_code
     LEFT JOIN dams.function_codes f ON f.code = d.function_code
     LEFT JOIN dams.condition_codes dc ON dc.code = d.condition_code
     LEFT JOIN dams.construction_type_codes dct ON dct.code = d.construction_type_code
     LEFT JOIN dams.size_codes ds ON ds.code = d.size_class_code
     LEFT JOIN dams.spillway_type_codes dsp ON dsp.code = d.spillway_type_code
     LEFT JOIN dams.turbine_type_codes dt ON dsp.code = d.turbine_type_code
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN dams.downstream_passage_route_codes down ON down.code = d.down_passage_route_code
     LEFT JOIN dams.dam_complete_level_codes cl ON cl.code = d.complete_level_code
     LEFT JOIN dams.lake_control_codes lk ON lk.code = d.lake_control_code
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = d.watershed_group_code::text;

grant all privileges on cabd.dams_medium_large_view to cabd;
     
DELETE FROM cabd.feature_types where type = 'dams_medium_large';
DELETE FROM cabd.feature_type_metadata where view_name = 'cabd.dams_medium_large_view';

INSERT INTO cabd.feature_types ("type",data_view) VALUES
	 ('dams_medium_large','cabd.dams_medium_large_view');

INSERT INTO cabd.feature_type_metadata (view_name,field_name,"name",description,is_link) VALUES
	 ('cabd.dams_medium_large_view','geometry','Location',NULL,false),
	 ('cabd.dams_medium_large_view','watershed_group_code','Watershed Group Code',NULL,false),
	 ('cabd.dams_medium_large_view','capture_date','Data Capture Date','The capture date for a structure as documented in the original dataset, if provided.',false),
	 ('cabd.dams_medium_large_view','last_update','Data Last Updated Date','Most recent date of the data source used to create, revise or confirm the dam record.',false),
	 ('cabd.dams_medium_large_view','data_source_id','Data Source Id','The unique id assigned to the dam record in the original data source.',false),
	 ('cabd.dams_medium_large_view','data_source','Data Source','The original data source from which the dam record was obtained.',false),
	 ('cabd.dams_medium_large_view','comments','Comments','Unstructured comments about the dam.',false),
	 ('cabd.dams_medium_large_view','complete_level_code','Complete Level Code',NULL,false),
	 ('cabd.dams_medium_large_view','complete_level','Complete Level',NULL,false),
	 ('cabd.dams_medium_large_view','feature_type','Feature Type',NULL,false),
	 ('cabd.dams_medium_large_view','latitude','Latitude','Latitude of point location of dam in decimal degrees; the point location is only an approximation of the actual dam location.',false),
	 ('cabd.dams_medium_large_view','longitude','Longitude','Longitude of point location of dam in decimal degrees; the point location is only an approximation of the actual dam location.',false),
	 ('cabd.dams_medium_large_view','dam_name_en','Dam Name (English)','Given or known name of the dam structure, English.',false),
	 ('cabd.dams_medium_large_view','dam_name_fr','Dam Name (French)','Given or known name of the dam structure, French.',false),
	 ('cabd.dams_medium_large_view','waterbody_name_en','Waterbody Name (English)','Name of waterbody in which the dam is recorded, English.',false),
	 ('cabd.dams_medium_large_view','waterbody_name_fr','Waterbody Name (French)','Name of waterbody in which the dam is recorded, French.',false),
	 ('cabd.dams_medium_large_view','reservoir_name_en','Reservoir Name (English)','Name of the reservoir or controlled lake (i.e., impounded waterbody), English.',false),
	 ('cabd.dams_medium_large_view','reservoir_name_fr','Reservoir Name (French)','Name of the reservoir or controlled lake (i.e., impounded waterbody), French.',false),
	 ('cabd.dams_medium_large_view','watershed_group_name','Watershed Group Name',NULL,false),
	 ('cabd.dams_medium_large_View', 'nhn_workunit_id', 'NHN Work Unit', NULL, false),
	 ('cabd.dams_medium_large_view','province_territory_code','Province/Territory Code',NULL,false),
	 ('cabd.dams_medium_large_view','province_territory','Province/Territory Name',NULL,false),
	 ('cabd.dams_medium_large_view','nearest_municipality','Nearest Municipality','Name of nearest municipality.',false),
	 ('cabd.dams_medium_large_view','province_reg_body','Provincial Regulatory Body','The public or government agency responsible for regulation, licensing, compliance and enforcement at the provincial level.',false),
	 ('cabd.dams_medium_large_view','federal_reg_body','Federal Regulatory Body','The public or government agency responsible for regulation, licensing, compliance and enforcement at the federal level.',false),
	 ('cabd.dams_medium_large_view','operating_note','Operating Note','Unstructured comments on important operation considerations for the dam structure.',false),
	 ('cabd.dams_medium_large_view','operating_status_code','Operating Status Code',NULL,false),
	 ('cabd.dams_medium_large_view','operating_status','Operating Status',NULL,false),
	 ('cabd.dams_medium_large_view','use_code','Dam Use Code',NULL,false),
	 ('cabd.dams_medium_large_view','dam_use','Dam Use',NULL,false),
	 ('cabd.dams_medium_large_view','use_irrigation_code','Use Irrigation Code','Indicates the dam is used for irrigation purposes, and the extent to which irrigation is a planned use (i.e., main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','use_irrigation','Use Irrigation',NULL,false),
	 ('cabd.dams_medium_large_view','use_electricity_code','Use Hydroelectricity Code','Indicates the dam is used for hydroelectric energy production, and the extent to which hydroelectric production is a planned use (i.e., main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','use_electricity','Use Hydroelectricity',NULL,false),
	 ('cabd.dams_medium_large_view','use_supply_code','Use Water Supply Code','Indicates the dam is used for water supply purposes, and the extent to which water supply is a planned use (i.e., main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','use_supply','Use Water Supply',NULL,false),
	 ('cabd.dams_medium_large_view','use_floodcontrol_code','Use Flood Control Code','Indicates the dam is used for flood control purposes, and the extent to which flood control is a planned use (i.e., main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','use_floodcontrol','Use Flood Control',NULL,false),
	 ('cabd.dams_medium_large_view','use_recreation_code','Use Recreation Code','Indicates the dam is used for recreation purposes, and the extent to which recreation is a planned use (i.e., main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','use_recreation','Use Recreation',NULL,false),
	 ('cabd.dams_medium_large_view','use_navigation_code','Use Navigation Code','Indicates the dam is used for navigation, and the extent to which navigation is a planned use (i.e., main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','cabd_id','Barrier Identifier','Unique Identifier for dam.',false),
	 ('cabd.dams_medium_large_view','use_navigation','Use Navigation',NULL,false),
	 ('cabd.dams_medium_large_view','use_fish_code','Use Fisheries Code','Indicates the dam is used for fisheries purposes, and the extent to which fisheries are a planned use (i.e., main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','use_fish','Use Fisheries',NULL,false),
	 ('cabd.dams_medium_large_view','use_pollution_code','Use Pollution Control Code','Indicates the dam is used for pollution control purposes, and the extent to which pollution control is a planned use (i.e., main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','use_pollution','Use Pollution Control',NULL,false),
	 ('cabd.dams_medium_large_view','use_invasivespecies_code','Use Invasive Species Control Code','Indicates the dam is used in control invasive species and the extent to which invasive species control is a planned use (i.e., Main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','use_invasivespecies','Use Invasive Species Control',NULL,false),
	 ('cabd.dams_medium_large_view','use_other_code','Use Other Code','Indicates the dam is used for Ã¢â‚¬Å“otherÃ¢â‚¬ purposes, and the extent to which it is a planned use (i.e., Main use, major use, or secondary use).',false),
	 ('cabd.dams_medium_large_view','use_other','Use Other',NULL,false),
	 ('cabd.dams_medium_large_view','lake_control_code','Lake Control Code','Indicates whether a reservoir has been built at the location of an existing natural lake using a lake control structure; currently this column only contains limited entries; Ã¢â‚¬Å“YesÃ¢â‚¬ = lake control structure raises original lake level; Ã¢â‚¬Å“EnlargedÃ¢â‚¬ = lake control structure enlarged the original lake surface area; Ã¢â‚¬Å“MaybeÃ¢â‚¬ = not sure, but data seems to indicate a lake control structure.',false),
	 ('cabd.dams_medium_large_view','lake_control','Lake Control',NULL,false),
	 ('cabd.dams_medium_large_view','construction_year','Construction Year','Year in which the dam was built (not further specified: year of construction; year of completion; year of commissioning; year of refurbishment/update; etc.)',false),
	 ('cabd.dams_medium_large_view','removed_year','Removed Year','Year in which the dam was decommissioned, removed, replaced, subsumed, or destroyed.',false),
	 ('cabd.dams_medium_large_view','assess_schedule','Assessment Schedule','Frequency at which the dam structure is assessed by ownership party or regulatory body.',false),
	 ('cabd.dams_medium_large_view','expected_life','Expected Life (years)','Number of years the dam structure is expected to last.',false),
	 ('cabd.dams_medium_large_view','maintenance_last','Last Maintenance Date','Date of last maintenance or renovation work performed on the barrier.',false),
	 ('cabd.dams_medium_large_view','maintenance_next','Next Maintenance Date','Date of the next scheduled maintenance or renovation work performed on the barrier.',false),
	 ('cabd.dams_medium_large_view','function_code','Dam Function Code',NULL,false),
	 ('cabd.dams_medium_large_view','condition_code','Dam Function',NULL,false),
	 ('cabd.dams_medium_large_view','dam_condition','Dam Condition',NULL,false),
	 ('cabd.dams_medium_large_view','construction_type_code','Construction Type Code',NULL,false),
	 ('cabd.dams_medium_large_view','construction_type','Construction Type',NULL,false),
	 ('cabd.dams_medium_large_view','height_m','Height (m)','Reported height of the dam in meters. Depending on the data source this could variably be height of dam wall, crest height, or head height.',false),
	 ('cabd.dams_medium_large_view','length_m','Length (m)','Length of the crest of the dam from one bank (or abutment) to the other in meters.',false),
	 ('cabd.dams_medium_large_view','size_class_code','Dam Size Class Code','The size classification of the dam based on the height of the dam in meters (dam_hgt_m).',false),
	 ('cabd.dams_medium_large_view','size_class','Dam Size',NULL,false),
	 ('cabd.dams_medium_large_view','spillway_capacity','Spillway Capacity','The designed capacity of the spillway in m3 per second.',false),
	 ('cabd.dams_medium_large_view','spillway_type_code','Spillway Type Code',NULL,false),
	 ('cabd.dams_medium_large_view','spillway_type','Spillway Type',NULL,false),
	 ('cabd.dams_medium_large_view','reservoir_present','Reservoir Present','Indicates if a reservoir is present due to the creation of the dam.',false),
	 ('cabd.dams_medium_large_view','reservoir_area_skm','Reservoir Area (sqm)','Representative surface area of reservoir in square kilometers.',false),
	 ('cabd.dams_medium_large_view','reservoir_depth_m','Reservoir Depth (m)','Average depth of reservoir in meters.',false),
	 ('cabd.dams_medium_large_view','storage_capacity_mcm','Storage Capacity (cm)','Storage capacity of reservoir in million cubic meters.',false),
	 ('cabd.dams_medium_large_view','avg_rate_of_discharge_ls','Average Rate Of Discharge (l/s)','Average rate of discharge at dam location in liters per second.',false),
	 ('cabd.dams_medium_large_view','degree_of_regulation_pc','Degree of Regulation','Degree of Regulation (DOR) in percent; equivalent to Ã¢â‚¬Å“residence timeÃ¢â‚¬ of water in the reservoir; calculated as ratio between storage capacity (Ã¢â‚¬ËœCap_mcmÃ¢â‚¬â„¢) and total annual flow (derived from Ã¢â‚¬ËœDis_avg_lsÃ¢â‚¬â„¢); values capped at 10,000 indicate exceedingly high values, which may be due to inconsistencies in the data and/or incorrect allocation to the river network and the associated discharges.',false),
	 ('cabd.dams_medium_large_view','provincial_flow_req','Provinical Flow Requirements (cm)','Legislated flow requirements for the dam structure in cubic meters per second regulated by the provincial licensing body.',false),
	 ('cabd.dams_medium_large_view','federal_flow_req','Federal Flow Requirements (cm)','Minimum flow recommendations for the dam structure in cubic meters per second. Based on assessments by Fisheries and Oceans Canada for the protection of fish and fish habitat.',false),
	 ('cabd.dams_medium_large_view','catchment_area_skm','Upstream Catchment Area (sqkm)','Area of upstream catchment draining into the reservoir in square kilometers. The area of upstream catchment is defined by Ã¢â‚¬Å“Elementary CatchmentÃ¢â‚¬ units in the National Hydrography Network.',false),
	 ('cabd.dams_medium_large_view','hydro_peaking_system','Has Hydro Peaking System','Indicates if the dam employs a hydro peaking system.',false),
	 ('cabd.dams_medium_large_view','generating_capacity_mwh','Generating Capacity (mwh)','The amount of electricity the hydroelectric facility can produce in megawatt hours.',false),
	 ('cabd.dams_medium_large_view','turbine_number','Number of Turbines','The number of turbines in the dam structure.',false),
	 ('cabd.dams_medium_large_view','turbine_type_code','Turbine Type Code',NULL,false),
	 ('cabd.dams_medium_large_view','turbine_type','Turbine Type',NULL,false),
	 ('cabd.dams_medium_large_view','up_passage_type_code','Upstream Passage Type Code',NULL,false),
	 ('cabd.dams_medium_large_view','up_passage_type','Upstream Passage Type',NULL,false),
	 ('cabd.dams_medium_large_view','down_passage_route_code','Downstream Passage Route Code',NULL,false),
	 ('cabd.dams_medium_large_view','down_passage_route','Downstream Passage Route',NULL,false),
	 ('cabd.dams_medium_large_view','owner','Owner','''Person, company, organization, government unit, public utility, corporation or other entity which either holds a water license to operate a dam or retains the legal property title on the dam site.'';',false),
	 ('cabd.dams_medium_large_view','ownership_type_code','Ownership Type Code',NULL,false),
	 ('cabd.dams_medium_large_view','ownership_type','Ownership Type',NULL,false),
	 ('cabd.dams_medium_large_view','province_compliance_status','Provincial Compliance Status','The status of regulatory compliance with provincial licensing body.',false),
	 ('cabd.dams_medium_large_view','federal_compliance_status','Federal Compliance Status','The status of regulatory compliance with the federal licensing body.',false);	 