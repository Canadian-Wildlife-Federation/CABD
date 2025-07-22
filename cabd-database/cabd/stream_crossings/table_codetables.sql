-- supporting data types

drop type if exists stream_crossings.status_type;
-- Enum type columns appear as dropdowns when the layer is pulled into QGIS.
-- Unfortunately, NULL is not an option that can appear in a dropdown in QGIS but the type can be 
-- set to NULL. One way to get around this is to have a 'NULL' option in the enum and a trigger to set this value to NULL
create type stream_crossings.status_type as enum('NEW', 'REVIEWED', 'PROCESSED', 'ERROR/WARNING', 'REQUIRES CLARIFICATION');

-- this type already exists in the database;created with the stream_crossings.satellite_review table
-- CREATE TYPE stream_crossings.new_crossing_type AS ENUM('open-bottom structure', 'closed-bottom structure', 'multiple closed-bottom structure', 'ford-like structure', 'no crossing', 'removed crossing', 'NULL');
-- drop type if exists stream_crossings.new_crossing_type;


-- CREATE various code tables and populates them
-- source: codes_tables.xlsx

create table stream_crossings.incomplete_assessment_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
);
insert into stream_crossings.incomplete_assessment_codes (code, name_en) values
(1,'buried stream'),
(2,'inaccessible'),
(3,'partially inaccessible'),
(4,'no upstream channel'),
(5,'bridge adequate');

create table stream_crossings.slope_method_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.slope_method_codes (code, name_en) values
(1,'rangefinder/hypsometer'),
(2,'auto level'),
(3,'clinometer'),
(4,'stadia rod'),
(5,'other');


create table stream_crossings.structure_coverage_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.structure_coverage_codes (code, name_en) values
(1,'none'),
(2,'<25%'),
(3,'25%-50%'),
(4,'51%-75%'),
(5,'76%-99%'),
(6,'100%'),
(99,'unknown');


create table stream_crossings.blockage_severity_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.blockage_severity_codes (code, name_en) values
(1,'none'),
(2,'minor'),
(3,'moderate'),
(4,'severe'),
(5,'no aquatic passage');

create table stream_crossings.obs_constriction_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.obs_constriction_codes (code, name_en) values
(1,'no - structure is wider than the stream'),
(2,'no - strucutre is the same width as the stream'),
(3,'yes - structure narrows stream'),
(4,'unsure');


create table stream_crossings.outlet_drop_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.outlet_drop_codes (code, name_en) values
(1,'>30 cm'),
(2,'<30 cm');
	

create table stream_crossings.cbs_constriction_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.cbs_constriction_codes (code, name_en) values
(1,'larger'),
(2,'equal'),
(3,'smaller - less than half'),
(4,'smaller - greater than half'),
(5,'unsure');



create table stream_crossings.ford_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

insert into stream_crossings.ford_type_codes (code, name_en) values
(1,'stream-bed'),
(2,'engineered'),
(3,'unsure');


-- existing below ---

create table stream_crossings.crossing_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
);
insert into stream_crossings.crossing_type_codes (code, name_en) values
(1,'open-bottom structure'),
(2,'closed-bottom structure'),
(3,'multiple closed-bottom structures'),
(4,'ford-like structure'),
(5,'no crossing'),
(6,'removed crossing'),
(99,'unknown');
	

create table stream_crossings.flow_condition_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
);
insert into stream_crossings.flow_condition_codes (code, name_en) values
(1,'dry'),
(2,'unusually low'),
(3,'typical low'),
(4,'moderate'),
(5,'high');
	

create table stream_crossings.crossing_condition_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
);    
insert into stream_crossings.crossing_condition_codes (code, name_en) values
(1,'new'),
(2,'ok'),
(3,'poor'),
(4,'failing'),
(99,'unknown');
	
create table stream_crossings.site_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.site_type_codes (code, name_en) values
(1,'tidal'),
(2,'nontidal'),
(99,'unknown');


create table stream_crossings.alignment_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.alignment_codes (code, name_en) values
(1,'flow aligned'),
(2,'skewed');



create table stream_crossings.confidence_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.confidence_codes (code, name_en) values
(1,'high'),
(2,'low'),
(3,'estimated');


create table stream_crossings.scour_pool_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.scour_pool_codes (code, name_en) values
(1,'none'),
(2,'small'),
(3,'large'),
(99,'yes-extent unknown');


create table stream_crossings.shape_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.shape_codes (code, name_en) values
(1,'round culvert'),
(2,'closed-bottom pipe arch culvert'),
(3,'closed-bottom elliptical culvert'),
(4,'open bottom arch bridge/culvert'),
(5,'box culvert'),
(6,'bridge with side slopes'),
(7,'box/bridge with abutments'),
(8,'bridge with abutments and side slopes'),
(9,'ford'),
(10,'removed'),
(99,'unknown');


create table stream_crossings.material_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.material_codes (code, name_en) values
(1,'concrete'),
(2,'rock/stone'),
(3,'wood'),
(4,'metal (smooth)'),
(5,'metal (corrugated annular)'),
(6,'metal (corrugated spiral)'),
(7,'plastic (smooth)'),
(8,'plastic (smooth)'),
(9,'plastic (corrugated)'),
(10,'fiberglass');


create table stream_crossings.internal_structure_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.internal_structure_codes (code, name_en) values
(1,'liner'),
(2,'baffles/weirs'),
(3,'supports'),
(4,'none'),
(5,'other');


create table stream_crossings.armouring_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.armouring_codes (code, name_en) values
(1,'none'),
(2,'not extensive'),
(3,'extensive'),
(99,'yes-extent unknown');


create table stream_crossings.grade_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.grade_codes (code, name_en) values
(1,'at stream grade'),
(2,'free fall'),
(3,'cascade'),
(4,'free fall onto cascade'),
(5,'clogged/collapsed/submerged'),
(6,'inlet drop'),
(7,'perched'),
(99,'unknown');


create table stream_crossings.inlet_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.inlet_type_codes (code, name_en) values
(1,'projecting'),
(2,'headwall'),
(3,'wingwalls'),
(4,'headwall and wingwalls'),
(5,'mitered to slope'),
(6,'flush'),
(7,'other'),
(8,'none');

create table stream_crossings.relative_slope_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.relative_slope_codes (code, name_en) values
(1,'comparable'),
(2,'substantially flatter'),
(3,'substantially steeper');


create table stream_crossings.substrate_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.substrate_type_codes (code, name_en) values
(1,'none'),
(2,'silt'),
(3,'sand'),
(4,'gravel'),
(5,'cobble'),
(6,'boulder'),
(7,'bedrock'),
(99,'unknown');
	

create table stream_crossings.substrate_matches_stream_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.substrate_matches_stream_codes (code, name_en) values
(1,'comparable'),
(2,'contrasting'),
(3,'significantly contrasting'),
(4,'none'),
(99,'unknown');




create table stream_crossings.water_depth_matches_stream_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.water_depth_matches_stream_codes (code, name_en) values
(1,'yes'),
(2,'no-shallower'),
(3,'no-deeper'),
(4,'dry'),
(99,'unknown');



create table stream_crossings.water_velocity_matches_stream_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 
insert into stream_crossings.water_velocity_matches_stream_codes (code, name_en) values
(1,'yes'),
(2,'no-faster'),
(3,'no-slower'),
(4,'dry'),
(99,'unknown');






create table cabd.feature_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

create table cabd.response_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

create table cabd.no_access_reason_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

create table cabd.road_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

create table cabd.access_method_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

create table cabd.blockage_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

create table cabd.flowing_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

create table cabd.passability_status_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

create table cabd.assessment_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

create table cabd.addressed_status_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 


-- CABD
insert into cabd.feature_type_codes (code, name_en) values
(1,'Stream Crossing'),
(2,'Dam'),
(3,'No Structure'),
(4,'No Access');
	
insert into cabd.response_codes (code, name_en) values
(1,'yes'),
(2,'no'),
(3,'unsure'),
(99,'unknown');
	

insert into cabd.no_access_reason_codes (code, name_en) values
(1,'Safety Concerns'),
(2,'Fencing'),
(3,'Private Property'),
(4,'Difficult Terrain'),
(5,'Impassable Road'),
(6,'Other');

insert into cabd.road_type_codes (code, name_en) values	
(1,'multilane'),
(2,'1 or 2 lane paved'),
(3,'unpaved'),
(4,'driveway'),
(5,'trail'),
(6,'railroad (active)'),
(7,'railroad (inactive)');
	
insert into cabd.access_method_codes (code, name_en) values	
(1,'adjacent road/trail'),
(2,'on foot'),
(3,'boat/canoe'),
(4,'wading');

insert into cabd.blockage_type_codes (code, name_en) values	
(1,'none'),
(2,'debris/sediment/rock'),
(3,'deformation'),
(4,'free fall (no specified)'),
(5,'human-made free fall'),
(6,'natural free fall'),
(7,'fencing'),
(8,'dry/subsurface'),
(9,'beaver dam'),
(10,'other');

insert into cabd.flowing_codes (code, name_en) values	
(1,'no_dry'),
(2,'yes_standing'),
(3,'yes_moving');

insert into cabd.passability_status_codes (code, name_en) values	
(1,'Barrier'),
(2,'Partial Barrier'),
(3,'Passable'),
(4,'Unknown'),
(5,'NA - No Structure'),
(6,'NA - Decommissioned/Removed');

insert into cabd.assessment_type_codes (code, name_en) values	
(1,'rapid'),
(2,'full'),
(3,'other'),
(4,'imagery'),
(5,'modelled'),
(6,'inventory');

insert into cabd.addressed_status_codes (code, name_en) values	
(1,'decommissioned/removed'),
(2,'replaced'),
(3,'fish ladder'),
(4,'baffling'),
(5,'backwatering'),
(6,'other'),
(99,'unknown');




create table dams.side_channel_bypass_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar,
  description_fr  varchar
); 

insert into dams.side_channel_bypass_codes	(code, name_en) values
(1,'Through'),
(2,'Around'),
(3,'Over'),
(4,'Dry - No water passing');
