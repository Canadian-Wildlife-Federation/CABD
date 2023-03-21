drop schema if exists stream_crossings cascade;
create schema stream_crossings;

create table stream_crossings.relative_slope_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.relative_slope_codes (code, name_en) values
(1, 'comparable'),
(2, 'substantially flatter'),
(3, 'substantially steeper');

create table stream_crossings.relative_water_depth_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.relative_water_depth_codes (code, name_en, name_fr) values
(1, '<0.10', '<0.10'),
(2, '0.10-0.24', '0.10-0.24'),
(3, '0.25-0.49', '0.25-0.49'),
(4, '0.50-0.74', '0.50-0.74'),
(5, '0.75-0.99', '0.75-0.99'),
(6, '>=1.0', '>=1.0');


create table stream_crossings.tide_gate_severity_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.tide_gate_severity_codes (code, name_en) values
(1, 'no tide gate'),
(2, 'minor'),
(3, 'moderate'),
(4, 'severe'),
(5, 'no aquatic passage');


create table stream_crossings.tide_gate_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.tide_gate_codes (code, name_en) values
(1, 'no tide gate'),
(2, 'stop logs'),
(3, 'flap gate'),
(4, 'sluice gate'),
(5, 'self-regulating'),
(6, 'other'),
(99, 'yes-type unknown');


create table stream_crossings.water_velocity_matches_stream_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.water_velocity_matches_stream_codes (code, name_en) values
(1, 'yes'),
(2, 'no-faster'),
(3, 'no-slower'),
(4, 'dry'),
(99, 'unknown');



create table stream_crossings.water_depth_matches_stream_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.water_depth_matches_stream_codes (code, name_en) values
(1, 'yes'),
(2, 'no-shallower'),
(3, 'no-deeper'),
(4, 'dry'),
(99, 'unknown');

create table stream_crossings.physical_barrier_severity_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.physical_barrier_severity_codes (code, name_en) values
(1, 'none'),
(2, 'minor'),
(3, 'moderate'),
(4, 'severe'),
(5, 'no aquatic passage');


create table stream_crossings.physical_barrier_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.physical_barrier_codes (code, name_en) values
(1, 'none'),
(2, 'sediment blockage'),
(3, 'debris'),
(4, 'deformation'),
(5, 'free falls'),
(6, 'fencing'),
(7, 'dry'),
(8, 'pipes'),
(9, 'other');


create table stream_crossings.substrate_coverage_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.substrate_coverage_codes (code, name_en) values
(1, 'none'),
(2, '<25%'),
(3, '25%-49%'),
(4, '50%-74%'),
(5, '75%-99%'),
(6, '100%'),
(99, 'unknown');


create table stream_crossings.substrate_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.substrate_type_codes (code, name_en) values
(1, 'none'),
(2, 'muck/silt'),
(3, 'sand'),
(4, 'gravel'),
(5, 'cobble'),
(6, 'boulder'),
(7, 'bedrock'),
(99, 'unknown');

create table stream_crossings.substrate_matches_stream_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.substrate_matches_stream_codes (code, name_en) values
(1, 'comparable'),
(2, 'contrasting'),
(3, 'not appropriate'),
(4, 'none'),
(99, 'unknown');


create table stream_crossings.internal_structure_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.internal_structure_codes (code, name_en) values
(1, 'baffles/weirs'),
(2, 'supports'),
(3, 'none'),
(4, 'other');


create table stream_crossings.inlet_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.inlet_type_codes (code, name_en) values
(1, 'projecting'),
(2, 'headwall'),
(3, 'wingwalls'),
(4, 'headwall and wingwalls'),
(5, 'mitered to slope'),
(6, 'flush'),
(7, 'other');


create table stream_crossings.grade_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.grade_codes (code, name_en) values
(1, 'at stream grade'),
(2, 'free fall'),
(3, 'cascade'),
(4, 'free fall onto cascade'),
(5, 'clogged/collapsed/submerged'),
(6, 'inlet drop'),
(7, 'perched'),
(99, 'unknown');

create table stream_crossings.armouring_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.armouring_codes (code, name_en) values
(1, 'none'),
(2, 'not extensive'),
(3, 'extensive'),
(99, 'yes-extent unknown');

create table stream_crossings.shape_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.shape_codes (code, name_en) values
(1, 'round culvert'),
(2, 'pipe arch/elliptical culvert'),
(3, 'open bottom arch bridge/culvert'),
(4, 'box culvert'),
(5, 'bridge with side slopes'),
(6, 'box/bridge with abutments'),
(7, 'bridge with abutments and side slopes'),
(8, 'ford'),
(9, 'removed'),
(10, 'clogged/collapsed/submerged'),
(99, 'unknown');



create table stream_crossings.material_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.material_codes (code, name_en) values
(1, 'concrete'),
(2, 'stone'),
(3, 'wood'),
(4, 'metal'),
(5, 'metal (smooth)'),
(6, 'metal (corrugated)'),
(7, 'plastic'),
(8, 'plastic (smooth)'),
(9, 'plastic (corrugated)'),
(10, 'other');



create table stream_crossings.vegetation_change_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.vegetation_change_codes (code, name_en, description_en) values
(1, 'comparable', 'vegetative structure and species composition are not noticeably different'),
(2, 'slightly different', 'differences in vegetative structure and species composition are evident, but small'),
(3, 'moderately different', 'differences in vegetative structure and species composition are obvious and substantial, but similarities remain'),
(4, 'very different', 'vegetative structure and species composition are so different that different vegetative communities occur above and below the crossing. This typically occurs where there is a salt marsh below and a freshwater wetland above the crossing.'),
(99, 'unknown', 'impossible to assess vegetation above and/or below the crossing, due to time of year or lack of a vantage point for observations.');


create table stream_crossings.visible_utilities_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.visible_utilities_codes (code, name_en, description_en) values
(1, 'overhead wires', 'electrical, telephone or cable wires.'),
(2, 'water/sewer pipes', 'visible pipes carrying drinking water, storm water or waste water either in or on the substrate, suspended within the structure(s), or otherwise associated with the crossing.'),
(3, 'gas line', 'pipes for natural gas (often not visible at the surface and only indicated by markers).'),
(4, 'other', 'other type of utility installation not covered above.');

create table stream_crossings.stream_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.stream_type_codes (code, name_en) values
(1, 'salt marsh creek'),
(2, 'salt/brackish flow-through stream'),
(3, 'freshwater tidal');


create table stream_crossings.tide_stage_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.tide_stage_codes (code, name_en) values
(1, 'low slack tide '),
(2, 'low ebb tide '),
(3, 'low flood tide '),
(4, 'other'),
(99, 'unknown');



create table stream_crossings.scour_pool_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.scour_pool_codes (code, name_en) values
(1, 'none'),
(2, 'small'),
(3, 'large'),
(99, 'yes-extent unknown');


create table stream_crossings.constriction_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.constriction_codes (code, name_en) values
(1, 'severe'),
(2, 'moderate'),
(3, 'spans only bankfull/active channel'),
(4, 'spans full channel and banks');


create table stream_crossings.confidence_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.confidence_codes (code, name_en) values
(1, 'high'),
(2, 'low');


create table stream_crossings.alignment_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.alignment_codes (code, name_en) values
(1, 'flow-aligned'),
(2, 'skewed');


create table stream_crossings.crossing_condition_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.crossing_condition_codes (code, name_en) values
(1, 'new'),
(2, 'ok'),
(3, 'poor'),
(4, 'failing'),
(99, 'unknown');


create table stream_crossings.flow_condition_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.flow_condition_codes (code, name_en) values
(1, 'dewatered'),
(2, 'unusually low'),
(3, 'typical low flow'),
(4, 'moderate flow'),
(5, 'high flow');

create table stream_crossings.crossing_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.crossing_type_codes (code, name_en) values
(1, 'bridge'),
(2, 'culvert'),
(3, 'multiple culvert'),
(4, 'ford'),
(5, 'no crossing'),
(6, 'removed crossing'),
(7, 'buried stream'),
(8, 'inaccessible'),
(9, 'partially inaccessible'),
(10, 'no upstream channel'),
(11, 'bridge adequate');


create table stream_crossings.road_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.road_type_codes (code, name_en) values
(1, 'multilane'),
(2, 'paved'),
(3, 'unpaved'),
(4, 'driveway'),
(5, 'road'),
(6, 'resource road'),
(7, 'trail'),
(8, 'railroad');

create table stream_crossings.site_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into stream_crossings.site_type_codes (code, name_en) values
(1, 'tidal'),
(2, 'nontidal'),
(99, 'unknown');



create table stream_crossings.nontidal_sites(
  cabd_id uuid primary key,
  data_source_id uuid,
  cabd_assessment_id uuid,
  original_assessment_id varchar,
  date_observed date,
  lead_observer varchar(100000),
  town_county varchar(100000), 
  stream_name varchar(100000),
  strahler_order integer,  
  road_name varchar(100000),
  road_type_code integer references stream_crossings.road_type_codes(code),
  road_class varchar(100000),
  road_surface varchar(100000),
  transport_feature_owner varchar(100000),
  railway_operator varchar(100000),
  num_railway_tracks integer,
  railway_status varchar(100000),
  location_description varchar(100000),
  crossing_type_code integer references stream_crossings.crossing_type_codes(code),
  structure_count integer,
  photo_id_inlet uuid,
  photo_id_outlet uuid,
  photo_id_upstream uuid,
  photo_id_downstream uuid,
  photo_id_other uuid,
  flow_condition_code integer references stream_crossings.flow_condition_codes(code),
  crossing_condition_code integer references stream_crossings.crossing_condition_codes(code),
  site_type integer references stream_crossings.site_type_codes(code),
  alignment_code integer references stream_crossings.alignment_codes(code),
  road_fill_height_m numeric,
  upstream_channel_depth_m numeric,
  downstream_channel_depth_m numeric,
  upstream_bankfull_width_m numeric,
  downstream_bankfull_width_m numeric,
  upstream_bankfull_width_confidence_code integer references stream_crossings.confidence_codes(code),
  downstream_bankfull_width_confidence_code integer references stream_crossings.confidence_codes(code),
  constriction_code integer,
  tailwater_scour_pool_code integer references stream_crossings.scour_pool_codes(code),
  upstream_scour_pool_width_m numeric,
  downstream_scour_pool_width_m numeric,
  crossing_comments varchar(100000),
  original_point geometry(Point,4617),
  snapped_point_prov geometry(Point,4617),
  snapped_point geometry(Point,4617)
);
comment on column stream_crossings.nontidal_sites.stream_name is 'Stream name may come either from the CHyF hydro networks, or from assessment data.';



create table stream_crossings.nontidal_structures (
  structure_id uuid primary key,
  site_id uuid references stream_crossings.nontidal_sites (cabd_id),
  data_source_id uuid,
  cabd_assessment_id uuid,
  original_assessment_id varchar,
  primary_structure boolean,
  structure_number integer,
  outlet_shape_code integer references stream_crossings.shape_codes(code),
  outlet_armouring_code integer references stream_crossings.armouring_codes(code),
  outlet_grade_code integer references stream_crossings.grade_codes(code),
  outlet_width_m numeric,
  outlet_height_m numeric,
  structure_length_m numeric,
  outlet_substrate_water_width_m numeric,
  outlet_water_depth_m numeric,
  outlet_drop_to_water_surface_m numeric,
  outlet_drop_to_stream_bottom_m numeric,
  outlet_drop_residual_pool_m numeric,
  abutment_height_m numeric,
  inlet_shape_code integer references stream_crossings.shape_codes(code),
  inlet_type_code integer references stream_crossings.inlet_type_codes(code),
  inlet_grade_code integer references stream_crossings.grade_codes(code),
  inlet_width_m numeric,
  inlet_height_m numeric,
  inlet_substrate_water_width_m numeric,
  inlet_water_depth_m numeric,
  structure_slope_pct numeric,
  structure_slope_confidence_code integer references stream_crossings.confidence_codes(code),
  internal_structures_code integer references stream_crossings.internal_structure_codes(code),
  substrate_matches_stream_code integer references stream_crossings.substrate_matches_stream_codes(code),
  substrate_type_code integer references stream_crossings.substrate_type_codes(code),
  substrate_coverage_code integer references stream_crossings.substrate_coverage_codes(code),
  physical_barrier_severity_code integer references stream_crossings.physical_barrier_severity_codes(code),
  water_depth_matches_stream_code integer references stream_crossings.water_depth_matches_stream_codes(code),
  water_velocity_matches_stream_code integer references stream_crossings.water_velocity_matches_stream_codes(code),
  dry_passage boolean,
  height_above_dry_passage_m numeric,
  structure_comments varchar (100000),
  passability_status_code integer references cabd.passability_status_codes (code)
);

comment on column stream_crossings.nontidal_structures.data_source_id is 'Link to CABD data source';
comment on column stream_crossings.nontidal_structures.cabd_assessment_id is 'A unique id assigned to the original assessment record';
comment on column stream_crossings.nontidal_structures.original_assessment_id is 'The structure id from the original data source (eg. TB-001)';



create table stream_crossings.tidal_sites (
  cabd_id uuid primary key,
  data_source_id uuid,
  cabd_assessment_id uuid,
  original_assessment_id varchar,
  date_observed date,
  lead_observer varchar(100000),
  town_county varchar(100000),
  stream_name varchar(100000),
  strahler_order integer,
  road_name varchar(100000),
  road_type_code integer references stream_crossings.road_type_codes(code),
  road_class varchar(100000),
  road_surface varchar(100000),
  transport_feature_owner varchar(100000),
  railway_operator varchar(100000),
  num_railway_tracks integer,
  railway_status varchar(100000),
  location_description varchar(100000),
  crossing_type_code integer references stream_crossings.crossing_type_codes(code),
  structure_count integer,
  flow_condition_code integer references stream_crossings.flow_condition_codes(code),
  crossing_condition_code integer references stream_crossings.crossing_condition_codes(code),
  site_type integer references stream_crossings.site_type_codes(code),
  alignment_code integer references stream_crossings.alignment_codes(code),
  road_fill_height_m numeric,
  upstream_channel_depth_m numeric,
  downstream_channel_depth_m numeric,
  upstream_channel_width_m numeric,
  upstream_scour_pool_width_m numeric,
  downstream_channel_width_m numeric,
  downstream_scour_pool_width_m numeric,
  photo_id_inlet uuid,
  photo_id_outlet uuid,
  photo_id_upstream uuid,
  photo_id_downstream uuid,
  photo_id_other uuid,
  crossing_comments varchar(100000),
  tidal_tide_stage_code integer references stream_crossings.tide_stage_codes(code),
  tidal_low_tide_prediction time,
  tidal_tide_station integer,
  tidal_stream_type_code integer references stream_crossings.stream_type_codes(code),
  tidal_salinity_ppt integer,
  tidal_visible_utilities_code integer references stream_crossings.visible_utilities_codes(code),
  tidal_road_flooded_high_tide boolean,
  tidal_upstream_tidal_range_m numeric,
  tidal_downstream_tidal_range_m numeric,
  tidal_veg_change_code integer references stream_crossings.vegetation_change_codes(code),
  original_point geometry(Point,4617),
  snapped_point_prov geometry(Point,4617),
  snapped_point geometry(Point,4617)
);
comment on column stream_crossings.tidal_sites.stream_name is 'Stream name may come either from the CHyF hydro networks, or from assessment data.';

create table stream_crossings.tidal_structures (
  structure_id uuid primary key,
  site_id uuid references stream_crossings.tidal_sites(cabd_id),
  data_source_id uuid,
  cabd_assessment_id uuid,
  original_assessment_id varchar,
  primary_structure boolean,
  structure_number integer,
  tidal_tide_gate_type_code integer references stream_crossings.tide_gate_codes(code),
  tidal_tide_gate_barrier_severity_code integer references stream_crossings.tide_gate_severity_codes(code),
  outlet_shape_code integer references stream_crossings.shape_codes(code),
  outlet_width_m numeric,
  outlet_height_m numeric,
  outlet_substrate_water_width_m numeric,
  outlet_water_depth_m numeric,
  outlet_abutment_height_m numeric,
  outlet_high_tide_water_depth_m numeric,
  outlet_spring_tide_water_depth_m numeric,
  outlet_perch_low_tide_m numeric,
  outlet_perch_high_tide_m numeric,
  outlet_drop_height_residual_pool_m numeric,
  outlet_armouring_code integer references stream_crossings.armouring_codes(code),
  outlet_grade_code integer references stream_crossings.grade_codes(code),
  inlet_type_code integer references stream_crossings.inlet_type_codes(code),
  inlet_shape_code integer references stream_crossings.shape_codes(code),
  inlet_width_m numeric,
  inlet_height_m numeric,
  inlet_substrate_water_width_m numeric,
  inlet_water_depth_m numeric,
  inlet_abutment_height_m numeric,
  inlet_high_tide_water_depth_m numeric,
  inlet_spring_tide_water_depth_m numeric,
  inlet_perch_low_tide_m numeric,
  inlet_perch_high_tide_m numeric,
  inlet_armouring_code integer references stream_crossings.armouring_codes(code),
  inlet_grade_code integer references stream_crossings.grade_codes(code),
  structure_length_m numeric,
  tidal_relative_water_depth_code integer references stream_crossings.relative_water_depth_codes(code),
  substrate_type_code integer references stream_crossings.substrate_type_codes(code),
  substrate_matches_stream_code integer references stream_crossings.substrate_matches_stream_codes(code),
  substrate_coverage_code integer references stream_crossings.substrate_coverage_codes(code),
  tidal_relative_slope_code integer references stream_crossings.relative_slope_codes(code),
  physical_barrier_severity_code integer references stream_crossings.physical_barrier_severity_codes(code),
  dry_passage boolean,
  height_above_dry_passage_m numeric,
  structure_comments varchar,
  passability_status_code integer references cabd.passability_status_codes(code)
);

comment on column stream_crossings.tidal_structures.data_source_id is 'Link to CABD data source';
comment on column stream_crossings.tidal_structures.cabd_assessment_id is 'A unique id assigned to the original assessment record';
comment on column stream_crossings.tidal_structures.original_assessment_id is 'The structure id from the original data source (eg. TB-001)';


create table stream_crossings.tidal_material_mapping (
  structure_id uuid references stream_crossings.tidal_structures(structure_id),
  material_code integer not null references stream_crossings.material_codes(code),
  primary key (structure_id, material_code)
);
create table stream_crossings.nontidal_material_mapping (
  structure_id uuid references stream_crossings.nontidal_structures(structure_id),
  material_code integer not null references stream_crossings.material_codes(code),
  primary key (structure_id, material_code)
);


create table stream_crossings.tidal_physical_barrier_mapping (
  structure_id uuid not null references stream_crossings.tidal_structures(structure_id),
  physical_barrier_code integer not null references stream_crossings.physical_barrier_codes(code),
  primary key (structure_id, physical_barrier_code)
);
create table stream_crossings.nontidal_physical_barrier_mapping (
  structure_id uuid not null references stream_crossings.nontidal_structures(structure_id),
  physical_barrier_code integer not null references stream_crossings.physical_barrier_codes(code),
  primary key (structure_id, physical_barrier_code)
);
 
alter schema stream_crossings owner to cabd;

alter table stream_crossings.flow_condition_codes owner to cabd;
alter table stream_crossings.crossing_condition_codes owner to cabd;
alter table stream_crossings.site_type_codes owner to cabd;
alter table stream_crossings.alignment_codes owner to cabd;
alter table stream_crossings.confidence_codes owner to cabd;
alter table stream_crossings.scour_pool_codes owner to cabd;
alter table stream_crossings.nontidal_structures owner to cabd;
alter table stream_crossings.shape_codes owner to cabd;
alter table stream_crossings.armouring_codes owner to cabd;
alter table stream_crossings.grade_codes owner to cabd;
alter table stream_crossings.inlet_type_codes owner to cabd;
alter table stream_crossings.internal_structure_codes owner to cabd;
alter table stream_crossings.substrate_matches_stream_codes owner to cabd;
alter table stream_crossings.substrate_type_codes owner to cabd;
alter table stream_crossings.substrate_coverage_codes owner to cabd;
alter table stream_crossings.physical_barrier_severity_codes owner to cabd;
alter table stream_crossings.relative_slope_codes owner to cabd;
alter table stream_crossings.tidal_material_mapping owner to cabd;
alter table stream_crossings.material_codes owner to cabd;
alter table stream_crossings.nontidal_material_mapping owner to cabd;
alter table stream_crossings.tidal_physical_barrier_mapping owner to cabd;
alter table stream_crossings.physical_barrier_codes owner to cabd;
alter table stream_crossings.nontidal_physical_barrier_mapping owner to cabd;
alter table stream_crossings.constriction_codes owner to cabd;
alter table stream_crossings.road_type_codes owner to cabd;
alter table stream_crossings.nontidal_sites owner to cabd;
alter table stream_crossings.crossing_type_codes owner to cabd;
alter table stream_crossings.water_depth_matches_stream_codes owner to cabd;
alter table stream_crossings.water_velocity_matches_stream_codes owner to cabd;
alter table stream_crossings.tidal_sites owner to cabd;
alter table stream_crossings.tide_stage_codes owner to cabd;
alter table stream_crossings.stream_type_codes owner to cabd;
alter table stream_crossings.visible_utilities_codes owner to cabd;
alter table stream_crossings.vegetation_change_codes owner to cabd;
alter table stream_crossings.tidal_structures owner to cabd;
alter table stream_crossings.tide_gate_codes owner to cabd;
alter table stream_crossings.tide_gate_severity_codes owner to cabd;
alter table stream_crossings.relative_water_depth_codes owner to cabd;