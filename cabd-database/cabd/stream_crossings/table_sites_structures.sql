-- drops and creates sites and structures table
-- source: tbl_structure_stream_crossings_sites.xlsx & tbl_structure_stream_crossings_structures.xlsx

-- notes:
-- include_in_act -? I've setup as generated

drop table if exists stream_crossings.sites_attribute_source;
drop table if exists stream_crossings.structures_attribute_source;
drop table if exists stream_crossings.structures;
drop table if exists stream_crossings.sites;

create table stream_crossings.sites(
    cabd_id uuid not null primary key,
    last_modified timestamp, --set on insert/update via trigger below
    other_id varchar,
    cabd_assessment_id uuid,
    original_assessment_id varchar ,
    date_assessed date,
    lead_assessor varchar,
    municipality varchar,
    stream_name varchar,
    road_name varchar,
    road_type_code integer references cabd.road_type_codes (code),
    location_description varchar,
    land_ownership_context varchar,
    incomplete_assess_code integer references stream_crossings.incomplete_assessment_codes(code),
    crossing_type_code integer references stream_crossings.crossing_type_codes(code),
    num_structures integer,
    photo_id_inlet varchar,
    photo_id_outlet varchar,
    photo_id_upstream varchar,
    photo_id_downstream varchar,
    photo_id_road_surface varchar,
    photo_id_other_a varchar,
    photo_id_other_b varchar,
    photo_id_other_c varchar,
    flow_condition_code integer references stream_crossings.flow_condition_codes(code),
    crossing_condition_code integer references stream_crossings.crossing_condition_codes(code),
    site_type_code integer references stream_crossings.site_type_codes(code),
    alignment_code integer references stream_crossings.alignment_codes(code),
    road_fill_height_m numeric,
    bankfull_width_upstr_a_m numeric,
    bankfull_width_upstr_b_m numeric,
    bankfull_width_upstr_c_m numeric,
    bankfull_width_upstr_avg_m numeric,
    bankfull_width_dnstr_a_m numeric,
    bankfull_width_dnstr_b_m numeric,
    bankfull_width_dnstr_c_m numeric,
    bankfull_width_dnstr_avg_m numeric,
    bankfull_confidence_code integer references stream_crossings.confidence_codes(code),
    scour_pool_tailwater_code integer references stream_crossings.scour_pool_codes(code),
    crossing_comments varchar,
    original_point geometry (point, 4617), 
    snapped_point geometry(point, 4617), --auto updated based on spatial query
    snapped_ncc geometry(point, 3979), --auto updated based on spatial query
    province_territory_code varchar, --auto updated based on spatial query
    nhn_watershed_id varchar, --auto updated based on spatial query
    strahler_order integer,
    assessment_type_code integer,
    addressed_status_code integer,
    chu_12_id varchar, --[auto updated based on spatial query]
    chu_10_id varchar, --[auto updated based on spatial query]
    chu_8_id varchar, --[auto updated based on spatial query]
    chu_6_id varchar, --[auto updated based on spatial query]
    chu_4_id varchar, --[auto updated based on spatial query]
    chu_2_id varchar, --[auto updated based on spatial query]
    include_in_act boolean GENERATED always as (case when snapped_ncc is not null then true else false end) stored   --IF sites.snapped_ncc IS NOT NULL THEN "TRUE"; ELSE "FALSE"
);

-- Index speeds up snapping function in init script
CREATE INDEX ON stream_crossings.sites USING GIST (original_point);


create table stream_crossings.sites_attribute_source(
    cabd_id uuid not null primary key references stream_crossings.sites(cabd_id),
    other_id_src varchar(1) check (other_id_src in ('m', 's', 'c', 'a')),
    other_id_dsid uuid,
    municipality_src varchar(1) check (municipality_src in ('m', 's', 'c', 'a')),
    municipality_dsid uuid,
    stream_name_src varchar(1) check (stream_name_src in ('m', 's', 'c', 'a')),
    stream_name_dsid uuid,
    road_name_src varchar(1) check (road_name_src in ('m', 's', 'c', 'a')),
    road_name_dsid uuid,
    road_type_code_src varchar(1) check (road_type_code_src in ('m', 's', 'c', 'a')),
    road_type_code_dsid uuid,
    location_description_src varchar(1) check (location_description_src in ('m', 's', 'c', 'a')),
    location_description_dsid uuid,
    land_ownership_context_src varchar(1) check (land_ownership_context_src in ('m', 's', 'c', 'a')),
    land_ownership_context_dsid uuid,
    incomplete_assess_code_src varchar(1) check (incomplete_assess_code_src in ('m', 's', 'c', 'a')),
    incomplete_assess_code_dsid uuid,
    crossing_type_code_src varchar(1) check (crossing_type_code_src in ('m', 's', 'c', 'a')),
    crossing_type_code_dsid uuid,
    num_structures_src varchar(1) check (num_structures_src in ('m', 's', 'c', 'a')),
    num_structures_dsid uuid,
    photo_id_inlet_src varchar(1) check (photo_id_inlet_src in ('m', 's', 'c', 'a')),
    photo_id_inlet_dsid uuid,
    photo_id_outlet_src varchar(1) check (photo_id_outlet_src in ('m', 's', 'c', 'a')),
    photo_id_outlet_dsid uuid,
    photo_id_upstream_src varchar(1) check (photo_id_upstream_src in ('m', 's', 'c', 'a')),
    photo_id_upstream_dsid uuid,
    photo_id_downstream_src varchar(1) check (photo_id_downstream_src in ('m', 's', 'c', 'a')),
    photo_id_downstream_dsid uuid,
    photo_id_road_surface_src varchar(1) check (photo_id_road_surface_src in ('m', 's', 'c', 'a')),
    photo_id_road_surface_dsid uuid,
    photo_id_other_a_src varchar(1) check (photo_id_other_a_src in ('m', 's', 'c', 'a')),
    photo_id_other_a_dsid uuid,
    photo_id_other_b_src varchar(1) check (photo_id_other_b_src in ('m', 's', 'c', 'a')),
    photo_id_other_b_dsid uuid,
    photo_id_other_c_src varchar(1) check (photo_id_other_c_src in ('m', 's', 'c', 'a')),
    photo_id_other_c_dsid uuid,
    flow_condition_code_src varchar(1) check (flow_condition_code_src in ('m', 's', 'c', 'a')),
    flow_condition_code_dsid uuid,
    crossing_condition_code_src varchar(1) check (crossing_condition_code_src in ('m', 's', 'c', 'a')),
    crossing_condition_code_dsid uuid,
    site_type_code_src varchar(1) check (site_type_code_src in ('m', 's', 'c', 'a')),
    site_type_code_dsid uuid,
    alignment_code_src varchar(1) check (alignment_code_src in ('m', 's', 'c', 'a')),
    alignment_code_dsid uuid,
    road_fill_height_m_src varchar(1) check (road_fill_height_m_src in ('m', 's', 'c', 'a')),
    road_fill_height_m_dsid uuid,
    bankfull_width_upstr_a_m_src varchar(1) check (bankfull_width_upstr_a_m_src in ('m', 's', 'c', 'a')),
    bankfull_width_upstr_a_m_dsid uuid,
    bankfull_width_upstr_b_m_src varchar(1) check (bankfull_width_upstr_b_m_src in ('m', 's', 'c', 'a')),
    bankfull_width_upstr_b_m_dsid uuid,
    bankfull_width_upstr_c_m_src varchar(1) check (bankfull_width_upstr_c_m_src in ('m', 's', 'c', 'a')),
    bankfull_width_upstr_c_m_dsid uuid,
    bankfull_width_upstr_avg_m_src varchar(1) check (bankfull_width_upstr_avg_m_src in ('m', 's', 'c', 'a')),
    bankfull_width_upstr_avg_m_dsid uuid,
    bankfull_width_dnstr_a_m_src varchar(1) check (bankfull_width_dnstr_a_m_src in ('m', 's', 'c', 'a')),
    bankfull_width_dnstr_a_m_dsid uuid,
    bankfull_width_dnstr_b_m_src varchar(1) check (bankfull_width_dnstr_b_m_src in ('m', 's', 'c', 'a')),
    bankfull_width_dnstr_b_m_dsid uuid,
    bankfull_width_dnstr_c_m_src varchar(1) check (bankfull_width_dnstr_c_m_src in ('m', 's', 'c', 'a')),
    bankfull_width_dnstr_c_m_dsid uuid,
    bankfull_width_dnstr_avg_m_src varchar(1) check (bankfull_width_dnstr_avg_m_src in ('m', 's', 'c', 'a')),
    bankfull_width_dnstr_avg_m_dsid uuid,
    bankfull_confidence_code_src varchar(1) check (bankfull_confidence_code_src in ('m', 's', 'c', 'a')),
    bankfull_confidence_code_dsid uuid,
    scour_pool_tailwater_code_src varchar(1) check (scour_pool_tailwater_code_src in ('m', 's', 'c', 'a')),
    scour_pool_tailwater_code_dsid uuid,
    crossing_comments_src varchar(1) check (crossing_comments_src in ('m', 's', 'c', 'a')),
    crossing_comments_dsid uuid, 
    original_point_src varchar(1) check (original_point_src in ('m', 's', 'c', 'a')),
    original_point_dsid uuid,
    strahler_order_src varchar(1) check (strahler_order_src in ('m', 's', 'c', 'a')),
    strahler_order_dsid uuid,
    assessment_type_code_src varchar(1) check (assessment_type_code_src in ('m', 's', 'c', 'a')),
    assessment_type_code_dsid uuid,
    addressed_status_code_src varchar(1) check (addressed_status_code_src in ('m', 's', 'c', 'a')),
    addressed_status_code_dsid uuid
);

create table stream_crossings.structures(
    last_modified timestamp, --set on update/insert via trigger below
    structure_id uuid not null primary key,  --[autogenerated]
    site_id uuid references stream_crossings.sites(cabd_id),
    cabd_assessment_id uuid,
    original_assessment_id varchar,
    primary_structure boolean,
    structure_number integer,
    outlet_shape_code integer references stream_crossings.shape_codes(code),
    structure_material_code integer[], -- stream_crossings.material_codes(code)
    internal_structures_code integer references stream_crossings.internal_structure_codes(code),
    liner_material_code integer references stream_crossings.material_codes(code),
    outlet_armouring_code integer references stream_crossings.armouring_codes(code),
    outlet_grade_code integer references stream_crossings.grade_codes(code),
    outlet_width_m numeric,
    outlet_height_m numeric,
    outlet_substrate_water_width_m numeric,
    outlet_water_depth_m numeric,
    abutment_height_m numeric,
    outlet_drop_to_water_surface_m numeric,
    outlet_drop_to_stream_bottom_m numeric, 
    outlet_water_surface_to_residual_pool_top_m numeric,
    residual_pool_confidence_code integer references stream_crossings.confidence_codes(code),
    structure_length_m numeric,
    inlet_shape_code integer references stream_crossings.shape_codes(code),
    inlet_type_code integer references stream_crossings.inlet_type_codes(code),
    inlet_grade_code integer references stream_crossings.grade_codes(code),
    inlet_width_m numeric,
    inlet_height_m numeric,
    inlet_substrate_water_width_m numeric,
    inlet_water_depth_m numeric,
    structure_slope_pct numeric,
    structure_slope_method_code integer references stream_crossings.slope_method_codes(code),
    structure_slope_to_channel_code integer references stream_crossings.relative_slope_codes(code),
    substrate_type_code integer references stream_crossings.substrate_type_codes(code),
    substrate_matches_stream_code integer references stream_crossings.substrate_matches_stream_codes(code),
    substrate_coverage_code integer references stream_crossings.structure_coverage_codes(code),
    substrate_depth_consistent_code integer references cabd.response_codes(code),
    backwatered_pct_code integer references stream_crossings.structure_coverage_codes(code),
    physical_blockages_code int[], -- stream_crossings.blockage_type_codes(code),
    physical_blockage_severity_code integer references stream_crossings.blockage_severity_codes(code),
    water_depth_matches_stream_code integer references stream_crossings.water_depth_matches_stream_codes(code),
    water_velocity_matches_stream_code integer references stream_crossings.water_velocity_matches_stream_codes(code),
    dry_passage_code integer references cabd.response_codes(code),
    height_above_dry_passage_m numeric,
    structure_comments varchar,
    passability_status_code integer
);


create table stream_crossings.structures_attribute_source(    
  structure_id uuid not null primary key references stream_crossings.structures(structure_id),
  cabd_assessment_id_src varchar(1) check (cabd_assessment_id_src in ('m', 's', 'c', 'a')),
  cabd_assessment_id_dsid uuid,
  original_assessment_id_src varchar(1) check (original_assessment_id_src in ('m', 's', 'c', 'a')),
  original_assessment_id_dsid uuid,
  primary_structure_src varchar(1) check (primary_structure_src in ('m', 's', 'c', 'a')),
  primary_structure_dsid uuid,
  structure_number_src varchar(1) check (structure_number_src in ('m', 's', 'c', 'a')),
  structure_number_dsid uuid,
  outlet_shape_code_src varchar(1) check (outlet_shape_code_src in ('m', 's', 'c', 'a')),
  outlet_shape_code_dsid uuid,
  structure_material_code_src varchar(1) check (structure_material_code_src in ('m', 's', 'c', 'a')),
  structure_material_code_dsid uuid,
  internal_structures_code_src varchar(1) check (internal_structures_code_src in ('m', 's', 'c', 'a')),
  internal_structures_code_dsid uuid,
  liner_material_code_src varchar(1) check (liner_material_code_src in ('m', 's', 'c', 'a')),
  liner_material_code_dsid uuid,
  outlet_armouring_code_src varchar(1) check (outlet_armouring_code_src in ('m', 's', 'c', 'a')),
  outlet_armouring_code_dsid uuid,
  outlet_grade_code_src varchar(1) check (outlet_grade_code_src in ('m', 's', 'c', 'a')),
  outlet_grade_code_dsid uuid,
  outlet_width_m_src varchar(1) check (outlet_width_m_src in ('m', 's', 'c', 'a')),
  outlet_width_m_dsid uuid,
  outlet_height_m_src varchar(1) check (outlet_height_m_src in ('m', 's', 'c', 'a')),
  outlet_height_m_dsid uuid,
  outlet_substrate_water_width_m_src varchar(1) check (outlet_substrate_water_width_m_src in ('m', 's', 'c', 'a')),
  outlet_substrate_water_width_m_dsid uuid,
  outlet_water_depth_m_src varchar(1) check (outlet_water_depth_m_src in ('m', 's', 'c', 'a')),
  outlet_water_depth_m_dsid uuid,
  abutment_height_m_src varchar(1) check (abutment_height_m_src in ('m', 's', 'c', 'a')),
  abutment_height_m_dsid uuid,
  outlet_drop_to_water_surface_m_src varchar(1) check (outlet_drop_to_water_surface_m_src in ('m', 's', 'c', 'a')),
  outlet_drop_to_water_surface_m_dsid uuid,
  outlet_drop_to_stream_bottom_m_src varchar(1) check (outlet_drop_to_stream_bottom_m_src in ('m', 's', 'c', 'a')),
  outlet_drop_to_stream_bottom_m_dsid uuid,
  outlet_water_surface_to_residual_pool_top_m_src varchar(1) check (outlet_water_surface_to_residual_pool_top_m_src in ('m', 's', 'c', 'a')),
  outlet_water_surface_to_residual_pool_top_m_dsid uuid,
  residual_pool_confidence_code_src varchar(1) check (residual_pool_confidence_code_src in ('m', 's', 'c', 'a')),
  residual_pool_confidence_code_dsid uuid,
  structure_length_m_src varchar(1) check (structure_length_m_src in ('m', 's', 'c', 'a')),
  structure_length_m_dsid uuid,
  inlet_shape_code_src varchar(1) check (inlet_shape_code_src in ('m', 's', 'c', 'a')),
  inlet_shape_code_dsid uuid,
  inlet_type_code_src varchar(1) check (inlet_type_code_src in ('m', 's', 'c', 'a')),
  inlet_type_code_dsid uuid,
  inlet_grade_code_src varchar(1) check (inlet_grade_code_src in ('m', 's', 'c', 'a')),
  inlet_grade_code_dsid uuid,
  inlet_width_m_src varchar(1) check (inlet_width_m_src in ('m', 's', 'c', 'a')),
  inlet_width_m_dsid uuid,
  inlet_height_m_src varchar(1) check (inlet_height_m_src in ('m', 's', 'c', 'a')),
  inlet_height_m_dsid uuid,
  inlet_substrate_water_width_m_src varchar(1) check (inlet_substrate_water_width_m_src in ('m', 's', 'c', 'a')),
  inlet_substrate_water_width_m_dsid uuid,
  inlet_water_depth_m_src varchar(1) check (inlet_water_depth_m_src in ('m', 's', 'c', 'a')),
  inlet_water_depth_m_dsid uuid,
  structure_slope_pct_src varchar(1) check (structure_slope_pct_src in ('m', 's', 'c', 'a')),
  structure_slope_pct_dsid uuid,
  structure_slope_method_code_src varchar(1) check (structure_slope_method_code_src in ('m', 's', 'c', 'a')),
  structure_slope_method_code_dsid uuid,
  structure_slope_to_channel_code_src varchar(1) check (structure_slope_to_channel_code_src in ('m', 's', 'c', 'a')),
  structure_slope_to_channel_code_dsid uuid,
  substrate_type_code_src varchar(1) check (substrate_type_code_src in ('m', 's', 'c', 'a')),
  substrate_type_code_dsid uuid,
  substrate_matches_stream_code_src varchar(1) check (substrate_matches_stream_code_src in ('m', 's', 'c', 'a')),
  substrate_matches_stream_code_dsid uuid,
  substrate_coverage_code_src varchar(1) check (substrate_coverage_code_src in ('m', 's', 'c', 'a')),
  substrate_coverage_code_dsid uuid,
  substrate_depth_consistent_code_src varchar(1) check (substrate_depth_consistent_code_src in ('m', 's', 'c', 'a')),
  substrate_depth_consistent_code_dsid uuid,
  backwatered_pct_code_src varchar(1) check (backwatered_pct_code_src in ('m', 's', 'c', 'a')),
  backwatered_pct_code_dsid uuid,
  physical_blockages_code_src varchar(1) check (physical_blockages_code_src in ('m', 's', 'c', 'a')),
  physical_blockages_code_dsid uuid,
  physical_blockage_severity_code_src varchar(1) check (physical_blockage_severity_code_src in ('m', 's', 'c', 'a')),
  physical_blockage_severity_code_dsid uuid,
  water_depth_matches_stream_code_src varchar(1) check (water_depth_matches_stream_code_src in ('m', 's', 'c', 'a')),
  water_depth_matches_stream_code_dsid uuid,
  water_velocity_matches_stream_code_src varchar(1) check (water_velocity_matches_stream_code_src in ('m', 's', 'c', 'a')),
  water_velocity_matches_stream_code_dsid uuid,
  dry_passage_code_src varchar(1) check (dry_passage_code_src in ('m', 's', 'c', 'a')),
  dry_passage_code_dsid uuid,
  height_above_dry_passage_m_src varchar(1) check (height_above_dry_passage_m_src in ('m', 's', 'c', 'a')),
  height_above_dry_passage_m_dsid uuid,
  structure_comments_src varchar(1) check (structure_comments_src in ('m', 's', 'c', 'a')),
  structure_comments_dsid uuid,
  passability_status_code_src varchar(1) check (passability_status_code_src in ('m', 's', 'c', 'a')),
  passability_status_code_dsid uuid
);    



-- last modified on sites
CREATE OR REPLACE FUNCTION set_last_modified()
RETURNS TRIGGER AS $$
BEGIN
  NEW.last_modified := CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER stream_crossings_sites_last_modified_trg
BEFORE INSERT OR UPDATE ON stream_crossings.sites
FOR EACH ROW EXECUTE FUNCTION set_last_modified();

CREATE TRIGGER stream_crossings_structures_last_modified_trg
BEFORE INSERT OR UPDATE ON stream_crossings.structures
FOR EACH ROW EXECUTE FUNCTION set_last_modified();