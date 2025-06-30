-- drops and creates sites and structures table
-- source: tbl_structure_community_data_holding.xlsx
--
--notes:
-- downstream_direction_image and upstream_direction_image were duplicated
-- upstream_physical_blockages_code & downstream_physical_blockages_code are int arrays, postgresql does not support foreign keys on arrays
drop table if exists stream_crossings.assessment_data;
drop table if exists stream_crossings.assessment_structure_data;

create table stream_crossings.assessment_data(
    id uuid primary key not null,
    data_source_id uuid,
    cabd_assessment_id uuid,
    original_assessment_id varchar,    
    cabd_id uuid not null,
    other_id varchar, 
    date_assessed date,
    lead_assessor varchar,
    municipality varchar,
    stream_name varchar,
    road_name varchar,
    road_type_code integer references cabd.road_type_codes(code),
    latitude decimal,
    longitude decimal, 
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
    status stream_crossings.status_type
);
    
create table stream_crossings.assessment_structure_data(
    assessment_id uuid not null references stream_crossings.assessment_data(id),
    structure_number integer not null, 

    outlet_shape_code integer references stream_crossings.shape_codes(code),
    structure_material_code	integer[], --stream_crossings.material_codes
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
    physical_blockages_code integer[], --stream_crossings.blockage_type_codes
    physical_blockage_severity_code integer references stream_crossings.blockage_severity_codes(code),
    water_depth_matches_stream_code integer references stream_crossings.water_depth_matches_stream_codes(code),
    water_velocity_matches_stream_code integer references stream_crossings.water_velocity_matches_stream_codes(code),
    dry_passage_code integer references cabd.response_codes(code),
    height_above_dry_passage_m numeric, 
    structure_comments varchar,
    passability_status_code integer references cabd.passability_status_codes(code),
    
    primary key (assessment_id, structure_number)
);
