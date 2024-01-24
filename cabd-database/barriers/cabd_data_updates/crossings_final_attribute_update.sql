---------------------------------------------------------
-- set primary structures where null
---------------------------------------------------------

with site_count AS
(select site_id, count(*)
from featurecopy.nontidal_structures
group by site_id
HAVING count(*) > 1),

sites AS (
SELECT site_id FROM site_count)

UPDATE featurecopy.nontidal_structures
SET primary_structure = true,
    structure_number = 1
WHERE site_id NOT IN (select site_id from sites)
AND primary_structure is null
and structure_number is null;


with site_count AS
(select site_id, count(*)
from featurecopy.tidal_structures
group by site_id
HAVING count(*) > 1),

sites AS (
SELECT site_id FROM site_count)

UPDATE featurecopy.tidal_structures
SET primary_structure = true,
    structure_number = 1
WHERE site_id NOT IN (select site_id from sites)
AND primary_structure is null
and structure_number is null;

---------------------------------------------------------
-- set passability status of primary structure where null
---------------------------------------------------------

UPDATE featurecopy.nontidal_structures
SET passability_status_code = (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
WHERE passability_status_code IS NULL
AND primary_structure IS TRUE;


UPDATE featurecopy.tidal_structures
SET passability_status_code = (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
WHERE passability_status_code IS NULL
AND primary_structure IS TRUE;

---------------------------------------------------------
-- set site_type to Unknown where NULL
---------------------------------------------------------

UPDATE featurecopy.nontidal_sites
SET site_type = (SELECT code FROM stream_crossings.site_type_codes WHERE name_en = 'unknown')
WHERE site_type IS NULL;

---------------------------------------------------------
-- set snapped geometries to original point where NULL
---------------------------------------------------------

UPDATE featurecopy.nontidal_sites
SET snapped_point = original_point
WHERE snapped_point IS NULL;

UPDATE featurecopy.nontidal_sites
SET snapped_point_prov = original_point
WHERE snapped_point_prov IS NULL;

UPDATE featurecopy.tidal_sites
SET snapped_point = original_point
WHERE snapped_point IS NULL;

UPDATE featurecopy.tidal_sites
SET snapped_point_prov = original_point
WHERE snapped_point_prov IS NULL;

---------------------------------------------------------
-- insert nontidal sites
---------------------------------------------------------

INSERT INTO stream_crossings.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    lead_observer,
    town_county,
    stream_name,
    strahler_order,
    road_name,
    road_type_code,
    road_class,
    road_surface,
    transport_feature_owner,
    railway_operator,
    num_railway_tracks,
    railway_status,
    location_description,
    crossing_type_code,
    structure_count,
    photo_id_inlet,
    photo_id_outlet,
    photo_id_upstream,
    photo_id_downstream,
    photo_id_other,
    flow_condition_code,
    crossing_condition_code,
    site_type,
    alignment_code,
    road_fill_height_m,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_bankfull_width_m,
    downstream_bankfull_width_m,
    upstream_bankfull_width_confidence_code,
    downstream_bankfull_width_confidence_code,
    constriction_code,
    tailwater_scour_pool_code,
    upstream_scour_pool_width_m,
    downstream_scour_pool_width_m,
    crossing_comments,
    original_point,
    snapped_point_prov,
    snapped_point
)
SELECT
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    lead_observer,
    town_county,
    stream_name,
    strahler_order,
    road_name,
    road_type_code,
    road_class,
    road_surface,
    transport_feature_owner,
    railway_operator,
    num_railway_tracks,
    railway_status,
    location_description,
    crossing_type_code,
    structure_count,
    photo_id_inlet,
    photo_id_outlet,
    photo_id_upstream,
    photo_id_downstream,
    photo_id_other,
    flow_condition_code,
    crossing_condition_code,
    site_type,
    alignment_code,
    road_fill_height_m,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_bankfull_width_m,
    downstream_bankfull_width_m,
    upstream_bankfull_width_confidence_code,
    downstream_bankfull_width_confidence_code,
    constriction_code,
    tailwater_scour_pool_code,
    upstream_scour_pool_width_m,
    downstream_scour_pool_width_m,
    crossing_comments,
    original_point,
    snapped_point_prov,
    snapped_point
FROM featurecopy.nontidal_sites;

---------------------------------------------------------
-- insert nontidal structures
---------------------------------------------------------

INSERT INTO stream_crossings.nontidal_structures (
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    primary_structure,
    structure_number,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    outlet_drop_residual_pool_m,
    abutment_height_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    structure_slope_pct,
    structure_slope_confidence_code,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments,
    passability_status_code
)
SELECT
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    primary_structure,
    structure_number,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    outlet_drop_residual_pool_m,
    abutment_height_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    structure_slope_pct,
    structure_slope_confidence_code,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments,
    passability_status_code
FROM featurecopy.nontidal_structures;

---------------------------------------------------------
-- insert nontidal mapping records
---------------------------------------------------------

INSERT INTO stream_crossings.nontidal_material_mapping (
    structure_id,
    material_code
)
SELECT
    structure_id,
    material_code
FROM
    featurecopy.nontidal_material_mapping;

INSERT INTO stream_crossings.nontidal_physical_barrier_mapping (
    structure_id,
    physical_barrier_code
)
SELECT
    structure_id,
    physical_barrier_code
FROM
    featurecopy.nontidal_physical_barrier_mapping;
    
---------------------------------------------------------
-- insert tidal sites
---------------------------------------------------------

INSERT INTO stream_crossings.tidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    lead_observer,
    town_county,
    stream_name,
    strahler_order,
    road_name,
    road_type_code,
    road_class,
    road_surface,
    transport_feature_owner,
    railway_operator,
    num_railway_tracks,
    railway_status,
    location_description,
    crossing_type_code,
    structure_count,
    flow_condition_code,
    crossing_condition_code,
    site_type,
    alignment_code,
    road_fill_height_m,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_channel_width_m,
    upstream_scour_pool_width_m,
    downstream_channel_width_m,
    downstream_scour_pool_width_m,
    photo_id_inlet,
    photo_id_outlet,
    photo_id_upstream,
    photo_id_downstream,
    photo_id_other,
    crossing_comments,
    tidal_tide_stage_code,
    tidal_low_tide_prediction,
    tidal_tide_station,
    tidal_stream_type_code,
    tidal_salinity_ppt,
    tidal_visible_utilities_code,
    tidal_road_flooded_high_tide,
    tidal_upstream_tidal_range_m,
    tidal_downstream_tidal_range_m,
    tidal_veg_change_code,
    original_point,
    snapped_point_prov,
    snapped_point
)
SELECT
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    lead_observer,
    town_county,
    stream_name,
    strahler_order,
    road_name,
    road_type_code,
    road_class,
    road_surface,
    transport_feature_owner,
    railway_operator,
    num_railway_tracks,
    railway_status,
    location_description,
    crossing_type_code,
    structure_count,
    flow_condition_code,
    crossing_condition_code,
    site_type,
    alignment_code,
    road_fill_height_m,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_channel_width_m,
    upstream_scour_pool_width_m,
    downstream_channel_width_m,
    downstream_scour_pool_width_m,
    photo_id_inlet,
    photo_id_outlet,
    photo_id_upstream,
    photo_id_downstream,
    photo_id_other,
    crossing_comments,
    tidal_tide_stage_code,
    tidal_low_tide_prediction,
    tidal_tide_station,
    tidal_stream_type_code,
    tidal_salinity_ppt,
    tidal_visible_utilities_code,
    tidal_road_flooded_high_tide,
    tidal_upstream_tidal_range_m,
    tidal_downstream_tidal_range_m,
    tidal_veg_change_code,
    original_point,
    snapped_point_prov,
    snapped_point
FROM featurecopy.tidal_sites;

---------------------------------------------------------
-- insert tidal structures
---------------------------------------------------------

INSERT INTO stream_crossings.tidal_structures (
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    primary_structure,
    structure_number,
    tidal_tide_gate_type_code,
    tidal_tide_gate_barrier_severity_code,
    outlet_shape_code,
    outlet_width_m,
    outlet_height_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_abutment_height_m,
    outlet_high_tide_water_depth_m,
    outlet_spring_tide_water_depth_m,
    outlet_perch_low_tide_m,
    outlet_perch_high_tide_m,
    outlet_drop_height_residual_pool_m,
    outlet_armouring_code,
    outlet_grade_code,
    inlet_type_code,
    inlet_shape_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    inlet_abutment_height_m,
    inlet_high_tide_water_depth_m,
    inlet_spring_tide_water_depth_m,
    inlet_perch_low_tide_m,
    inlet_perch_high_tide_m,
    inlet_armouring_code,
    inlet_grade_code,
    structure_length_m,
    tidal_relative_water_depth_code,
    substrate_type_code,
    substrate_matches_stream_code,
    substrate_coverage_code,
    tidal_relative_slope_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments,
    passability_status_code
)
SELECT
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    primary_structure,
    structure_number,
    tidal_tide_gate_type_code,
    tidal_tide_gate_barrier_severity_code,
    outlet_shape_code,
    outlet_width_m,
    outlet_height_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_abutment_height_m,
    outlet_high_tide_water_depth_m,
    outlet_spring_tide_water_depth_m,
    outlet_perch_low_tide_m,
    outlet_perch_high_tide_m,
    outlet_drop_height_residual_pool_m,
    outlet_armouring_code,
    outlet_grade_code,
    inlet_type_code,
    inlet_shape_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    inlet_abutment_height_m,
    inlet_high_tide_water_depth_m,
    inlet_spring_tide_water_depth_m,
    inlet_perch_low_tide_m,
    inlet_perch_high_tide_m,
    inlet_armouring_code,
    inlet_grade_code,
    structure_length_m,
    tidal_relative_water_depth_code,
    substrate_type_code,
    substrate_matches_stream_code,
    substrate_coverage_code,
    tidal_relative_slope_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments,
    passability_status_code
FROM featurecopy.tidal_structures;

---------------------------------------------------------
-- insert tidal mapping records
---------------------------------------------------------

INSERT INTO stream_crossings.tidal_material_mapping (
    structure_id,
    material_code
)
SELECT
    structure_id,
    material_code
FROM
    featurecopy.tidal_material_mapping;

INSERT INTO stream_crossings.tidal_physical_barrier_mapping (
    structure_id,
    physical_barrier_code
)
SELECT
    structure_id,
    physical_barrier_code
FROM
    featurecopy.tidal_physical_barrier_mapping;

---------------------------------------------------------
-- set up views for structures
---------------------------------------------------------
CREATE VIEW stream_crossings.nontidal_structures_view AS (
SELECT 
    s.structure_id,
    s.site_id,
    s.data_source_id,
    s.cabd_assessment_id,
    s.original_assessment_id,
    s.primary_structure,
    s.structure_number,
    s.outlet_shape_code,
    s.outlet_armouring_code,
    s.outlet_grade_code,
    s.outlet_width_m,
    s.outlet_height_m,
    s.structure_length_m,
    s.outlet_substrate_water_width_m,
    s.outlet_water_depth_m,
    s.outlet_drop_to_water_surface_m,
    s.outlet_drop_to_stream_bottom_m,
    s.outlet_drop_residual_pool_m,
    s.abutment_height_m,
    s.inlet_shape_code,
    s.inlet_type_code,
    s.inlet_grade_code,
    s.inlet_width_m,
    s.inlet_height_m,
    s.inlet_substrate_water_width_m,
    s.inlet_water_depth_m,
    s.structure_slope_pct,
    s.structure_slope_confidence_code,
    s.internal_structures_code,
    s.substrate_matches_stream_code,
    s.substrate_type_code,
    s.substrate_coverage_code,
    s.physical_barrier_severity_code,
    s.water_depth_matches_stream_code,
    s.water_velocity_matches_stream_code,
    s.dry_passage,
    s.height_above_dry_passage_m,
    m.materials AS structure_material_code,
    p.physical_barriers AS physical_barriers_code,
    s.structure_comments,
    s.passability_status_code
FROM stream_crossings.nontidal_structures s
LEFT JOIN (SELECT a.structure_id,
            array_agg(b.name_en) AS materials
           FROM stream_crossings.nontidal_material_mapping a
             JOIN stream_crossings.material_codes b ON a.material_code = b.code
          GROUP BY a.structure_id) m ON m.structure_id = s.structure_id
LEFT JOIN (SELECT p.structure_id,
            array_agg(b.name_en) AS physical_barriers
           FROM stream_crossings.nontidal_physical_barrier_mapping p
             JOIN stream_crossings.physical_barrier_codes b ON p.physical_barrier_code = b.code
          GROUP BY p.structure_id) p ON p.structure_id = s.structure_id
);

CREATE VIEW stream_crossings.tidal_structures_view AS (
SELECT 
    s.structure_id,
    s.site_id,
    s.data_source_id,
    s.cabd_assessment_id,
    s.original_assessment_id,
    s.primary_structure,
    s.structure_number,
    s.tidal_tide_gate_type_code,
    s.tidal_tide_gate_barrier_severity_code,
    s.outlet_shape_code,
    s.outlet_width_m,
    s.outlet_height_m,
    s.outlet_substrate_water_width_m,
    s.outlet_water_depth_m,
    s.outlet_abutment_height_m,
    s.outlet_high_tide_water_depth_m,
    s.outlet_spring_tide_water_depth_m,
    s.outlet_perch_low_tide_m,
    s.outlet_perch_high_tide_m,
    s.outlet_drop_height_residual_pool_m,
    s.outlet_armouring_code,
    s.outlet_grade_code,
    s.inlet_type_code,
    s.inlet_shape_code,
    s.inlet_width_m,
    s.inlet_height_m,
    s.inlet_substrate_water_width_m,
    s.inlet_water_depth_m,
    s.inlet_abutment_height_m,
    s.inlet_high_tide_water_depth_m,
    s.inlet_spring_tide_water_depth_m,
    s.inlet_perch_low_tide_m,
    s.inlet_perch_high_tide_m,
    s.inlet_armouring_code,
    s.inlet_grade_code,
    s.structure_length_m,
    s.tidal_relative_water_depth_code,
    s.substrate_type_code,
    s.substrate_matches_stream_code,
    s.substrate_coverage_code,
    s.tidal_relative_slope_code,
    s.physical_barrier_severity_code,
    s.dry_passage,
    s.height_above_dry_passage_m,
    m.materials AS structure_material_code,
    p.physical_barriers AS physical_barriers_code,
    s.structure_comments,
    s.passability_status_code
FROM stream_crossings.tidal_structures s
LEFT JOIN (SELECT a.structure_id,
            array_agg(b.name_en) AS materials
           FROM stream_crossings.tidal_material_mapping a
             JOIN stream_crossings.material_codes b ON a.material_code = b.code
          GROUP BY a.structure_id) m ON m.structure_id = s.structure_id
LEFT JOIN (SELECT p.structure_id,
            array_agg(b.name_en) AS physical_barriers
           FROM stream_crossings.tidal_physical_barrier_mapping p
             JOIN stream_crossings.physical_barrier_codes b ON p.physical_barrier_code = b.code
          GROUP BY p.structure_id) p ON p.structure_id = s.structure_id
);