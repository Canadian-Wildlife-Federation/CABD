---------------------------------------------------
-- nontidal
---------------------------------------------------

UPDATE featurecopy.nontidal_structures_kwrc_bridges bar SET site_id = foo.cabd_id FROM featurecopy.nontidal_sites foo WHERE foo.cabd_assessment_id = bar.cabd_assessment_id;
UPDATE featurecopy.nontidal_structures_kwrc_intermitted_dry_drains bar SET site_id = foo.cabd_id FROM featurecopy.nontidal_sites foo WHERE foo.cabd_assessment_id = bar.cabd_assessment_id;
UPDATE featurecopy.nontidal_structures_kwrc_current_aug_6 bar SET site_id = foo.cabd_id FROM featurecopy.nontidal_sites foo WHERE foo.cabd_assessment_id = bar.cabd_assessment_id;
UPDATE featurecopy.nontidal_structures_kwrc_master_2 bar SET site_id = foo.cabd_id FROM featurecopy.nontidal_sites foo WHERE foo.cabd_assessment_id = bar.cabd_assessment_id;
UPDATE featurecopy.nontidal_structures_peskotomuhkati_nation_01192023 bar SET site_id = foo.cabd_id FROM featurecopy.nontidal_sites foo WHERE foo.cabd_assessment_id = bar.cabd_assessment_id;

TRUNCATE TABLE featurecopy.nontidal_structures;

INSERT INTO featurecopy.nontidal_structures (
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id
)
SELECT
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id
FROM featurecopy.nontidal_structures_kwrc_bridges;


INSERT INTO featurecopy.nontidal_structures (
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_grade_code,
    inlet_width_m,
    inlet_water_depth_m,
    structure_slope_pct,
    structure_slope_confidence_code,
    substrate_type_code,
    water_velocity_matches_stream_code
)
SELECT
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_grade_code,
    inlet_width_m,
    inlet_water_depth_m,
    structure_slope_pct,
    structure_slope_confidence_code,
    substrate_type_code,
    water_velocity_matches_stream_code
FROM featurecopy.nontidal_structures_kwrc_current_aug_6;


INSERT INTO featurecopy.nontidal_structures (
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id
)
SELECT
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id
FROM featurecopy.nontidal_structures_kwrc_intermitted_dry_drains;


INSERT INTO featurecopy.nontidal_structures (
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_grade_code,
    inlet_width_m,
    inlet_water_depth_m,
    structure_slope_pct,
    structure_slope_confidence_code,
    substrate_type_code,
    water_velocity_matches_stream_code
)
SELECT
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_grade_code,
    inlet_width_m,
    inlet_water_depth_m,
    structure_slope_pct,
    structure_slope_confidence_code,
    substrate_type_code,
    water_velocity_matches_stream_code
FROM featurecopy.nontidal_structures_kwrc_master_2;


INSERT INTO featurecopy.nontidal_structures (
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    structure_number,
    primary_structure,
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
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
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
    structure_number,
    primary_structure,
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
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
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
FROM featurecopy.nontidal_structures_peskotomuhkati_nation_01192023;

---------------------------------------------------
-- tidal
---------------------------------------------------

UPDATE featurecopy.tidal_structures_peskotomuhkati_nation_01192023 bar SET site_id = foo.cabd_id FROM featurecopy.tidal_sites foo WHERE foo.cabd_assessment_id = bar.cabd_assessment_id;

TRUNCATE TABLE featurecopy.tidal_structures;

INSERT INTO featurecopy.tidal_structures (
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    passability_status_code,
    structure_comments
)
SELECT
    structure_id,
    site_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    passability_status_code,
    structure_comments
FROM featurecopy.tidal_structures_peskotomuhkati_nation_01192023;