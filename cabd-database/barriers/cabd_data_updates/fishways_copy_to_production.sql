-- https://www.postgresql.org/docs/13/sql-begin.html
-- if you are using a GUI you need to configure the script editor to 
-- not run in autocommit mode 

BEGIN TRANSACTION;
--push to production table
INSERT INTO fishways.fishways(
    cabd_id, dam_id, structure_name_en, structure_name_fr, waterbody_name_en, waterbody_name_fr, river_name_en, river_name_fr,
    nhn_workunit_id, province_territory_code, municipality, fishpass_type_code, monitoring_equipment, architect, 
    contracted_by, constructed_by, plans_held_by, purpose, designed_on_biology, length_m, elevation_m, gradient, 
    depth_m, entrance_location_code, entrance_position_code, modified, modification_year, modification_purpose, 
    year_constructed, operated_by, operation_period, has_evaluating_studies, nature_of_evaluation_studies, engineering_notes,
    operating_notes, mean_fishway_velocity_ms, max_fishway_velocity_ms, estimate_of_attraction_pct, 
    estimate_of_passage_success_pct, fishway_reference_id, complete_level_code, original_point
)
SELECT
    cabd_id, dam_id, structure_name_en, structure_name_fr, waterbody_name_en, waterbody_name_fr, river_name_en, river_name_fr,
    nhn_workunit_id, province_territory_code, municipality, fishpass_type_code, monitoring_equipment, architect, 
    contracted_by, constructed_by, plans_held_by, purpose, designed_on_biology, length_m, elevation_m, gradient, 
    depth_m, entrance_location_code, entrance_position_code, modified, modification_year, modification_purpose, 
    year_constructed, operated_by, operation_period, has_evaluating_studies, nature_of_evaluation_studies, engineering_notes,
    operating_notes, mean_fishway_velocity_ms, max_fishway_velocity_ms, estimate_of_attraction_pct, 
    estimate_of_passage_success_pct, fishway_reference_id, complete_level_code, original_point
FROM featurecopy.fishways;


INSERT INTO fishways.fishways_attribute_source(
    cabd_id, structure_name_en_ds, structure_name_en_dsfid, structure_name_fr_ds, structure_name_fr_dsfid, waterbody_name_en_ds,
    waterbody_name_en_dsfid, waterbody_name_fr_ds, waterbody_name_fr_dsfid, river_name_en_ds, river_name_en_dsfid,
    river_name_fr_ds, river_name_fr_dsfid, fishpass_type_code_ds, fishpass_type_code_dsfid, monitoring_equipment_ds,
    monitoring_equipment_dsfid, architect_ds, architect_dsfid, contracted_by_ds, contracted_by_dsfid,
    constructed_by_ds, constructed_by_dsfid, plans_held_by_ds, plans_held_by_dsfid, purpose_ds, purpose_dsfid,
    designed_on_biology_ds, designed_on_biology_dsfid, length_m_ds, length_m_dsfid, elevation_m_ds, elevation_m_dsfid,
    gradient_ds, gradient_dsfid, depth_m_ds, depth_m_dsfid, entrance_location_code_ds, entrance_location_code_dsfid,
    entrance_position_code_ds, entrance_position_code_dsfid, modified_ds, modified_dsfid, modification_year_ds,
    modification_year_dsfid, modification_purpose_ds, modification_purpose_dsfid, year_constructed_ds,
    year_constructed_dsfid, operated_by_ds, operated_by_dsfid, operation_period_ds, operation_period_dsfid,
    has_evaluating_studies_ds, has_evaluating_studies_dsfid, nature_of_evaluation_studies_ds,
    nature_of_evaluation_studies_dsfid, engineering_notes_ds, engineering_notes_dsfid, operating_notes_ds,
    operating_notes_dsfid, mean_fishway_velocity_ms_ds, mean_fishway_velocity_ms_dsfid, max_fishway_velocity_ms_ds,
    max_fishway_velocity_ms_dsfid, estimate_of_attraction_pct_ds, estimate_of_attraction_pct_dsfid,
    estimate_of_passage_success_pct_ds, estimate_of_passage_success_pct_dsfid, fishway_reference_id_ds,
    fishway_reference_id_dsfid, complete_level_code_ds, complete_level_code_dsfid, original_point_ds, original_point_dsfid
)
SELECT
    cabd_id, structure_name_en_ds, structure_name_en_dsfid, structure_name_fr_ds, structure_name_fr_dsfid, waterbody_name_en_ds,
    waterbody_name_en_dsfid, waterbody_name_fr_ds, waterbody_name_fr_dsfid, river_name_en_ds, river_name_en_dsfid,
    river_name_fr_ds, river_name_fr_dsfid, fishpass_type_code_ds, fishpass_type_code_dsfid, monitoring_equipment_ds,
    monitoring_equipment_dsfid, architect_ds, architect_dsfid, contracted_by_ds, contracted_by_dsfid,
    constructed_by_ds, constructed_by_dsfid, plans_held_by_ds, plans_held_by_dsfid, purpose_ds, purpose_dsfid,
    designed_on_biology_ds, designed_on_biology_dsfid, length_m_ds, length_m_dsfid, elevation_m_ds, elevation_m_dsfid,
    gradient_ds, gradient_dsfid, depth_m_ds, depth_m_dsfid, entrance_location_code_ds, entrance_location_code_dsfid,
    entrance_position_code_ds, entrance_position_code_dsfid, modified_ds, modified_dsfid, modification_year_ds,
    modification_year_dsfid, modification_purpose_ds, modification_purpose_dsfid, year_constructed_ds,
    year_constructed_dsfid, operated_by_ds, operated_by_dsfid, operation_period_ds, operation_period_dsfid,
    has_evaluating_studies_ds, has_evaluating_studies_dsfid, nature_of_evaluation_studies_ds,
    nature_of_evaluation_studies_dsfid, engineering_notes_ds, engineering_notes_dsfid, operating_notes_ds,
    operating_notes_dsfid, mean_fishway_velocity_ms_ds, mean_fishway_velocity_ms_dsfid, max_fishway_velocity_ms_ds,
    max_fishway_velocity_ms_dsfid, estimate_of_attraction_pct_ds, estimate_of_attraction_pct_dsfid,
    estimate_of_passage_success_pct_ds, estimate_of_passage_success_pct_dsfid, fishway_reference_id_ds,
    fishway_reference_id_dsfid, complete_level_code_ds, complete_level_code_dsfid, original_point_ds, original_point_dsfid
FROM featurecopy.fishways_attribute_source;

--create records in species mapping table for fishways indicating species that do/do not use it
INSERT INTO fishways.species_mapping (fishway_id, species_id, known_to_use)
SELECT DISTINCT a.cabd_id, b.fishid, true
FROM featurecopy.fishways a, featurecopy.fishmapping b
WHERE a.species_known_to_use_fishway LIKE '%' || b.name || '%' AND b.fishid IS NOT NULL;
INSERT INTO fishways.species_mapping (fishway_id, species_id, known_to_use)
SELECT cabd_id, fishid, false FROM
(SELECT DISTINCT a.cabd_id, b.fishid
FROM featurecopy.fishways a, featurecopy.fishmapping b
WHERE a.species_known_not_to_use_fishway LIKE '%' || b.name || '%' AND b.fishid IS NOT NULL
EXCEPT
SELECT fishway_id, species_id FROM fishways.species_mapping sm ) foo;

--do whatever qa checks you want to do here?

SELECT COUNT(*) FROM fishways.fishways;
SELECT COUNT(*) FROM fishways.fishways_attribute_source;

--COMMIT;