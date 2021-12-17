-- https://www.postgresql.org/docs/13/sql-begin.html
-- if you are using a GUI you need to configure the script editor to 
-- not run in autocommit mode 

BEGIN TRANSACTION;
--push to production table
INSERT INTO fishways.fishways(
    cabd_id, dam_id, dam_name_en, dam_name_fr, waterbody_name_en, waterbody_name_fr, river_name_en, river_name_fr,
    nhn_workunit_id, province_territory_code, municipality, fishpass_type_code, monitoring_equipment, architect, 
    contracted_by, constructed_by, plans_held_by, purpose, designed_on_biology, length_m, elevation_m, gradient, 
    depth_m, entrance_location_code, entrance_position_code, modified, modification_year, modification_purpose, 
    year_constructed, operated_by, operation_period, has_evaluating_studies, nature_of_evaluation_studies, engineering_notes,
    operating_notes, mean_fishway_velocity_ms, max_fishway_velocity_ms, estimate_of_attraction_pct, 
    estimate_of_passage_success_pct, fishway_reference_id, complete_level_code, original_point
)
SELECT
    cabd_id, dam_id, dam_name_en, dam_name_fr, waterbody_name_en, waterbody_name_fr, river_name_en, river_name_fr,
    nhn_workunit_id, province_territory_code, municipality, fishpass_type_code, monitoring_equipment, architect, 
    contracted_by, constructed_by, plans_held_by, purpose, designed_on_biology, length_m, elevation_m, gradient, 
    depth_m, entrance_location_code, entrance_position_code, modified, modification_year, modification_purpose, 
    year_constructed, operated_by, operation_period, has_evaluating_studies, nature_of_evaluation_studies, engineering_notes,
    operating_notes, mean_fishway_velocity_ms, max_fishway_velocity_ms, estimate_of_attraction_pct, 
    estimate_of_passage_success_pct, fishway_reference_id, complete_level_code, original_point
FROM featurecopy.fishways;

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

--COMMIT;