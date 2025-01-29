##################

# Expected usage: py map_fishway_updates.py fishways
# Please ensure you have run insert_data_sources.py for the data sources you are mapping from
# Otherwise any updates missing a data source id will not be made

# This script will process all updates with an update_status of 'ready'

##################

import user_submit as main

script = main.MappingScript("fishway_updates")

query = f"""
-- add data source ids to the table
ALTER TABLE {script.fishUpdateTable} ADD COLUMN IF NOT EXISTS data_source uuid;
UPDATE {script.fishUpdateTable} AS s SET data_source = d.id FROM cabd.data_source AS d
    WHERE d.name = s.data_source_short_name
    AND s.update_status = 'ready';

ALTER TABLE {script.fishUpdateTable} ALTER COLUMN submitted_on TYPE timestamptz USING submitted_on::timestamptz;
"""

initializequery = f"""
--where multiple updates exist for a feature, only update one at a time
WITH cte AS (
  SELECT id, cabd_id,
    row_number() OVER(PARTITION BY cabd_id ORDER BY submitted_on ASC) AS rn
  FROM {script.fishUpdateTable} WHERE update_status = 'ready'
)
UPDATE {script.fishUpdateTable}
SET update_status = 'wait'
    WHERE id IN (SELECT id FROM cte WHERE rn > 1);
"""

mappingquery = f"""
UPDATE {script.fishUpdateTable} 
    SET fishpass_type_code = (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown')
    WHERE fishpass_type_code IS NULL
    AND entry_classification = 'new feature';

-- deal with new and modified records
WITH new_points AS (
    SELECT (ST_SetSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4617)) AS geom, cabd_id AS id, province_territory_code AS loc, fishpass_type_code AS fishpass_type_code
    FROM {script.fishUpdateTable}
    WHERE entry_classification = 'new feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND update_status = 'ready'
    )
INSERT INTO {script.fishTable} (original_point, cabd_id, province_territory_code, fishpass_type_code)
    SELECT geom, id, loc, fishpass_type_code FROM new_points;

INSERT INTO {script.fishAttributeTable} (cabd_id)
    SELECT cabd_id FROM {script.fishUpdateTable}
    WHERE entry_classification = 'new feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND update_status = 'ready';

WITH move_points AS (
    SELECT (ST_SetSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4617)) AS geom, cabd_id AS id
    FROM {script.fishUpdateTable}
    WHERE entry_classification = 'modify feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND update_status = 'ready'
    )
UPDATE {script.fishTable} AS foo 
    SET original_point = bar.geom
    FROM move_points AS bar
    WHERE cabd_id = bar.id;

-- add records to feature_source table
ALTER TABLE {script.fishFeatureTable} ALTER COLUMN datasource_feature_id DROP NOT NULL;

INSERT INTO {script.fishFeatureTable} (cabd_id, datasource_id)
SELECT cabd_id, data_source FROM {script.fishUpdateTable} WHERE update_status = 'ready'
ON CONFLICT DO NOTHING;

-- update attribute_source table for live data
UPDATE
    {script.fishAttributeTable} AS cabdsource
SET  
    architect_ds = CASE WHEN ({script.datasetName}.architect IS NOT NULL AND {script.datasetName}.architect IS DISTINCT FROM cabd.architect) THEN {script.datasetName}.data_source ELSE cabdsource.architect_ds END,
    constructed_by_ds = CASE WHEN ({script.datasetName}.constructed_by IS NOT NULL AND {script.datasetName}.constructed_by IS DISTINCT FROM cabd.constructed_by) THEN {script.datasetName}.data_source ELSE cabdsource.constructed_by_ds END,
    contracted_by_ds = CASE WHEN ({script.datasetName}.contracted_by IS NOT NULL AND {script.datasetName}.contracted_by IS DISTINCT FROM cabd.contracted_by) THEN {script.datasetName}.data_source ELSE cabdsource.contracted_by_ds END,
    depth_m_ds = CASE WHEN ({script.datasetName}.depth_m IS NOT NULL AND {script.datasetName}.depth_m IS DISTINCT FROM cabd.depth_m) THEN {script.datasetName}.data_source ELSE cabdsource.depth_m_ds END,
    designed_on_biology_ds = CASE WHEN ({script.datasetName}.designed_on_biology IS NOT NULL AND {script.datasetName}.designed_on_biology IS DISTINCT FROM cabd.designed_on_biology) THEN {script.datasetName}.data_source ELSE cabdsource.designed_on_biology_ds END,
    elevation_m_ds = CASE WHEN ({script.datasetName}.elevation_m IS NOT NULL AND {script.datasetName}.elevation_m IS DISTINCT FROM cabd.elevation_m) THEN {script.datasetName}.data_source ELSE cabdsource.elevation_m_ds END,
    engineering_notes_ds = CASE WHEN ({script.datasetName}.engineering_notes IS NOT NULL AND {script.datasetName}.engineering_notes IS DISTINCT FROM cabd.engineering_notes) THEN {script.datasetName}.data_source ELSE cabdsource.engineering_notes_ds END,
    entrance_location_code_ds = CASE WHEN ({script.datasetName}.entrance_location_code IS NOT NULL AND {script.datasetName}.entrance_location_code IS DISTINCT FROM cabd.entrance_location_code) THEN {script.datasetName}.data_source ELSE cabdsource.entrance_location_code_ds END,
    entrance_position_code_ds = CASE WHEN ({script.datasetName}.entrance_position_code IS NOT NULL AND {script.datasetName}.entrance_position_code IS DISTINCT FROM cabd.entrance_position_code) THEN {script.datasetName}.data_source ELSE cabdsource.entrance_position_code_ds END,
    estimate_of_attraction_pct_ds = CASE WHEN ({script.datasetName}.estimate_of_attraction_pct IS NOT NULL AND {script.datasetName}.estimate_of_attraction_pct IS DISTINCT FROM cabd.estimate_of_attraction_pct) THEN {script.datasetName}.data_source ELSE cabdsource.estimate_of_attraction_pct_ds END,
    estimate_of_passage_success_pct_ds = CASE WHEN ({script.datasetName}.estimate_of_passage_success_pct IS NOT NULL AND {script.datasetName}.estimate_of_passage_success_pct IS DISTINCT FROM cabd.estimate_of_passage_success_pct) THEN {script.datasetName}.data_source ELSE cabdsource.estimate_of_passage_success_pct_ds END,
    fishpass_type_code_ds = CASE WHEN ({script.datasetName}.fishpass_type_code IS NOT NULL AND {script.datasetName}.fishpass_type_code IS DISTINCT FROM cabd.fishpass_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.fishpass_type_code_ds END,
    fishway_reference_id_ds = CASE WHEN ({script.datasetName}.fishway_reference_id IS NOT NULL AND {script.datasetName}.fishway_reference_id IS DISTINCT FROM cabd.fishway_reference_id) THEN {script.datasetName}.data_source ELSE cabdsource.fishway_reference_id_ds END,
    gradient_ds = CASE WHEN ({script.datasetName}.gradient IS NOT NULL AND {script.datasetName}.gradient IS DISTINCT FROM cabd.gradient) THEN {script.datasetName}.data_source ELSE cabdsource.gradient_ds END,
    has_evaluating_studies_ds = CASE WHEN ({script.datasetName}.has_evaluating_studies IS NOT NULL AND {script.datasetName}.has_evaluating_studies IS DISTINCT FROM cabd.has_evaluating_studies) THEN {script.datasetName}.data_source ELSE cabdsource.has_evaluating_studies_ds END,
    length_m_ds = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.data_source ELSE cabdsource.length_m_ds END,
    max_fishway_velocity_ms_ds = CASE WHEN ({script.datasetName}.max_fishway_velocity_ms IS NOT NULL AND {script.datasetName}.max_fishway_velocity_ms IS DISTINCT FROM cabd.max_fishway_velocity_ms) THEN {script.datasetName}.data_source ELSE cabdsource.max_fishway_velocity_ms_ds END,
    mean_fishway_velocity_ms_ds = CASE WHEN ({script.datasetName}.mean_fishway_velocity_ms IS NOT NULL AND {script.datasetName}.mean_fishway_velocity_ms IS DISTINCT FROM cabd.mean_fishway_velocity_ms) THEN {script.datasetName}.data_source ELSE cabdsource.mean_fishway_velocity_ms_ds END,
    modification_purpose_ds = CASE WHEN ({script.datasetName}.modification_purpose IS NOT NULL AND {script.datasetName}.modification_purpose IS DISTINCT FROM cabd.modification_purpose) THEN {script.datasetName}.data_source ELSE cabdsource.modification_purpose_ds END,
    modification_year_ds = CASE WHEN ({script.datasetName}.modification_year IS NOT NULL AND {script.datasetName}.modification_year IS DISTINCT FROM cabd.modification_year) THEN {script.datasetName}.data_source ELSE cabdsource.modification_year_ds END,
    modified_ds = CASE WHEN ({script.datasetName}.modified IS NOT NULL AND {script.datasetName}.modified IS DISTINCT FROM cabd.modified) THEN {script.datasetName}.data_source ELSE cabdsource.modified_ds END,
    monitoring_equipment_ds = CASE WHEN ({script.datasetName}.monitoring_equipment IS NOT NULL AND {script.datasetName}.monitoring_equipment IS DISTINCT FROM cabd.monitoring_equipment) THEN {script.datasetName}.data_source ELSE cabdsource.monitoring_equipment_ds END,
    nature_of_evaluation_studies_ds = CASE WHEN ({script.datasetName}.nature_of_evaluation_studies IS NOT NULL AND {script.datasetName}.nature_of_evaluation_studies IS DISTINCT FROM cabd.nature_of_evaluation_studies) THEN {script.datasetName}.data_source ELSE cabdsource.nature_of_evaluation_studies_ds END,
    operated_by_ds = CASE WHEN ({script.datasetName}.operated_by IS NOT NULL AND {script.datasetName}.operated_by IS DISTINCT FROM cabd.operated_by) THEN {script.datasetName}.data_source ELSE cabdsource.operated_by_ds END,
    operating_notes_ds = CASE WHEN ({script.datasetName}.operating_notes IS NOT NULL AND {script.datasetName}.operating_notes IS DISTINCT FROM cabd.operating_notes) THEN {script.datasetName}.data_source ELSE cabdsource.operating_notes_ds END,
    operation_period_ds = CASE WHEN ({script.datasetName}.operation_period IS NOT NULL AND {script.datasetName}.operation_period IS DISTINCT FROM cabd.operation_period) THEN {script.datasetName}.data_source ELSE cabdsource.operation_period_ds END,
    plans_held_by_ds = CASE WHEN ({script.datasetName}.plans_held_by IS NOT NULL AND {script.datasetName}.plans_held_by IS DISTINCT FROM cabd.plans_held_by) THEN {script.datasetName}.data_source ELSE cabdsource.plans_held_by_ds END,
    purpose_ds = CASE WHEN ({script.datasetName}.purpose IS NOT NULL AND {script.datasetName}.purpose IS DISTINCT FROM cabd.purpose) THEN {script.datasetName}.data_source ELSE cabdsource.purpose_ds END,
    river_name_en_ds = CASE WHEN ({script.datasetName}.river_name_en IS NOT NULL AND {script.datasetName}.river_name_en IS DISTINCT FROM cabd.river_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.river_name_en_ds END,
    river_name_fr_ds = CASE WHEN ({script.datasetName}.river_name_fr IS NOT NULL AND {script.datasetName}.river_name_fr IS DISTINCT FROM cabd.river_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.river_name_fr_ds END,
    structure_name_en_ds = CASE WHEN ({script.datasetName}.structure_name_en IS NOT NULL AND {script.datasetName}.structure_name_en IS DISTINCT FROM cabd.structure_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.structure_name_en_ds END,
    structure_name_fr_ds = CASE WHEN ({script.datasetName}.structure_name_fr IS NOT NULL AND {script.datasetName}.structure_name_fr IS DISTINCT FROM cabd.structure_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.structure_name_fr_ds END,
    waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    waterbody_name_fr_ds = CASE WHEN ({script.datasetName}.waterbody_name_fr IS NOT NULL AND {script.datasetName}.waterbody_name_fr IS DISTINCT FROM cabd.waterbody_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_fr_ds END,
    year_constructed_ds = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN {script.datasetName}.data_source ELSE cabdsource.year_constructed_ds END

FROM
    {script.fishTable} AS cabd,
    {script.fishUpdateTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id and cabd.cabd_id = cabdsource.cabd_id
    AND {script.datasetName}.entry_classification IN ('new feature', 'modify feature')
    AND {script.datasetName}.update_status = 'ready'
    AND {script.datasetName}.data_source IS NOT NULL;

--update attributes in live data
UPDATE
    {script.fishTable} AS cabd
SET
    architect = CASE WHEN ({script.datasetName}.architect IS NOT NULL AND {script.datasetName}.architect IS DISTINCT FROM cabd.architect) THEN {script.datasetName}.architect ELSE cabd.architect END,
    constructed_by = CASE WHEN ({script.datasetName}.constructed_by IS NOT NULL AND {script.datasetName}.constructed_by IS DISTINCT FROM cabd.constructed_by) THEN {script.datasetName}.constructed_by ELSE cabd.constructed_by END,
    contracted_by = CASE WHEN ({script.datasetName}.contracted_by IS NOT NULL AND {script.datasetName}.contracted_by IS DISTINCT FROM cabd.contracted_by) THEN {script.datasetName}.contracted_by ELSE cabd.contracted_by END,
    dam_id = CASE WHEN ({script.datasetName}.dam_id IS NOT NULL AND {script.datasetName}.dam_id IS DISTINCT FROM cabd.dam_id) THEN {script.datasetName}.dam_id ELSE cabd.dam_id END,
    depth_m = CASE WHEN ({script.datasetName}.depth_m IS NOT NULL AND {script.datasetName}.depth_m IS DISTINCT FROM cabd.depth_m) THEN {script.datasetName}.depth_m ELSE cabd.depth_m END,
    designed_on_biology = CASE WHEN ({script.datasetName}.designed_on_biology IS NOT NULL AND {script.datasetName}.designed_on_biology IS DISTINCT FROM cabd.designed_on_biology) THEN {script.datasetName}.designed_on_biology ELSE cabd.designed_on_biology END,
    elevation_m = CASE WHEN ({script.datasetName}.elevation_m IS NOT NULL AND {script.datasetName}.elevation_m IS DISTINCT FROM cabd.elevation_m) THEN {script.datasetName}.elevation_m ELSE cabd.elevation_m END,
    engineering_notes = CASE WHEN ({script.datasetName}.engineering_notes IS NOT NULL AND {script.datasetName}.engineering_notes IS DISTINCT FROM cabd.engineering_notes) THEN {script.datasetName}.engineering_notes ELSE cabd.engineering_notes END,
    entrance_location_code = CASE WHEN ({script.datasetName}.entrance_location_code IS NOT NULL AND {script.datasetName}.entrance_location_code IS DISTINCT FROM cabd.entrance_location_code) THEN {script.datasetName}.entrance_location_code ELSE cabd.entrance_location_code END,
    entrance_position_code = CASE WHEN ({script.datasetName}.entrance_position_code IS NOT NULL AND {script.datasetName}.entrance_position_code IS DISTINCT FROM cabd.entrance_position_code) THEN {script.datasetName}.entrance_position_code ELSE cabd.entrance_position_code END,
    estimate_of_attraction_pct = CASE WHEN ({script.datasetName}.estimate_of_attraction_pct IS NOT NULL AND {script.datasetName}.estimate_of_attraction_pct IS DISTINCT FROM cabd.estimate_of_attraction_pct) THEN {script.datasetName}.estimate_of_attraction_pct ELSE cabd.estimate_of_attraction_pct END,
    estimate_of_passage_success_pct = CASE WHEN ({script.datasetName}.estimate_of_passage_success_pct IS NOT NULL AND {script.datasetName}.estimate_of_passage_success_pct IS DISTINCT FROM cabd.estimate_of_passage_success_pct) THEN {script.datasetName}.estimate_of_passage_success_pct ELSE cabd.estimate_of_passage_success_pct END,
    fishpass_type_code = CASE WHEN ({script.datasetName}.fishpass_type_code IS NOT NULL AND {script.datasetName}.fishpass_type_code IS DISTINCT FROM cabd.fishpass_type_code) THEN {script.datasetName}.fishpass_type_code ELSE cabd.fishpass_type_code END,
    fishway_reference_id = CASE WHEN ({script.datasetName}.fishway_reference_id IS NOT NULL AND {script.datasetName}.fishway_reference_id IS DISTINCT FROM cabd.fishway_reference_id) THEN {script.datasetName}.fishway_reference_id ELSE cabd.fishway_reference_id END,
    gradient = CASE WHEN ({script.datasetName}.gradient IS NOT NULL AND {script.datasetName}.gradient IS DISTINCT FROM cabd.gradient) THEN {script.datasetName}.gradient ELSE cabd.gradient END,
    has_evaluating_studies = CASE WHEN ({script.datasetName}.has_evaluating_studies IS NOT NULL AND {script.datasetName}.has_evaluating_studies IS DISTINCT FROM cabd.has_evaluating_studies) THEN {script.datasetName}.has_evaluating_studies ELSE cabd.has_evaluating_studies END,
    length_m = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.length_m ELSE cabd.length_m END,
    max_fishway_velocity_ms = CASE WHEN ({script.datasetName}.max_fishway_velocity_ms IS NOT NULL AND {script.datasetName}.max_fishway_velocity_ms IS DISTINCT FROM cabd.max_fishway_velocity_ms) THEN {script.datasetName}.max_fishway_velocity_ms ELSE cabd.max_fishway_velocity_ms END,
    mean_fishway_velocity_ms = CASE WHEN ({script.datasetName}.mean_fishway_velocity_ms IS NOT NULL AND {script.datasetName}.mean_fishway_velocity_ms IS DISTINCT FROM cabd.mean_fishway_velocity_ms) THEN {script.datasetName}.mean_fishway_velocity_ms ELSE cabd.mean_fishway_velocity_ms END,
    modification_purpose = CASE WHEN ({script.datasetName}.modification_purpose IS NOT NULL AND {script.datasetName}.modification_purpose IS DISTINCT FROM cabd.modification_purpose) THEN {script.datasetName}.modification_purpose ELSE cabd.modification_purpose END,
    modification_year = CASE WHEN ({script.datasetName}.modification_year IS NOT NULL AND {script.datasetName}.modification_year IS DISTINCT FROM cabd.modification_year) THEN {script.datasetName}.modification_year ELSE cabd.modification_year END,
    modified = CASE WHEN ({script.datasetName}.modified IS NOT NULL AND {script.datasetName}.modified IS DISTINCT FROM cabd.modified) THEN {script.datasetName}.modified ELSE cabd.modified END,
    monitoring_equipment = CASE WHEN ({script.datasetName}.monitoring_equipment IS NOT NULL AND {script.datasetName}.monitoring_equipment IS DISTINCT FROM cabd.monitoring_equipment) THEN {script.datasetName}.monitoring_equipment ELSE cabd.monitoring_equipment END,
    nature_of_evaluation_studies = CASE WHEN ({script.datasetName}.nature_of_evaluation_studies IS NOT NULL AND {script.datasetName}.nature_of_evaluation_studies IS DISTINCT FROM cabd.nature_of_evaluation_studies) THEN {script.datasetName}.nature_of_evaluation_studies ELSE cabd.nature_of_evaluation_studies END,
    operated_by = CASE WHEN ({script.datasetName}.operated_by IS NOT NULL AND {script.datasetName}.operated_by IS DISTINCT FROM cabd.operated_by) THEN {script.datasetName}.operated_by ELSE cabd.operated_by END,
    operating_notes = CASE WHEN ({script.datasetName}.operating_notes IS NOT NULL AND {script.datasetName}.operating_notes IS DISTINCT FROM cabd.operating_notes) THEN {script.datasetName}.operating_notes ELSE cabd.operating_notes END,
    operation_period = CASE WHEN ({script.datasetName}.operation_period IS NOT NULL AND {script.datasetName}.operation_period IS DISTINCT FROM cabd.operation_period) THEN {script.datasetName}.operation_period ELSE cabd.operation_period END,
    plans_held_by = CASE WHEN ({script.datasetName}.plans_held_by IS NOT NULL AND {script.datasetName}.plans_held_by IS DISTINCT FROM cabd.plans_held_by) THEN {script.datasetName}.plans_held_by ELSE cabd.plans_held_by END,
    purpose = CASE WHEN ({script.datasetName}.purpose IS NOT NULL AND {script.datasetName}.purpose IS DISTINCT FROM cabd.purpose) THEN {script.datasetName}.purpose ELSE cabd.purpose END,
    river_name_en = CASE WHEN ({script.datasetName}.river_name_en IS NOT NULL AND {script.datasetName}.river_name_en IS DISTINCT FROM cabd.river_name_en) THEN {script.datasetName}.river_name_en ELSE cabd.river_name_en END,
    river_name_fr = CASE WHEN ({script.datasetName}.river_name_fr IS NOT NULL AND {script.datasetName}.river_name_fr IS DISTINCT FROM cabd.river_name_fr) THEN {script.datasetName}.river_name_fr ELSE cabd.river_name_fr END,
    structure_name_en = CASE WHEN ({script.datasetName}.structure_name_en IS NOT NULL AND {script.datasetName}.structure_name_en IS DISTINCT FROM cabd.structure_name_en) THEN {script.datasetName}.structure_name_en ELSE cabd.structure_name_en END,
    structure_name_fr = CASE WHEN ({script.datasetName}.structure_name_fr IS NOT NULL AND {script.datasetName}.structure_name_fr IS DISTINCT FROM cabd.structure_name_fr) THEN {script.datasetName}.structure_name_fr ELSE cabd.structure_name_fr END,
    waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    waterbody_name_fr = CASE WHEN ({script.datasetName}.waterbody_name_fr IS NOT NULL AND {script.datasetName}.waterbody_name_fr IS DISTINCT FROM cabd.waterbody_name_fr) THEN {script.datasetName}.waterbody_name_fr ELSE cabd.waterbody_name_fr END,
    year_constructed = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN {script.datasetName}.year_constructed ELSE cabd.year_constructed END

FROM
    {script.fishUpdateTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id
    AND {script.datasetName}.entry_classification IN ('new feature', 'modify feature')
    AND {script.datasetName}.update_status = 'ready'
    AND {script.datasetName}.data_source IS NOT NULL;

-- create records in species mapping table for fishways indicating species that do/do not use it
INSERT INTO fishways.species_mapping (fishway_id, species_id, known_to_use)
SELECT a.cabd_id, b.id, true
FROM {script.fishUpdateTable} a, cabd.fish_species b
WHERE a.known_use ILIKE '%' || b.common_name || '%' AND b.id IS NOT NULL
AND a.update_status = 'ready'
ON CONFLICT DO NOTHING;

INSERT INTO fishways.species_mapping (fishway_id, species_id, known_to_use)
SELECT a.cabd_id, b.id, false
FROM {script.fishUpdateTable} a, cabd.fish_species b
WHERE a.known_notuse ILIKE '%' || b.common_name || '%' AND b.id IS NOT NULL
AND a.update_status = 'ready'
ON CONFLICT DO NOTHING;

-- deal with records to be deleted
DELETE FROM fishways.species_mapping WHERE fishway_id IN (SELECT cabd_id FROM {script.fishUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');
DELETE FROM {script.fishAttributeTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.fishUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');
DELETE FROM {script.fishFeatureTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.fishUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');
DELETE FROM {script.fishTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.fishUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');

-- set records to 'done' and delete from update table
UPDATE {script.fishUpdateTable} SET update_status = 'done' WHERE update_status = 'ready';
UPDATE {script.fishUpdateTable} SET update_status = 'ready' WHERE update_status = 'wait';
-- DELETE FROM {script.fishUpdateTable} WHERE update_status = 'done';

"""

script.do_work(query, initializequery, mappingquery)
