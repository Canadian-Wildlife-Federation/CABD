##################

# Expected usage: py map_dam_updates.py <featureType>
# Please ensure you have run insert_data_sources.py for the data sources you are mapping from
# Otherwise any updates missing a data source id will not be made

# This script will process all updates with an update_status of 'ready'

##################

import user_submit as main

script = main.MappingScript("dam_updates")

query = f"""
-- add data source ids to the table
ALTER TABLE {script.damUpdateTable} ADD COLUMN IF NOT EXISTS data_source uuid;
UPDATE {script.damUpdateTable} AS s SET data_source = d.id FROM cabd.data_source AS d
    WHERE d.name = s.data_source_short_name
    AND s.update_status = 'ready';

ALTER TABLE cabd.dam_updates ALTER COLUMN submitted_on TYPE timestamptz USING submitted_on::timestamptz;
"""

initializequery = f"""
--where multiple updates exist for a feature, only update one at a time
WITH cte AS (
  SELECT id, cabd_id,
    row_number() OVER(PARTITION BY cabd_id ORDER BY submitted_on ASC) AS rn
  FROM {script.damUpdateTable} WHERE update_status = 'ready'
)
UPDATE {script.damUpdateTable}
SET update_status = 'wait'
    WHERE id IN (SELECT id FROM cte WHERE rn > 1);
"""

mappingquery = f"""
--------------------------------------------------------------------------
-- TO DO: add dsfid records to damAttributeTable for updates coming from BC water rights database
--------------------------------------------------------------------------

-- deal with new and modified records
WITH new_points AS (
    SELECT (ST_SetSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4617)) AS geom, cabd_id AS id, province_territory_code AS loc
    FROM {script.damUpdateTable}
    WHERE entry_classification = 'new feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND update_status = 'ready'
    )
INSERT INTO {script.damTable} (original_point, cabd_id, province_territory_code)
    SELECT geom, id, loc FROM new_points;

INSERT INTO {script.damAttributeTable} (cabd_id)
    SELECT cabd_id FROM {script.damUpdateTable}
    WHERE entry_classification = 'new feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND update_status = 'ready';

UPDATE {script.damTable} SET snapped_point = original_point WHERE snapped_point IS NULL;

WITH move_points AS (
    SELECT (ST_SetSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4617)) AS geom, cabd_id AS id
    FROM {script.damUpdateTable}
    WHERE entry_classification = 'modify feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND update_status = 'ready'
    )
UPDATE {script.damTable} AS foo 
    SET snapped_point = bar.geom,
        original_point = bar.geom
    FROM move_points AS bar
    WHERE cabd_id = bar.id;

-- add records to feature_source table
INSERT INTO {script.damFeatureTable} (cabd_id, datasource_id)
SELECT cabd_id, data_source FROM {script.damUpdateTable} WHERE update_status = 'ready'
ON CONFLICT DO NOTHING;

--------------------------------------------------------------------------
-- CHECK FOR ACCURACY
-- this should return 456 (pilot region features only) as of Jan 2023
-- if rows have been added for pilot region features since then, this should return 0
-- SELECT COUNT(*) FROM dams.dams
-- WHERE cabd_id NOT IN (SELECT cabd_id FROM dams.dams_feature_source);
--------------------------------------------------------------------------

-- update attribute_source table for live data
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    dam_name_fr_ds = CASE WHEN ({script.datasetName}.dam_name_fr IS NOT NULL AND {script.datasetName}.dam_name_fr IS DISTINCT FROM cabd.dam_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_fr_ds END,
    waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    waterbody_name_fr_ds = CASE WHEN ({script.datasetName}.waterbody_name_fr IS NOT NULL AND {script.datasetName}.waterbody_name_fr IS DISTINCT FROM cabd.waterbody_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_fr_ds END,
    reservoir_name_en_ds = CASE WHEN ({script.datasetName}.reservoir_name_en IS NOT NULL AND {script.datasetName}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_name_en_ds END,
    reservoir_name_fr_ds = CASE WHEN ({script.datasetName}.reservoir_name_fr IS NOT NULL AND {script.datasetName}.reservoir_name_fr IS DISTINCT FROM cabd.reservoir_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_name_fr_ds END,
    owner_ds = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.data_source ELSE cabdsource.owner_ds END,
    ownership_type_code_ds = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.ownership_type_code_ds END,
    provincial_compliance_status_ds = CASE WHEN ({script.datasetName}.provincial_compliance_status IS NOT NULL AND {script.datasetName}.provincial_compliance_status IS DISTINCT FROM cabd.provincial_compliance_status) THEN {script.datasetName}.data_source ELSE cabdsource.provincial_compliance_status_ds END,
    federal_compliance_status_ds = CASE WHEN ({script.datasetName}.federal_compliance_status IS NOT NULL AND {script.datasetName}.federal_compliance_status IS DISTINCT FROM cabd.federal_compliance_status) THEN {script.datasetName}.data_source ELSE cabdsource.federal_compliance_status_ds END,
    operating_notes_ds = CASE WHEN ({script.datasetName}.operating_notes IS NOT NULL AND {script.datasetName}.operating_notes IS DISTINCT FROM cabd.operating_notes) THEN {script.datasetName}.data_source ELSE cabdsource.operating_notes_ds END,
    operating_status_code_ds = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.operating_status_code_ds END,
    use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
    use_irrigation_code_ds = CASE WHEN ({script.datasetName}.use_irrigation_code IS NOT NULL AND {script.datasetName}.use_irrigation_code IS DISTINCT FROM cabd.use_irrigation_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_irrigation_code_ds END,
    use_electricity_code_ds = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_electricity_code_ds END,
    use_supply_code_ds = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_supply_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN ({script.datasetName}.use_floodcontrol_code IS NOT NULL AND {script.datasetName}.use_floodcontrol_code IS DISTINCT FROM cabd.use_floodcontrol_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    use_recreation_code_ds = CASE WHEN ({script.datasetName}.use_recreation_code IS NOT NULL AND {script.datasetName}.use_recreation_code IS DISTINCT FROM cabd.use_recreation_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_recreation_code_ds END,
    use_navigation_code_ds = CASE WHEN ({script.datasetName}.use_navigation_code IS NOT NULL AND {script.datasetName}.use_navigation_code IS DISTINCT FROM cabd.use_navigation_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_navigation_code_ds END,
    use_fish_code_ds = CASE WHEN ({script.datasetName}.use_fish_code IS NOT NULL AND {script.datasetName}.use_fish_code IS DISTINCT FROM cabd.use_fish_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_fish_code_ds END,
    use_pollution_code_ds = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_pollution_code_ds END,
    use_invasivespecies_code_ds = CASE WHEN ({script.datasetName}.use_invasivespecies_code IS NOT NULL AND {script.datasetName}.use_invasivespecies_code IS DISTINCT FROM cabd.use_invasivespecies_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_invasivespecies_code_ds END,
    --use_conservation_code_ds = CASE WHEN ({script.datasetName}.use_conservation_code IS NOT NULL AND {script.datasetName}.use_conservation_code IS DISTINCT FROM cabd.use_conservation_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_conservation_code_ds END,
    use_other_code_ds = CASE WHEN ({script.datasetName}.use_other_code IS NOT NULL AND {script.datasetName}.use_other_code IS DISTINCT FROM cabd.use_other_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_other_code_ds END,
    lake_control_code_ds = CASE WHEN ({script.datasetName}.lake_control_code IS NOT NULL AND {script.datasetName}.lake_control_code IS DISTINCT FROM cabd.lake_control_code) THEN {script.datasetName}.data_source ELSE cabdsource.lake_control_code_ds END,
    construction_year_ds = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.data_source ELSE cabdsource.construction_year_ds END,
    removed_year_ds = CASE WHEN ({script.datasetName}.removed_year IS NOT NULL AND {script.datasetName}.removed_year IS DISTINCT FROM cabd.removed_year) THEN {script.datasetName}.data_source ELSE cabdsource.removed_year_ds END,
    assess_schedule_ds = CASE WHEN ({script.datasetName}.assess_schedule IS NOT NULL AND {script.datasetName}.assess_schedule IS DISTINCT FROM cabd.assess_schedule) THEN {script.datasetName}.data_source ELSE cabdsource.assess_schedule_ds END,
    expected_end_of_life_ds = CASE WHEN ({script.datasetName}.expected_end_of_life IS NOT NULL AND {script.datasetName}.expected_end_of_life IS DISTINCT FROM cabd.expected_end_of_life) THEN {script.datasetName}.data_source ELSE cabdsource.expected_end_of_life_ds END,
    maintenance_last_ds = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.data_source ELSE cabdsource.maintenance_last_ds END,
    maintenance_next_ds = CASE WHEN ({script.datasetName}.maintenance_next IS NOT NULL AND {script.datasetName}.maintenance_next IS DISTINCT FROM cabd.maintenance_next) THEN {script.datasetName}.data_source ELSE cabdsource.maintenance_next_ds END,
    function_code_ds = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
    condition_code_ds = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN {script.datasetName}.data_source ELSE cabdsource.condition_code_ds END,
    structure_type_code_ds = CASE WHEN ({script.datasetName}.structure_type_code IS NOT NULL AND {script.datasetName}.structure_type_code IS DISTINCT FROM cabd.structure_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.structure_type_code_ds END,
    construction_material_code_ds = CASE WHEN ({script.datasetName}.construction_material_code IS NOT NULL AND {script.datasetName}.construction_material_code IS DISTINCT FROM cabd.construction_material_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_material_code_ds END,
    height_m_ds = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.data_source ELSE cabdsource.height_m_ds END,
    length_m_ds = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.data_source ELSE cabdsource.length_m_ds END,
    spillway_capacity_ds = CASE WHEN ({script.datasetName}.spillway_capacity IS NOT NULL AND {script.datasetName}.spillway_capacity IS DISTINCT FROM cabd.spillway_capacity) THEN {script.datasetName}.data_source ELSE cabdsource.spillway_capacity_ds END,
    spillway_type_code_ds = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.spillway_type_code_ds END,
    reservoir_present_ds = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_present_ds END,
    reservoir_area_skm_ds = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
    reservoir_depth_m_ds = CASE WHEN ({script.datasetName}.reservoir_depth_m IS NOT NULL AND {script.datasetName}.reservoir_depth_m IS DISTINCT FROM cabd.reservoir_depth_m) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_depth_m_ds END,
    storage_capacity_mcm_ds = CASE WHEN ({script.datasetName}.storage_capacity_mcm IS NOT NULL AND {script.datasetName}.storage_capacity_mcm IS DISTINCT FROM cabd.storage_capacity_mcm) THEN {script.datasetName}.data_source ELSE cabdsource.storage_capacity_mcm_ds END,
    avg_rate_of_discharge_ls_ds = CASE WHEN ({script.datasetName}.avg_rate_of_discharge_ls IS NOT NULL AND {script.datasetName}.avg_rate_of_discharge_ls IS DISTINCT FROM cabd.avg_rate_of_discharge_ls) THEN {script.datasetName}.data_source ELSE cabdsource.avg_rate_of_discharge_ls_ds END,
    degree_of_regulation_pc_ds = CASE WHEN ({script.datasetName}.degree_of_regulation_pc IS NOT NULL AND {script.datasetName}.degree_of_regulation_pc IS DISTINCT FROM cabd.degree_of_regulation_pc) THEN {script.datasetName}.data_source ELSE cabdsource.degree_of_regulation_pc_ds END,
    provincial_flow_req_ds = CASE WHEN ({script.datasetName}.provincial_flow_req IS NOT NULL AND {script.datasetName}.provincial_flow_req IS DISTINCT FROM cabd.provincial_flow_req) THEN {script.datasetName}.data_source ELSE cabdsource.provincial_flow_req_ds END,
    federal_flow_req_ds = CASE WHEN ({script.datasetName}.federal_flow_req IS NOT NULL AND {script.datasetName}.federal_flow_req IS DISTINCT FROM cabd.federal_flow_req) THEN {script.datasetName}.data_source ELSE cabdsource.federal_flow_req_ds END,
    hydro_peaking_system_ds = CASE WHEN ({script.datasetName}.hydro_peaking_system IS NOT NULL AND {script.datasetName}.hydro_peaking_system IS DISTINCT FROM cabd.hydro_peaking_system) THEN {script.datasetName}.data_source ELSE cabdsource.hydro_peaking_system_ds END,
    generating_capacity_mwh_ds = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
    turbine_number_ds = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.data_source ELSE cabdsource.turbine_number_ds END,
    turbine_type_code_ds = CASE WHEN ({script.datasetName}.turbine_type_code IS NOT NULL AND {script.datasetName}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.turbine_type_code_ds END,
    up_passage_type_code_ds = CASE WHEN ({script.datasetName}.up_passage_type_code IS NOT NULL AND {script.datasetName}.up_passage_type_code IS DISTINCT FROM cabd.up_passage_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.up_passage_type_code_ds END,
    down_passage_route_code_ds = CASE WHEN ({script.datasetName}.down_passage_route_code IS NOT NULL AND {script.datasetName}.down_passage_route_code IS DISTINCT FROM cabd.down_passage_route_code) THEN {script.datasetName}.data_source ELSE cabdsource.down_passage_route_code_ds END,
    comments_ds = CASE WHEN ({script.datasetName}.comments IS NOT NULL AND {script.datasetName}.comments IS DISTINCT FROM cabd.comments) THEN {script.datasetName}.data_source ELSE cabdsource.comments_ds END,
    passability_status_code_ds = CASE WHEN ({script.datasetName}.passability_status_code IS NOT NULL AND {script.datasetName}.passability_status_code IS DISTINCT FROM cabd.passability_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.passability_status_code_ds END,
    passability_status_note_ds = CASE WHEN ({script.datasetName}.passability_status_note IS NOT NULL AND {script.datasetName}.passability_status_note IS DISTINCT FROM cabd.passability_status_note) THEN {script.datasetName}.data_source ELSE cabdsource.passability_status_note_ds END,
    facility_name_en_ds = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_en_ds END,
    facility_name_fr_ds = CASE WHEN ({script.datasetName}.facility_name_fr IS NOT NULL AND {script.datasetName}.facility_name_fr IS DISTINCT FROM cabd.facility_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_fr_ds END
FROM
    {script.damTable} AS cabd,
    {script.damUpdateTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id and cabd.cabd_id = cabdsource.cabd_id
    AND {script.datasetName}.entry_classification IN ('new feature', 'modify feature')
    AND {script.datasetName}.update_status = 'ready'
    AND {script.datasetName}.data_source IS NOT NULL;

--update attributes in live data
UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    dam_name_fr = CASE WHEN ({script.datasetName}.dam_name_fr IS NOT NULL AND {script.datasetName}.dam_name_fr IS DISTINCT FROM cabd.dam_name_fr) THEN {script.datasetName}.dam_name_fr ELSE cabd.dam_name_fr END,
    waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,    
    waterbody_name_fr = CASE WHEN ({script.datasetName}.waterbody_name_fr IS NOT NULL AND {script.datasetName}.waterbody_name_fr IS DISTINCT FROM cabd.waterbody_name_fr) THEN {script.datasetName}.waterbody_name_fr ELSE cabd.waterbody_name_fr END,    
    reservoir_name_en = CASE WHEN ({script.datasetName}.reservoir_name_en IS NOT NULL AND {script.datasetName}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN {script.datasetName}.reservoir_name_en ELSE cabd.reservoir_name_en END,    
    reservoir_name_fr = CASE WHEN ({script.datasetName}.reservoir_name_fr IS NOT NULL AND {script.datasetName}.reservoir_name_fr IS DISTINCT FROM cabd.reservoir_name_fr) THEN {script.datasetName}.reservoir_name_fr ELSE cabd.reservoir_name_fr END,    
    "owner" = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.owner ELSE cabd.owner END,    
    ownership_type_code = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.ownership_type_code ELSE cabd.ownership_type_code END,    
    provincial_compliance_status = CASE WHEN ({script.datasetName}.provincial_compliance_status IS NOT NULL AND {script.datasetName}.provincial_compliance_status IS DISTINCT FROM cabd.provincial_compliance_status) THEN {script.datasetName}.provincial_compliance_status ELSE cabd.provincial_compliance_status END,    
    federal_compliance_status = CASE WHEN ({script.datasetName}.federal_compliance_status IS NOT NULL AND {script.datasetName}.federal_compliance_status IS DISTINCT FROM cabd.federal_compliance_status) THEN {script.datasetName}.federal_compliance_status ELSE cabd.federal_compliance_status END,    
    operating_notes = CASE WHEN ({script.datasetName}.operating_notes IS NOT NULL AND {script.datasetName}.operating_notes IS DISTINCT FROM cabd.operating_notes) THEN {script.datasetName}.operating_notes ELSE cabd.operating_notes END,    
    operating_status_code = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.operating_status_code ELSE cabd.operating_status_code END,    
    use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,    
    use_irrigation_code = CASE WHEN ({script.datasetName}.use_irrigation_code IS NOT NULL AND {script.datasetName}.use_irrigation_code IS DISTINCT FROM cabd.use_irrigation_code) THEN {script.datasetName}.use_irrigation_code ELSE cabd.use_irrigation_code END,    
    use_electricity_code = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetName}.use_electricity_code ELSE cabd.use_electricity_code END,    
    use_supply_code = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.use_supply_code ELSE cabd.use_supply_code END,    
    use_floodcontrol_code = CASE WHEN ({script.datasetName}.use_floodcontrol_code IS NOT NULL AND {script.datasetName}.use_floodcontrol_code IS DISTINCT FROM cabd.use_floodcontrol_code) THEN {script.datasetName}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,    
    use_recreation_code = CASE WHEN ({script.datasetName}.use_recreation_code IS NOT NULL AND {script.datasetName}.use_recreation_code IS DISTINCT FROM cabd.use_recreation_code) THEN {script.datasetName}.use_recreation_code ELSE cabd.use_recreation_code END,    
    use_navigation_code = CASE WHEN ({script.datasetName}.use_navigation_code IS NOT NULL AND {script.datasetName}.use_navigation_code IS DISTINCT FROM cabd.use_navigation_code) THEN {script.datasetName}.use_navigation_code ELSE cabd.use_navigation_code END,    
    use_fish_code = CASE WHEN ({script.datasetName}.use_fish_code IS NOT NULL AND {script.datasetName}.use_fish_code IS DISTINCT FROM cabd.use_fish_code) THEN {script.datasetName}.use_fish_code ELSE cabd.use_fish_code END,    
    use_pollution_code = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.use_pollution_code ELSE cabd.use_pollution_code END,    
    use_invasivespecies_code = CASE WHEN ({script.datasetName}.use_invasivespecies_code IS NOT NULL AND {script.datasetName}.use_invasivespecies_code IS DISTINCT FROM cabd.use_invasivespecies_code) THEN {script.datasetName}.use_invasivespecies_code ELSE cabd.use_invasivespecies_code END,    
    --use_conservation_code = CASE WHEN ({script.datasetName}.use_conservation_code IS NOT NULL AND {script.datasetName}.use_conservation_code IS DISTINCT FROM cabd.use_conservation_code) THEN {script.datasetName}.use_conservation_code ELSE cabd.use_conservation_code END,    
    use_other_code = CASE WHEN ({script.datasetName}.use_other_code IS NOT NULL AND {script.datasetName}.use_other_code IS DISTINCT FROM cabd.use_other_code) THEN {script.datasetName}.use_other_code ELSE cabd.use_other_code END,    
    lake_control_code = CASE WHEN ({script.datasetName}.lake_control_code IS NOT NULL AND {script.datasetName}.lake_control_code IS DISTINCT FROM cabd.lake_control_code) THEN {script.datasetName}.lake_control_code ELSE cabd.lake_control_code END,    
    construction_year = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.construction_year ELSE cabd.construction_year END,    
    removed_year = CASE WHEN ({script.datasetName}.removed_year IS NOT NULL AND {script.datasetName}.removed_year IS DISTINCT FROM cabd.removed_year) THEN {script.datasetName}.removed_year ELSE cabd.removed_year END,    
    assess_schedule = CASE WHEN ({script.datasetName}.assess_schedule IS NOT NULL AND {script.datasetName}.assess_schedule IS DISTINCT FROM cabd.assess_schedule) THEN {script.datasetName}.assess_schedule ELSE cabd.assess_schedule END,    
    expected_end_of_life = CASE WHEN ({script.datasetName}.expected_end_of_life IS NOT NULL AND {script.datasetName}.expected_end_of_life IS DISTINCT FROM cabd.expected_end_of_life) THEN {script.datasetName}.expected_end_of_life ELSE cabd.expected_end_of_life END,    
    maintenance_last = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.maintenance_last ELSE cabd.maintenance_last END,    
    maintenance_next = CASE WHEN ({script.datasetName}.maintenance_next IS NOT NULL AND {script.datasetName}.maintenance_next IS DISTINCT FROM cabd.maintenance_next) THEN {script.datasetName}.maintenance_next ELSE cabd.maintenance_next END,    
    function_code = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.function_code ELSE cabd.function_code END,    
    condition_code = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN {script.datasetName}.condition_code ELSE cabd.condition_code END,    
    structure_type_code = CASE WHEN ({script.datasetName}.structure_type_code IS NOT NULL AND {script.datasetName}.structure_type_code IS DISTINCT FROM cabd.structure_type_code) THEN {script.datasetName}.structure_type_code ELSE cabd.structure_type_code END,    
    construction_material_code = CASE WHEN ({script.datasetName}.construction_material_code IS NOT NULL AND {script.datasetName}.construction_material_code IS DISTINCT FROM cabd.construction_material_code) THEN {script.datasetName}.construction_material_code ELSE cabd.construction_material_code END,    
    height_m = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.height_m ELSE cabd.height_m END,    
    length_m = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.length_m ELSE cabd.length_m END,    
    spillway_capacity = CASE WHEN ({script.datasetName}.spillway_capacity IS NOT NULL AND {script.datasetName}.spillway_capacity IS DISTINCT FROM cabd.spillway_capacity) THEN {script.datasetName}.spillway_capacity ELSE cabd.spillway_capacity END,    
    spillway_type_code = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.spillway_type_code ELSE cabd.spillway_type_code END,    
    reservoir_present = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.reservoir_present ELSE cabd.reservoir_present END,    
    reservoir_area_skm = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetName}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,    
    reservoir_depth_m = CASE WHEN ({script.datasetName}.reservoir_depth_m IS NOT NULL AND {script.datasetName}.reservoir_depth_m IS DISTINCT FROM cabd.reservoir_depth_m) THEN {script.datasetName}.reservoir_depth_m ELSE cabd.reservoir_depth_m END,    
    storage_capacity_mcm = CASE WHEN ({script.datasetName}.storage_capacity_mcm IS NOT NULL AND {script.datasetName}.storage_capacity_mcm IS DISTINCT FROM cabd.storage_capacity_mcm) THEN {script.datasetName}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,    
    avg_rate_of_discharge_ls = CASE WHEN ({script.datasetName}.avg_rate_of_discharge_ls IS NOT NULL AND {script.datasetName}.avg_rate_of_discharge_ls IS DISTINCT FROM cabd.avg_rate_of_discharge_ls) THEN {script.datasetName}.avg_rate_of_discharge_ls ELSE cabd.avg_rate_of_discharge_ls END,    
    degree_of_regulation_pc = CASE WHEN ({script.datasetName}.degree_of_regulation_pc IS NOT NULL AND {script.datasetName}.degree_of_regulation_pc IS DISTINCT FROM cabd.degree_of_regulation_pc) THEN {script.datasetName}.degree_of_regulation_pc ELSE cabd.degree_of_regulation_pc END,    
    provincial_flow_req = CASE WHEN ({script.datasetName}.provincial_flow_req IS NOT NULL AND {script.datasetName}.provincial_flow_req IS DISTINCT FROM cabd.provincial_flow_req) THEN {script.datasetName}.provincial_flow_req ELSE cabd.provincial_flow_req END,    
    federal_flow_req = CASE WHEN ({script.datasetName}.federal_flow_req IS NOT NULL AND {script.datasetName}.federal_flow_req IS DISTINCT FROM cabd.federal_flow_req) THEN {script.datasetName}.federal_flow_req ELSE cabd.federal_flow_req END,    
    hydro_peaking_system = CASE WHEN ({script.datasetName}.hydro_peaking_system IS NOT NULL AND {script.datasetName}.hydro_peaking_system IS DISTINCT FROM cabd.hydro_peaking_system) THEN {script.datasetName}.hydro_peaking_system ELSE cabd.hydro_peaking_system END,    
    generating_capacity_mwh = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,    
    turbine_number = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.turbine_number ELSE cabd.turbine_number END,    
    turbine_type_code = CASE WHEN ({script.datasetName}.turbine_type_code IS NOT NULL AND {script.datasetName}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN {script.datasetName}.turbine_type_code ELSE cabd.turbine_type_code END,    
    up_passage_type_code = CASE WHEN ({script.datasetName}.up_passage_type_code IS NOT NULL AND {script.datasetName}.up_passage_type_code IS DISTINCT FROM cabd.up_passage_type_code) THEN {script.datasetName}.up_passage_type_code ELSE cabd.up_passage_type_code END,    
    down_passage_route_code = CASE WHEN ({script.datasetName}.down_passage_route_code IS NOT NULL AND {script.datasetName}.down_passage_route_code IS DISTINCT FROM cabd.down_passage_route_code) THEN {script.datasetName}.down_passage_route_code ELSE cabd.down_passage_route_code END,    
    "comments" = CASE WHEN ({script.datasetName}.comments IS NOT NULL AND {script.datasetName}.comments IS DISTINCT FROM cabd.comments) THEN {script.datasetName}.comments ELSE cabd.comments END,    
    passability_status_code = CASE WHEN ({script.datasetName}.passability_status_code IS NOT NULL AND {script.datasetName}.passability_status_code IS DISTINCT FROM cabd.passability_status_code) THEN {script.datasetName}.passability_status_code ELSE cabd.passability_status_code END,    
    passability_status_note = CASE WHEN ({script.datasetName}.passability_status_note IS NOT NULL AND {script.datasetName}.passability_status_note IS DISTINCT FROM cabd.passability_status_note) THEN {script.datasetName}.passability_status_note ELSE cabd.passability_status_note END,    
    use_analysis = CASE WHEN ({script.datasetName}.use_analysis IS NOT NULL AND {script.datasetName}.use_analysis IS DISTINCT FROM cabd.use_analysis) THEN {script.datasetName}.use_analysis ELSE cabd.use_analysis END,    
    facility_name_en = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.facility_name_en ELSE cabd.facility_name_en END,    
    facility_name_fr = CASE WHEN ({script.datasetName}.facility_name_fr IS NOT NULL AND {script.datasetName}.facility_name_fr IS DISTINCT FROM cabd.facility_name_fr) THEN {script.datasetName}.facility_name_fr ELSE cabd.facility_name_fr END
FROM
    {script.damUpdateTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id
    AND {script.datasetName}.entry_classification IN ('new feature', 'modify feature')
    AND {script.datasetName}.update_status = 'ready'
    AND {script.datasetName}.data_source IS NOT NULL;

-- deal with records to be deleted
DELETE FROM {script.damAttributeTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.damUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');
DELETE FROM {script.damFeatureTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.damUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');
DELETE FROM {script.damTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.damUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');

--------------------------------------------------------------------------
-- CHECK FOR ACCURACY
-- use counts from entry_classification field to check your work
-- e.g., for update_status of 'ready' and entry_classification of 'new feature'
-- two counts below should match
--
-- SELECT COUNT(DISTINCT cabd_id) FROM cabd.audit_log
-- WHERE action = 'INSERT' AND datetime::date = current_date AND tablename = 'dams';
--
-- SELECT COUNT(DISTINCT cabd_id) FROM cabd.audit_log
-- WHERE action = 'INSERT' AND datetime::date = current_date AND tablename = 'dams_attribute_source';
--------------------------------------------------------------------------

-- set records to 'done' and delete from update table
UPDATE {script.damUpdateTable} SET update_status = 'done' WHERE update_status = 'ready';
UPDATE {script.damUpdateTable} SET update_status = 'ready' WHERE update_status = 'wait';
-- DELETE FROM {script.damUpdateTable} WHERE update_status = 'done';

"""

script.do_work(query, initializequery, mappingquery)
