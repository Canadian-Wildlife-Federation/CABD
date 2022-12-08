import user_submit as main

#change the datasetName below for each round of updates
script = main.MappingScript("user_submitted_updates")

mappingquery = f"""
--add new data sources and match new uuids for each data source to each record
ALTER TABLE cabd.data_source ADD CONSTRAINT unique_name (name);
INSERT INTO cabd.data_source (name, id, source_type)
    SELECT DISTINCT data_source_short_name, gen_random_uuid(), 'non-spatial' FROM  {script.damUpdateTable}
    ON CONFLICT DO NOTHING;
ALTER TABLE cabd.data_source DROP CONSTRAINT unique_name;

--add data source ids to the table
ALTER TABLE  {script.damUpdateTable} ADD COLUMN data_source uuid;
UPDATE {script.damUpdateTable} AS s SET data_source = d.id FROM cabd.data_source AS d WHERE d.name = s.data_source_short_name;

--deal with new and modified records
INSERT INTO {script.damTable} (original_point)
    (SELECT (ST_SetSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4617)) FROM {script.damUpdateTable}
    WHERE entry_classification = 'new feature');

UPDATE {script.damTable} SET snapped_point = original_point WHERE snapped_point IS NULL;

UPDATE {script.damTable} SET snapped_point = 
    (SELECT (ST_SetSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4617)) FROM {script.damUpdateTable}
    WHERE entry_classification = 'modify feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL);

--update attribute_source table for live data
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    dam_name_fr_ds = CASE WHEN ({script.datasetName}.dam_name_fr IS NOT NULL AND {script.datasetName}.dam_name_fr IS DISTINCT FROM cabd.dam_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_fr_ds END,
    waterbody_name_en_ds =
    waterbody_name_fr_ds =
    reservoir_name_en_ds =
    reservoir_name_fr_ds =
    owner_ds =
    ownership_type_code_ds =
    provincial_compliance_status_ds =
    federal_compliance_status_ds =
    operating_notes_ds =
    operating_status_code_ds =
    use_code_ds =
    use_irrigation_code_ds =
    use_electricity_code_ds =
    use_supply_code_ds =
    use_floodcontrol_code_ds =
    use_recreation_code_ds =
    use_navigation_code_ds =
    use_fish_code_ds =
    use_pollution_code_ds =
    use_invasivespecies_code_ds =
    use_conservation_code_ds =
    use_other_code_ds =
    lake_control_code_ds =
    construction_year_ds =
    removed_year_ds =
    assess_schedule_ds =
    expected_end_of_life_ds =
    maintenance_last_ds =
    maintenance_next_ds =
    function_code_ds =
    condition_code_ds =
    structure_type_code_ds =
    construction_material_code_ds =
    height_m_ds =
    length_m_ds =
    spillway_capacity_ds =
    spillway_type_code_ds =
    reservoir_present_ds =
    reservoir_area_skm_ds =
    reservoir_depth_m_ds =
    storage_capacity_mcm_ds =
    avg_rate_of_discharge_ls_ds =
    degree_of_regulation_pc_ds =
    provincial_flow_req_ds =
    federal_flow_req_ds =
    hydro_peaking_system_ds =
    generating_capacity_mwh_ds =
    turbine_number_ds =
    turbine_type_code_ds =
    up_passage_type_code_ds =
    down_passage_route_code_ds =
    comments_ds =
    passability_status_code_ds =
    passability_status_note_ds =
    use_analysis_ds =
    facility_name_en_ds =
    facility_name_fr_ds = 
FROM
    {script.damTable} AS cabd,
    {script.damUpdateTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id and cabd.cabd_id = cabdsource.cabd_id
    AND {script.datasetName}.entry_classification IN ('new feature', 'modify feature');

--update attributes in live data
UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    dam_name_fr = CASE WHEN ({script.datasetName}.dam_name_fr IS NOT NULL AND {script.datasetName}.dam_name_fr IS DISTINCT FROM cabd.dam_name_fr) THEN {script.datasetName}.dam_name_fr ELSE cabd.dam_name_fr END,
    waterbody_name_en =
    waterbody_name_fr =
    reservoir_name_en =
    reservoir_name_fr =
    owner =
    ownership_type_code =
    provincial_compliance_status =
    federal_compliance_status =
    operating_notes =
    operating_status_code =
    use_code =
    use_irrigation_code =
    use_electricity_code =
    use_supply_code =
    use_floodcontrol_code =
    use_recreation_code =
    use_navigation_code =
    use_fish_code =
    use_pollution_code =
    use_invasivespecies_code =
    use_conservation_code =
    use_other_code =
    lake_control_code =
    construction_year =
    removed_year =
    assess_schedule =
    expected_end_of_life =
    maintenance_last =
    maintenance_next =
    function_code =
    condition_code =
    structure_type_code =
    construction_material_code =
    height_m =
    length_m =
    spillway_capacity =
    spillway_type_code =
    reservoir_present =
    reservoir_area_skm =
    reservoir_depth_m =
    storage_capacity_mcm =
    avg_rate_of_discharge_ls =
    degree_of_regulation_pc =
    provincial_flow_req =
    federal_flow_req =
    hydro_peaking_system =
    generating_capacity_mwh =
    turbine_number =
    turbine_type_code =
    up_passage_type_code =
    down_passage_route_code =
    comments =
    passability_status_code =
    passability_status_note =
    use_analysis =
    facility_name_en =
    facility_name_fr =

FROM
    {script.damUpdateTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id
    AND {script.datasetName}.entry_classification IN ('new feature', 'modify feature');

--deal with records to be deleted
DELETE FROM {script.damAttributeTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.damUpdateTable} WHERE entry_classification = 'delete feature');
DELETE FROM {script.damFeatureTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.damUpdateTable} WHERE entry_classification = 'delete feature');
DELETE FROM {script.damTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.damUpdateTable} WHERE entry_classification = 'delete feature');

"""

print(mappingquery)
# script.do_work(mappingquery)