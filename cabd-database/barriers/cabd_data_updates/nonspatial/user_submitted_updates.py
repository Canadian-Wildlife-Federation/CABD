import nonspatial as main

script = main.MappingScript("user_submitted_updates")

mappingquery = f"""

--TO DO: figure out if there is a better system than below for creating new records
--could be based on some column(s) we include in our CSV input to this script?
--we currently would just be hardcoding most of these values

--code below adds too many rows, will have to figure out how to deal with this
--INSERT INTO cabd.data_source (name, id, source_type)
--SELECT DISTINCT data_source, uuid_generate_v4(), 'non-spatial' FROM {script.sourceTable};

--create new data source records
INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    '...',
    '...',
    '...',
    'non-spatial');

--add data source ids to the table
ALTER TABLE {script.sourceTable} RENAME COLUMN data_source to data_source_text;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} AS s SET data_source = d.id FROM cabd.data_source AS d WHERE d.name = s.data_source_text;

--SELECT DISTINCT data_source, data_source_text FROM {script.sourceTable};

--TO DO: figure out how we want to handle the DISTINCT clause for coded value fields
--users will be giving us the name instead of the id for these in the template
--possible solution: run a pre-processing script on the CSV input to switch these values?

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    assess_schedule_ds = CASE WHEN ({script.datasetName}.assess_schedule IS NOT NULL AND {script.datasetName}.assess_schedule IS DISTINCT FROM cabd.assess_schedule) THEN {script.datasetName}.data_source ELSE cabdsource.assess_schedule_ds END,
    avg_rate_of_discharge_ls_ds = CASE WHEN ({script.datasetName}.avg_rate_of_discharge_ls IS NOT NULL AND {script.datasetName}.avg_rate_of_discharge_ls IS DISTINCT FROM cabd.avg_rate_of_discharge_ls) THEN {script.datasetName}.data_source ELSE cabdsource.avg_rate_of_discharge_ls_ds END,
    condition_code_ds = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN {script.datasetName}.data_source ELSE cabdsource.condition_code_ds END,    
    comments_ds = CASE WHEN ({script.datasetName}.comments IS NOT NULL AND {script.datasetName}.comments IS DISTINCT FROM cabd.comments) THEN {script.datasetName}.data_source ELSE cabdsource.comments_ds END,
    construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
    construction_year_ds = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.data_source ELSE cabdsource.construction_year_ds END,
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    dam_name_fr_ds = CASE WHEN ({script.datasetName}.dam_name_fr IS NOT NULL AND {script.datasetName}.dam_name_fr IS DISTINCT FROM cabd.dam_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_fr_ds END,
    degree_of_regulation_pc_ds = CASE WHEN ({script.datasetName}.degree_of_regulation_pc IS NOT NULL AND {script.datasetName}.degree_of_regulation_pc IS DISTINCT FROM cabd.degree_of_regulation_pc) THEN {script.datasetName}.data_source ELSE cabdsource.degree_of_regulation_pc_ds END,
    down_passage_route_code_ds = CASE WHEN ({script.datasetName}.down_passage_route_code IS NOT NULL AND {script.datasetName}.down_passage_route_code IS DISTINCT FROM cabd.down_passage_route_code) THEN {script.datasetName}.data_source ELSE cabdsource.down_passage_route_code_ds END,
    expected_life_ds = CASE WHEN ({script.datasetName}.expected_life IS NOT NULL AND {script.datasetName}.expected_life IS DISTINCT FROM cabd.expected_life) THEN {script.datasetName}.data_source ELSE cabdsource.expected_life_ds END,
    facility_name_en_ds = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_en_ds END,
    facility_name_fr_ds = CASE WHEN ({script.datasetName}.facility_name_fr IS NOT NULL AND {script.datasetName}.facility_name_fr IS DISTINCT FROM cabd.facility_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_fr_ds END,
    federal_compliance_status_ds = CASE WHEN ({script.datasetName}.federal_compliance_status IS NOT NULL AND {script.datasetName}.federal_compliance_status IS DISTINCT FROM cabd.federal_compliance_status) THEN {script.datasetName}.data_source ELSE cabdsource.federal_compliance_status_ds END,
    federal_flow_req_ds = CASE WHEN ({script.datasetName}.federal_flow_req IS NOT NULL AND {script.datasetName}.federal_flow_req IS DISTINCT FROM cabd.federal_flow_req) THEN {script.datasetName}.data_source ELSE cabdsource.federal_flow_req_ds END,
    function_code_ds = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
    generating_capacity_mwh_ds = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
    height_m_ds = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.data_source ELSE cabdsource.height_m_ds END,
    hydro_peaking_system_ds = CASE WHEN ({script.datasetName}.hydro_peaking_system IS NOT NULL AND {script.datasetName}.hydro_peaking_system IS DISTINCT FROM cabd.hydro_peaking_system) THEN {script.datasetName}.data_source ELSE cabdsource.hydro_peaking_system_ds END,
    lake_control_code_ds = CASE WHEN ({script.datasetName}.lake_control_code IS NOT NULL AND {script.datasetName}.lake_control_code IS DISTINCT FROM cabd.lake_control_code) THEN {script.datasetName}.data_source ELSE cabdsource.lake_control_code_ds END,
    length_m_ds = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.data_source ELSE cabdsource.length_m_ds END,
    maintenance_last_ds = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.data_source ELSE cabdsource.maintenance_last_ds END,
    maintenance_next_ds = CASE WHEN ({script.datasetName}.maintenance_next IS NOT NULL AND {script.datasetName}.maintenance_next IS DISTINCT FROM cabd.maintenance_next) THEN {script.datasetName}.data_source ELSE cabdsource.maintenance_next_ds END,
    operating_notes_ds = CASE WHEN ({script.datasetName}.operating_notes IS NOT NULL AND {script.datasetName}.operating_notes IS DISTINCT FROM cabd.operating_notes) THEN {script.datasetName}.data_source ELSE cabdsource.operating_notes_ds END,
    operating_status_code_ds = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.operating_status_code_ds END,
    owner_ds = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.data_source ELSE cabdsource.owner_ds END,
    ownership_type_code_ds = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.ownership_type_code_ds END,
    passability_status_code_ds = CASE WHEN ({script.datasetName}.passability_status_code IS NOT NULL AND {script.datasetName}.passability_status_code IS DISTINCT FROM cabd.passability_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.passability_status_code_ds END,
    passability_status_note_ds = CASE WHEN ({script.datasetName}.passability_status_note IS NOT NULL AND {script.datasetName}.passability_status_note IS DISTINCT FROM cabd.passability_status_note) THEN {script.datasetName}.data_source ELSE cabdsource.passability_status_note_ds END,
    provincial_compliance_status_ds = CASE WHEN ({script.datasetName}.provincial_compliance_status IS NOT NULL AND {script.datasetName}.provincial_compliance_status IS DISTINCT FROM cabd.provincial_compliance_status) THEN {script.datasetName}.data_source ELSE cabdsource.provincial_compliance_status_ds END,
    provincial_flow_req_ds = CASE WHEN ({script.datasetName}.provincial_flow_req IS NOT NULL AND {script.datasetName}.provincial_flow_req IS DISTINCT FROM cabd.provincial_flow_req) THEN {script.datasetName}.data_source ELSE cabdsource.provincial_flow_req_ds END,
    reservoir_area_skm_ds = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
    reservoir_depth_m_ds = CASE WHEN ({script.datasetName}.reservoir_depth_m IS NOT NULL AND {script.datasetName}.reservoir_depth_m IS DISTINCT FROM cabd.reservoir_depth_m) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_depth_m_ds END,
    reservoir_name_en_ds = CASE WHEN ({script.datasetName}.reservoir_name_en IS NOT NULL AND {script.datasetName}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_name_en_ds END,
    reservoir_name_fr_ds = CASE WHEN ({script.datasetName}.reservoir_name_fr IS NOT NULL AND {script.datasetName}.reservoir_name_fr IS DISTINCT FROM cabd.reservoir_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_name_fr_ds END,
    reservoir_present_ds = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_present_ds END,
    spillway_capacity_ds = CASE WHEN ({script.datasetName}.spillway_capacity IS NOT NULL AND {script.datasetName}.spillway_capacity IS DISTINCT FROM cabd.spillway_capacity) THEN {script.datasetName}.data_source ELSE cabdsource.spillway_capacity_ds END,
    spillway_type_code_ds = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.spillway_type_code_ds END,
    storage_capacity_mcm_ds = CASE WHEN ({script.datasetName}.storage_capacity_mcm IS NOT NULL AND {script.datasetName}.storage_capacity_mcm IS DISTINCT FROM cabd.storage_capacity_mcm) THEN {script.datasetName}.data_source ELSE cabdsource.storage_capacity_mcm_ds END,
    turbine_number_ds = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.data_source ELSE cabdsource.turbine_number_ds END,
    turbine_type_code_ds = CASE WHEN ({script.datasetName}.turbine_type_code IS NOT NULL AND {script.datasetName}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.turbine_type_code_ds END,
    up_passage_type_code_ds = CASE WHEN ({script.datasetName}.up_passage_type_code IS NOT NULL AND {script.datasetName}.up_passage_type_code IS DISTINCT FROM cabd.up_passage_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.up_passage_type_code_ds END,
    use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
    use_electricity_code_ds = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_electricity_code_ds END,
    use_fish_code_ds = CASE WHEN ({script.datasetName}.use_fish_code IS NOT NULL AND {script.datasetName}.use_fish_code IS DISTINCT FROM cabd.use_fish_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_fish_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN ({script.datasetName}.use_floodcontrol_code IS NOT NULL AND {script.datasetName}.use_floodcontrol_code IS DISTINCT FROM cabd.use_floodcontrol_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    use_invasivespecies_code_ds = CASE WHEN ({script.datasetName}.use_invasivespecies_code IS NOT NULL AND {script.datasetName}.use_invasivespecies_code IS DISTINCT FROM cabd.use_invasivespecies_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_invasivespecies_code_ds END,
    use_irrigation_code_ds = CASE WHEN ({script.datasetName}.use_irrigation_code IS NOT NULL AND {script.datasetName}.use_irrigation_code IS DISTINCT FROM cabd.use_irrigation_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_irrigation_code_ds END,
    use_navigation_code_ds = CASE WHEN ({script.datasetName}.use_navigation_code IS NOT NULL AND {script.datasetName}.use_navigation_code IS DISTINCT FROM cabd.use_navigation_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_navigation_code_ds END,
    use_other_code_ds = CASE WHEN ({script.datasetName}.use_other_code IS NOT NULL AND {script.datasetName}.use_other_code IS DISTINCT FROM cabd.use_other_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_other_code_ds END,
    use_pollution_code_ds = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_pollution_code_ds END,
    use_recreation_code_ds = CASE WHEN ({script.datasetName}.use_recreation_code IS NOT NULL AND {script.datasetName}.use_recreation_code IS DISTINCT FROM cabd.use_recreation_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_recreation_code_ds END,
    use_supply_code_ds = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_supply_code_ds END,
    waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    waterbody_name_fr_ds = CASE WHEN ({script.datasetName}.waterbody_name_fr IS NOT NULL AND {script.datasetName}.waterbody_name_fr IS DISTINCT FROM cabd.waterbody_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_fr_ds END

FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    assess_schedule = CASE WHEN ({script.datasetName}.assess_schedule IS NOT NULL AND {script.datasetName}.assess_schedule IS DISTINCT FROM cabd.assess_schedule) THEN {script.datasetName}.assess_schedule ELSE cabd.assess_schedule END,
    avg_rate_of_discharge_ls = CASE WHEN ({script.datasetName}.avg_rate_of_discharge_ls IS NOT NULL AND {script.datasetName}.avg_rate_of_discharge_ls IS DISTINCT FROM cabd.avg_rate_of_discharge_ls) THEN {script.datasetName}.avg_rate_of_discharge_ls ELSE cabd.avg_rate_of_discharge_ls END,
    condition_code = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN {script.datasetName}.condition_code ELSE cabd.condition_code END,
    "comments" = CASE WHEN ({script.datasetName}.comments IS NOT NULL AND {script.datasetName}.comments IS DISTINCT FROM cabd.comments) THEN {script.datasetName}.comments ELSE cabd.comments END,
    construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
    construction_year = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.construction_year ELSE cabd.construction_year END,
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    dam_name_fr = CASE WHEN ({script.datasetName}.dam_name_fr IS NOT NULL AND {script.datasetName}.dam_name_fr IS DISTINCT FROM cabd.dam_name_fr) THEN {script.datasetName}.dam_name_fr ELSE cabd.dam_name_fr END,
    degree_of_regulation_pc = CASE WHEN ({script.datasetName}.degree_of_regulation_pc IS NOT NULL AND {script.datasetName}.degree_of_regulation_pc IS DISTINCT FROM cabd.degree_of_regulation_pc) THEN {script.datasetName}.degree_of_regulation_pc ELSE cabd.degree_of_regulation_pc END,
    down_passage_route_code = CASE WHEN ({script.datasetName}.down_passage_route_code IS NOT NULL AND {script.datasetName}.down_passage_route_code IS DISTINCT FROM cabd.down_passage_route_code) THEN {script.datasetName}.down_passage_route_code ELSE cabd.down_passage_route_code END,
    expected_life = CASE WHEN ({script.datasetName}.expected_life IS NOT NULL AND {script.datasetName}.expected_life IS DISTINCT FROM cabd.expected_life) THEN {script.datasetName}.expected_life ELSE cabd.expected_life END,
    facility_name_en = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.facility_name_en ELSE cabd.facility_name_en END,
    facility_name_fr = CASE WHEN ({script.datasetName}.facility_name_fr IS NOT NULL AND {script.datasetName}.facility_name_fr IS DISTINCT FROM cabd.facility_name_fr) THEN {script.datasetName}.facility_name_fr ELSE cabd.facility_name_fr END,
    federal_compliance_status = CASE WHEN ({script.datasetName}.federal_compliance_status IS NOT NULL AND {script.datasetName}.federal_compliance_status IS DISTINCT FROM cabd.federal_compliance_status) THEN {script.datasetName}.federal_compliance_status ELSE cabd.federal_compliance_status END,
    federal_flow_req = CASE WHEN ({script.datasetName}.federal_flow_req IS NOT NULL AND {script.datasetName}.federal_flow_req IS DISTINCT FROM cabd.federal_flow_req) THEN {script.datasetName}.federal_flow_req ELSE cabd.federal_flow_req END,
    function_code = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.function_code ELSE cabd.function_code END,
    generating_capacity_mwh = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
    height_m = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.height_m ELSE cabd.height_m END,
    hydro_peaking_system = CASE WHEN ({script.datasetName}.hydro_peaking_system IS NOT NULL AND {script.datasetName}.hydro_peaking_system IS DISTINCT FROM cabd.hydro_peaking_system) THEN {script.datasetName}.hydro_peaking_system ELSE cabd.hydro_peaking_system END,
    lake_control_code = CASE WHEN ({script.datasetName}.lake_control_code IS NOT NULL AND {script.datasetName}.lake_control_code IS DISTINCT FROM cabd.lake_control_code) THEN {script.datasetName}.lake_control_code ELSE cabd.lake_control_code END,
    length_m = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.length_m ELSE cabd.length_m END,
    maintenance_last = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.maintenance_last ELSE cabd.maintenance_last END,
    maintenance_next = CASE WHEN ({script.datasetName}.maintenance_next IS NOT NULL AND {script.datasetName}.maintenance_next IS DISTINCT FROM cabd.maintenance_next) THEN {script.datasetName}.maintenance_next ELSE cabd.maintenance_next END,
    operating_notes = CASE WHEN ({script.datasetName}.operating_notes IS NOT NULL AND {script.datasetName}.operating_notes IS DISTINCT FROM cabd.operating_notes) THEN {script.datasetName}.operating_notes ELSE cabd.operating_notes END,
    operating_status_code = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.operating_status_code ELSE cabd.operating_status_code END,
    "owner" = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.ownership_type_code ELSE cabd.ownership_type_code END,
    passability_status_code = CASE WHEN ({script.datasetName}.passability_status_code IS NOT NULL AND {script.datasetName}.passability_status_code IS DISTINCT FROM cabd.passability_status_code) THEN {script.datasetName}.passability_status_code ELSE cabd.passability_status_code END,
    passability_status_note = CASE WHEN ({script.datasetName}.passability_status_note IS NOT NULL AND {script.datasetName}.passability_status_note IS DISTINCT FROM cabd.passability_status_note) THEN {script.datasetName}.passability_status_note ELSE cabd.passability_status_note END,
    provincial_compliance_status = CASE WHEN ({script.datasetName}.provincial_compliance_status IS NOT NULL AND {script.datasetName}.provincial_compliance_status IS DISTINCT FROM cabd.provincial_compliance_status) THEN {script.datasetName}.provincial_compliance_status ELSE cabd.provincial_compliance_status END,
    provincial_flow_req = CASE WHEN ({script.datasetName}.provincial_flow_req IS NOT NULL AND {script.datasetName}.provincial_flow_req IS DISTINCT FROM cabd.provincial_flow_req) THEN {script.datasetName}.provincial_flow_req ELSE cabd.provincial_flow_req END,
    reservoir_area_skm = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetName}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    reservoir_depth_m = CASE WHEN ({script.datasetName}.reservoir_depth_m IS NOT NULL AND {script.datasetName}.reservoir_depth_m IS DISTINCT FROM cabd.reservoir_depth_m) THEN {script.datasetName}.reservoir_depth_m ELSE cabd.reservoir_depth_m END,
    reservoir_name_en = CASE WHEN ({script.datasetName}.reservoir_name_en IS NOT NULL AND {script.datasetName}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN {script.datasetName}.reservoir_name_en ELSE cabd.reservoir_name_en END,
    reservoir_name_fr = CASE WHEN ({script.datasetName}.reservoir_name_fr IS NOT NULL AND {script.datasetName}.reservoir_name_fr IS DISTINCT FROM cabd.reservoir_name_fr) THEN {script.datasetName}.reservoir_name_fr ELSE cabd.reservoir_name_fr END,
    reservoir_present = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.reservoir_present ELSE cabd.reservoir_present END,
    spillway_capacity = CASE WHEN ({script.datasetName}.spillway_capacity IS NOT NULL AND {script.datasetName}.spillway_capacity IS DISTINCT FROM cabd.spillway_capacity) THEN {script.datasetName}.spillway_capacity ELSE cabd.spillway_capacity END,
    spillway_type_code = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.spillway_type_code ELSE cabd.spillway_type_code END,
    storage_capacity_mcm = CASE WHEN ({script.datasetName}.storage_capacity_mcm IS NOT NULL AND {script.datasetName}.storage_capacity_mcm IS DISTINCT FROM cabd.storage_capacity_mcm) THEN {script.datasetName}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,
    turbine_number = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.turbine_number ELSE cabd.turbine_number END,
    turbine_type_code = CASE WHEN ({script.datasetName}.turbine_type_code IS NOT NULL AND {script.datasetName}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN {script.datasetName}.turbine_type_code ELSE cabd.turbine_type_code END,
    up_passage_type_code = CASE WHEN ({script.datasetName}.up_passage_type_code IS NOT NULL AND {script.datasetName}.up_passage_type_code IS DISTINCT FROM cabd.up_passage_type_code) THEN {script.datasetName}.up_passage_type_code ELSE cabd.up_passage_type_code END,
    use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
    use_electricity_code = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetName}.use_electricity_code ELSE cabd.use_electricity_code END,
    use_fish_code = CASE WHEN ({script.datasetName}.use_fish_code IS NOT NULL AND {script.datasetName}.use_fish_code IS DISTINCT FROM cabd.use_fish_code) THEN {script.datasetName}.use_fish_code ELSE cabd.use_fish_code END,
    use_floodcontrol_code = CASE WHEN ({script.datasetName}.use_floodcontrol_code IS NOT NULL AND {script.datasetName}.use_floodcontrol_code IS DISTINCT FROM cabd.use_floodcontrol_code) THEN {script.datasetName}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    use_invasivespecies_code = CASE WHEN ({script.datasetName}.use_invasivespecies_code IS NOT NULL AND {script.datasetName}.use_invasivespecies_code IS DISTINCT FROM cabd.use_invasivespecies_code) THEN {script.datasetName}.use_invasivespecies_code ELSE cabd.use_invasivespecies_code END,
    use_irrigation_code = CASE WHEN ({script.datasetName}.use_irrigation_code IS NOT NULL AND {script.datasetName}.use_irrigation_code IS DISTINCT FROM cabd.use_irrigation_code) THEN {script.datasetName}.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_navigation_code = CASE WHEN ({script.datasetName}.use_navigation_code IS NOT NULL AND {script.datasetName}.use_navigation_code IS DISTINCT FROM cabd.use_navigation_code) THEN {script.datasetName}.use_navigation_code ELSE cabd.use_navigation_code END,
    use_other_code = CASE WHEN ({script.datasetName}.use_other_code IS NOT NULL AND {script.datasetName}.use_other_code IS DISTINCT FROM cabd.use_other_code) THEN {script.datasetName}.use_other_code ELSE cabd.use_other_code END,
    use_pollution_code = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.use_pollution_code ELSE cabd.use_pollution_code END,
    use_recreation_code = CASE WHEN ({script.datasetName}.use_recreation_code IS NOT NULL AND {script.datasetName}.use_recreation_code IS DISTINCT FROM cabd.use_recreation_code) THEN {script.datasetName}.use_recreation_code ELSE cabd.use_recreation_code END,
    use_supply_code = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.use_supply_code ELSE cabd.use_supply_code END,
    waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    waterbody_name_fr = CASE WHEN ({script.datasetName}.waterbody_name_fr IS NOT NULL AND {script.datasetName}.waterbody_name_fr IS DISTINCT FROM cabd.waterbody_name_fr) THEN {script.datasetName}.waterbody_name_fr ELSE cabd.waterbody_name_fr END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)
