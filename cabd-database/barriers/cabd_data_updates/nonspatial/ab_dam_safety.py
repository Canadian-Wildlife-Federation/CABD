import nonspatial as main

script = main.MappingScript("ab_dam_safety_map")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Alberta Dam Safety Map', now(), null, 'Alberta Environment and Parks', 'Data update - ' || now());

--add data source to the table
UPDATE TABLE {script.workingTable} ADD COLUMN data_source varchar(512);
UPDATE TABLE {script.workingTable} SET data_source = {script.dsUuid};

--update existing features 
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,   
   waterbody_name_en_ds = CASE WHEN {script.datasetname}.waterbody_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_en_ds END,   
   owner_ds = CASE WHEN {script.datasetname}.owner IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,   
   ownership_type_code_ds = CASE WHEN {script.datasetname}.ownership_type_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,   
   provincial_compliance_status_ds = CASE WHEN {script.datasetname}.provincial_compliance_status IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.provincial_compliance_status_ds END,   
   reservoir_present_ds = CASE WHEN {script.datasetname}.reservoir_present IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_present_ds END,   
   reservoir_name_en_ds = CASE WHEN {script.datasetname}.reservoir_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_name_en_ds END,   
   height_m_ds = CASE WHEN {script.datasetname}.height_m IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,   
   storage_capacity_mcm_ds = CASE WHEN {script.datasetname}.storage_capacity_mcm IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.storage_capacity_mcm_ds END,   
   use_code_ds = CASE WHEN {script.datasetname}.use_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,   
   use_irrigation_code_ds = CASE WHEN {script.datasetname}.use_irrigation_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_irrigation_code_ds END,   
   use_electricity_code_ds = CASE WHEN {script.datasetname}.use_electricity_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,   
   use_supply_code_ds = CASE WHEN {script.datasetname}.use_supply_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_supply_code_ds END,   
   use_floodcontrol_code_ds = CASE WHEN {script.datasetname}.use_floodcontrol_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_floodcontrol_code_ds END,   
   use_recreation_code_ds = CASE WHEN {script.datasetname}.use_recreation_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_recreation_code_ds END,   
   use_fish_code_ds = CASE WHEN {script.datasetname}.use_fish_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_fish_code_ds END,   
   use_other_code_ds = CASE WHEN {script.datasetname}.use_other_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_other_code_ds END,   
   lake_control_code_ds = CASE WHEN {script.datasetname}.lake_control_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.lake_control_code_ds END,   
   function_code_ds = CASE WHEN {script.datasetname}.function_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END,   
   construction_type_code_ds = CASE WHEN {script.datasetname}.construction_type_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END
FROM
   {script.damTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   waterbody_name_en = CASE WHEN {script.datasetname}.waterbody_name_en IS NOT NULL THEN {script.datasetname}.waterbody_name_en ELSE cabd.waterbody_name_en END,
   "owner" = CASE WHEN {script.datasetname}."owner" IS NOT NULL THEN {script.datasetname}."owner" ELSE cabd."owner" END,
   ownership_type_code = CASE WHEN {script.datasetname}.ownership_type_code IS NOT NULL THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END,
   provincial_compliance_status = CASE WHEN {script.datasetname}.provincial_compliance_status IS NOT NULL THEN {script.datasetname}.provincial_compliance_status ELSE cabd.provincial_compliance_status END,
   reservoir_present = CASE WHEN {script.datasetname}.reservoir_present IS NOT NULL THEN {script.datasetname}.reservoir_present ELSE cabd.reservoir_present END,
   reservoir_name_en = CASE WHEN {script.datasetname}.reservoir_name_en IS NOT NULL THEN {script.datasetname}.reservoir_name_en ELSE cabd.reservoir_name_en END,
   height_m = CASE WHEN {script.datasetname}.height_m IS NOT NULL THEN {script.datasetname}.height_m ELSE cabd.height_m END,
   storage_capacity_mcm = CASE WHEN {script.datasetname}.storage_capacity_mcm IS NOT NULL THEN {script.datasetname}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,
   use_code = CASE WHEN {script.datasetname}.use_code IS NOT NULL THEN {script.datasetname}.use_code ELSE cabd.use_code END,
   use_irrigation_code = CASE WHEN {script.datasetname}.use_irrigation_code IS NOT NULL THEN {script.datasetname}.use_irrigation_code ELSE cabd.use_irrigation_code END,
   use_electricity_code = CASE WHEN {script.datasetname}.use_electricity_code IS NOT NULL THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
   use_supply_code = CASE WHEN {script.datasetname}.use_supply_code IS NOT NULL THEN {script.datasetname}.use_supply_code ELSE cabd.use_supply_code END,
   use_floodcontrol_code = CASE WHEN {script.datasetname}.use_floodcontrol_code IS NOT NULL THEN {script.datasetname}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
   use_recreation_code = CASE WHEN {script.datasetname}.use_recreation_code IS NOT NULL THEN {script.datasetname}.use_recreation_code ELSE cabd.use_recreation_code END,
   use_fish_code = CASE WHEN {script.datasetname}.use_fish_code IS NOT NULL THEN {script.datasetname}.use_fish_code ELSE cabd.use_fish_code END,
   use_other_code = CASE WHEN {script.datasetname}.use_other_code IS NOT NULL THEN {script.datasetname}.use_other_code ELSE cabd.use_other_code END,
   lake_control_code = CASE WHEN {script.datasetname}.lake_control_code IS NOT NULL THEN {script.datasetname}.lake_control_code ELSE cabd.lake_control_code END,
   function_code = CASE WHEN {script.datasetname}.function_code IS NOT NULL THEN {script.datasetname}.function_code ELSE cabd.function_code END,
   construction_type_code = CASE WHEN {script.datasetname}.construction_type_code IS NOT NULL THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)