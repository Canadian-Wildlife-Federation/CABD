import nonspatial as main

script = main.MappingScript("ab_irrigation")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Alberta Irrigation Information 2017', now(), null, 'Ministry of Agriculture, Forestry and Rural Economic Development', 'Data update - ' || now());

--add data source to the table
ALTER TABLE {script.workingTable} ADD COLUMN data_source uuid;
UPDATE {script.workingTable} SET data_source = '{script.dsUuid}';

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,   
    waterbody_name_en_ds = CASE WHEN {script.datasetname}.waterbody_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_en_ds END,   
    owner_ds = CASE WHEN {script.datasetname}.owner IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,   
    ownership_type_code_ds = CASE WHEN {script.datasetname}.ownership_type_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,   
    reservoir_present_ds = CASE WHEN {script.datasetname}.reservoir_present IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_present_ds END,   
    reservoir_name_en_ds = CASE WHEN {script.datasetname}.reservoir_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_name_en_ds END,   
    use_irrigation_code_ds = CASE WHEN {script.datasetname}.use_irrigation_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_irrigation_code_ds END,   
    use_electricity_code_ds = CASE WHEN {script.datasetname}.use_electricity_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,   
    generating_capacity_mwh_ds = CASE WHEN {script.datasetname}.generating_capacity_mwh IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,   
    construction_year_ds = CASE WHEN {script.datasetname}.construction_year IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,   
    storage_capacity_mcm_ds = CASE WHEN {script.datasetname}.storage_capacity_mcm IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.storage_capacity_mcm_ds END   
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
    reservoir_present = CASE WHEN {script.datasetname}.reservoir_present IS NOT NULL THEN {script.datasetname}.reservoir_present ELSE cabd.reservoir_present END,
    reservoir_name_en = CASE WHEN {script.datasetname}.reservoir_name_en IS NOT NULL THEN {script.datasetname}.reservoir_name_en ELSE cabd.reservoir_name_en END,
    use_irrigation_code = CASE WHEN {script.datasetname}.use_irrigation_code IS NOT NULL THEN {script.datasetname}.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_electricity_code = CASE WHEN {script.datasetname}.use_electricity_code IS NOT NULL THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
    generating_capacity_mwh = CASE WHEN {script.datasetname}.generating_capacity_mwh IS NOT NULL THEN {script.datasetname}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,   
    construction_year = CASE WHEN {script.datasetname}.construction_year IS NOT NULL THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
    storage_capacity_mcm = CASE WHEN {script.datasetname}.storage_capacity_mcm IS NOT NULL THEN {script.datasetname}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)