import nonspatial as main

script = main.MappingScript("dfo_bulltrout")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Information in support of a recovery potential assessment of Bull Trout
(Salvelinus confluentus) (Saskatchewan and Nelson rivers populations) in Alberta',
now(), null, 'Fisheries and Oceans Canada', 'Data update - ' || now());

--add data source to the table
UPDATE TABLE {script.workingTable} ADD COLUMN data_source varchar(512);
UPDATE TABLE {script.workingTable} SET data_source = {script.dsUuid};

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
    construction_type_code_ds = CASE WHEN {script.datasetname}.construction_type_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
    function_code_ds = CASE WHEN {script.datasetname}.function_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END,
    height_m_ds = CASE WHEN {script.datasetname}.height_m IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
    storage_capacity_mcm_ds = CASE WHEN {script.datasetname}.storage_capacity_mcm IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.storage_capacity_mcm_ds END,
    use_code_ds = CASE WHEN {script.datasetname}.use_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,
    use_irrigation_code_ds = CASE WHEN {script.datasetname}.use_irrigation_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_irrigation_code_ds END,
    use_supply_code_ds = CASE WHEN {script.datasetname}.use_supply_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_supply_code_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    construction_type_code = CASE WHEN {script.datasetname}.construction_type_code IS NOT NULL THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
    function_code = CASE WHEN {script.datasetname}.function_code IS NOT NULL THEN {script.datasetname}.function_code ELSE cabd.function_code END,
    height_m = CASE WHEN {script.datasetname}.height_m IS NOT NULL THEN {script.datasetname}.height_m ELSE cabd.height_m END,
    storage_capacity_mcm = CASE WHEN {script.datasetname}.storage_capacity_mcm IS NOT NULL THEN {script.datasetname}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,
    use_code = CASE WHEN {script.datasetname}.use_code IS NOT NULL THEN {script.datasetname}.use_code ELSE cabd.use_code END,
    use_irrigation_code = CASE WHEN {script.datasetname}.use_irrigation_code IS NOT NULL THEN {script.datasetname}.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_supply_code = CASE WHEN {script.datasetname}.use_supply_code IS NOT NULL THEN {script.datasetname}.use_supply_code ELSE cabd.use_supply_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)