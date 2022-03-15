import nonspatial as main

script = main.MappingScript("dawson_wq_assurance")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'City of Dawson Creek Water Quality Assurance Plan Update',
now(), null, 'City of Dawson Creek', 'Data update - ' || now());

--add data source to the table
ALTER TABLE {script.workingTable} ADD COLUMN data_source uuid;
UPDATE {script.workingTable} SET data_source = '{script.dsUuid}';

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,   
    function_code_ds = CASE WHEN {script.datasetname}.function_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END,   
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
    function_code = CASE WHEN {script.datasetname}.function_code IS NOT NULL THEN {script.datasetname}.function_code ELSE cabd.function_code END,
    storage_capacity_mcm = CASE WHEN {script.datasetname}.storage_capacity_mcm IS NOT NULL THEN {script.datasetname}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
