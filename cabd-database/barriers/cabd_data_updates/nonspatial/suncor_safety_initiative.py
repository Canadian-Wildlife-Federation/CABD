import nonspatial as main

script = main.MappingScript("suncor_safety_initiative")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Investor Mining and Tailings Safety Initiative', now(), null, 'Suncor Energy', 'Data update - ' || now());

--add data source to the table
ALTER TABLE {script.workingTable} ADD COLUMN data_source uuid;
UPDATE {script.workingTable} SET data_source = '{script.dsUuid}';

--update existing features 
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,   
   operating_status_code_ds = CASE WHEN {script.datasetname}.operating_status_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.operating_status_code_ds END
FROM
   {script.damTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   operating_status_code = CASE WHEN {script.datasetname}.operating_status_code IS NOT NULL THEN {script.datasetname}.operating_status_code ELSE cabd.operating_status_code END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)