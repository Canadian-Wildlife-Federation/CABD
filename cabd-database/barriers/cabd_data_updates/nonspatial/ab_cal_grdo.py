import nonspatial as main

script = main.MappingScript("glenmore_operations")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Glenmore Reservoir Dam Operations', now(), null, 'City of Calgary', 'Data update - ' || now());

--add data source to the table
ALTER TABLE {script.workingTable} ADD COLUMN data_source uuid;
UPDATE {script.workingTable} SET data_source = '{script.dsUuid}';

--update existing features 
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
   construction_type_code_ds = CASE WHEN {script.datasetname}.construction_type_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
   use_recreation_code_ds = CASE WHEN {script.datasetname}.use_recreation_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_recreation_code_ds END,
   use_floodcontrol_code_ds = CASE WHEN {script.datasetname}.use_floodcontrol_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_floodcontrol_code_ds END
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
   use_recreation_code = CASE WHEN {script.datasetname}.use_recreation_code IS NOT NULL THEN {script.datasetname}.use_recreation_code ELSE cabd.use_recreation_code END,
   use_floodcontrol_code = CASE WHEN {script.datasetname}.use_floodcontrol_code IS NOT NULL THEN {script.datasetname}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
