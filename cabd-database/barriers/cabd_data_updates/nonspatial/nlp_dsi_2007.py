import nonspatial as main

script = main.MappingScript("nlp_dsi_2007")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2007-01-01',
    'Newfoundland Power, 2007. 2007 Dam Safety Inspection Report - Tors Cove / Rocky Pond Developments. Accessed from http://www.pub.nf.ca/applications/NP2009Capital/files/rfi/CA-NP-10.pdf',
    'Accessed February 22, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} SET data_source = '{script.dsUuid}';

--update new features
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
   construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
   function_code_ds = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
   function_code_dsfid = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END
FROM
   {script.damTable} AS cabd,
   {script.sourceTable} AS {script.datasetName}
WHERE
   cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
   construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
   function_code = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.function_code ELSE cabd.function_code END
FROM
   {script.sourceTable} AS {script.datasetName}
WHERE
   cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)