import nonspatial as main

script = main.MappingScript("nlp_fac_rehab_2014")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}', 
    '{script.datasetname}',
    '2013-06-01',
    'Newfoundland Power, 2013. 2014 Facility Rehabilitation. pp. 3, 5-6. Accessed from http://www.pub.nf.ca/applications/IslandInterconnectedSystem/files/rfi/PUB-NP-175.pdf',
    'Accessed February 22, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.workingTable} ADD COLUMN data_source uuid;
UPDATE {script.workingTable} SET data_source = '{script.dsUuid}';

--update new features
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
   construction_type_code_ds = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
   construction_year_ds = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
   maintenance_last_ds = CASE WHEN ({script.datasetname}.maintenance_last IS NOT NULL AND {script.datasetname}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetname}.data_source ELSE cabdsource.maintenance_last_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   construction_type_code_dsfid = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
   construction_year_dsfid = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
   maintenance_last_dsfid = CASE WHEN ({script.datasetname}.maintenance_last IS NOT NULL AND {script.datasetname}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN NULL ELSE cabdsource.maintenance_last_dsfid END
FROM
   {script.damTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   cabdsource.cabd_id = {script.datasetname}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   construction_type_code = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
   construction_year = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
   maintenance_last = CASE WHEN ({script.datasetname}.maintenance_last IS NOT NULL AND {script.datasetname}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetname}.maintenance_last ELSE cabd.maintenance_last END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)