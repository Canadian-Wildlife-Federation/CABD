import nonspatial as main

script = main.MappingScript("nlp_rbfc_2011")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}', 
    '{script.datasetName}',
    '2011-06-01',
    'Newfoundland Power, 2011. Rattling Brook Fisheries Compensation. pp. 3-4. Accessed from http://www.pub.nl.ca/applications/NP2012Capital/files/applic/NP2012Application-Generation.pdf',
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
   down_passage_route_code_ds = CASE WHEN ({script.datasetName}.down_passage_route_code IS NOT NULL AND {script.datasetName}.down_passage_route_code IS DISTINCT FROM cabd.down_passage_route_code) THEN {script.datasetName}.data_source ELSE cabdsource.down_passage_route_code_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   down_passage_route_code_dsfid = CASE WHEN ({script.datasetName}.down_passage_route_code IS NOT NULL AND {script.datasetName}.down_passage_route_code IS DISTINCT FROM cabd.down_passage_route_code) THEN NULL ELSE cabdsource.down_passage_route_code_dsfid END
FROM
   {script.damTable} AS cabd,
   {script.sourceTable} AS {script.datasetName}
WHERE
   {script.datasetName}.fishway_yn IS FALSE
   AND (cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
   down_passage_route_code = CASE WHEN ({script.datasetName}.down_passage_route_code IS NOT NULL AND {script.datasetName}.down_passage_route_code IS DISTINCT FROM cabd.down_passage_route_code) THEN {script.datasetName}.down_passage_route_code ELSE cabd.down_passage_route_code END
FROM
   {script.sourceTable} AS {script.datasetName}
WHERE
   {script.datasetName}.fishway_yn IS FALSE
   AND cabd.cabd_id = {script.datasetName}.cabd_id;


--update fishway features
UPDATE
   {script.fishAttributeTable} AS cabdsource
SET    
   structure_name_en_ds = CASE WHEN ({script.datasetName}.structure_name_en IS NOT NULL AND {script.datasetName}.structure_name_en IS DISTINCT FROM cabd.structure_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.structure_name_en_ds END,
   fishpass_type_code_ds = CASE WHEN ({script.datasetName}.fishpass_type_code IS NOT NULL AND {script.datasetName}.fishpass_type_code IS DISTINCT FROM cabd.fishpass_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.fishpass_type_code_ds END,
   year_constructed_ds = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN {script.datasetName}.data_source ELSE cabdsource.year_constructed_ds END,
   purpose_ds = CASE WHEN ({script.datasetName}.purpose IS NOT NULL AND {script.datasetName}.purpose IS DISTINCT FROM cabd.purpose) THEN {script.datasetName}.data_source ELSE cabdsource.purpose_ds END,
   
   structure_name_en_dsfid = CASE WHEN ({script.datasetName}.structure_name_en IS NOT NULL AND {script.datasetName}.structure_name_en IS DISTINCT FROM cabd.structure_name_en) THEN NULL ELSE cabdsource.structure_name_en_dsfid END,
   fishpass_type_code_dsfid = CASE WHEN ({script.datasetName}.fishpass_type_code IS NOT NULL AND {script.datasetName}.fishpass_type_code IS DISTINCT FROM cabd.fishpass_type_code) THEN NULL ELSE cabdsource.fishpass_type_code_dsfid END,
   year_constructed_dsfid = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN NULL ELSE cabdsource.year_constructed_dsfid END,
   purpose_dsfid = CASE WHEN ({script.datasetName}.purpose IS NOT NULL AND {script.datasetName}.purpose IS DISTINCT FROM cabd.purpose) THEN NULL ELSE cabdsource.purpose_dsfid END
FROM
   {script.fishTable} AS cabd,
   {script.sourceTable} AS {script.datasetName}
WHERE
   {script.datasetName}.fishway_yn IS TRUE
   AND (cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);

UPDATE
   {script.fishTable} AS cabd
SET
   structure_name_en = CASE WHEN ({script.datasetName}.structure_name_en IS NOT NULL AND {script.datasetName}.structure_name_en IS DISTINCT FROM cabd.structure_name_en) THEN {script.datasetName}.structure_name_en ELSE cabd.structure_name_en END,
   fishpass_type_code = CASE WHEN ({script.datasetName}.fishpass_type_code IS NOT NULL AND {script.datasetName}.fishpass_type_code IS DISTINCT FROM cabd.fishpass_type_code) THEN {script.datasetName}.fishpass_type_code ELSE cabd.fishpass_type_code END,
   year_constructed = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN {script.datasetName}.year_constructed ELSE cabd.year_constructed END,
   purpose = CASE WHEN ({script.datasetName}.purpose IS NOT NULL AND {script.datasetName}.purpose IS DISTINCT FROM cabd.purpose) THEN {script.datasetName}.purpose ELSE cabd.purpose END
FROM
   {script.sourceTable} AS {script.datasetName}
WHERE
   {script.datasetName}.fishway_yn IS TRUE
   AND cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)