import nonspatial as main

script = main.MappingScript("nlp_depreciation_2019")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}', 
    '{script.datasetName}',
    '2019-12-31',
    'Newfoundland Power, 2019. 2019 Depreciation Study - Hydro Plant Decommissioning Report. pp. 4-83. Accessed from http://publicinfo.nlh.nl.ca/Newfoundland%20Power''s%202022%20CBA/RFI%20Responses/CA-NP-021.pdf',
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
   facility_name_en_ds = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_en_ds END,
   owner_ds = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.data_source ELSE cabdsource.owner_ds END,
   ownership_type_code_ds = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.ownership_type_code_ds END,
   construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
   turbine_type_code_ds = CASE WHEN ({script.datasetName}.turbine_type_code IS NOT NULL AND {script.datasetName}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.turbine_type_code_ds END,
   turbine_number_ds = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.data_source ELSE cabdsource.turbine_number_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
   owner_dsfid = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
   ownership_type_code_dsfid = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN NULL ELSE cabdsource.ownership_type_code_dsfid END,
   construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
   turbine_type_code_dsfid = CASE WHEN ({script.datasetName}.turbine_type_code IS NOT NULL AND {script.datasetName}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN NULL ELSE cabdsource.turbine_type_code_dsfid END,
   turbine_number_dsfid = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN NULL ELSE cabdsource.turbine_number_dsfid END
FROM
   {script.damTable} AS cabd,
   {script.sourceTable} AS {script.datasetName}
WHERE
   cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
   facility_name_en = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.facility_name_en ELSE cabd.facility_name_en END,
   "owner" = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.owner ELSE cabd.owner END,
   ownership_type_code = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.ownership_type_code ELSE cabd.ownership_type_code END,
   construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
   turbine_type_code = CASE WHEN ({script.datasetName}.turbine_type_code IS NOT NULL AND {script.datasetName}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN {script.datasetName}.turbine_type_code ELSE cabd.turbine_type_code END,
   turbine_number = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.turbine_number ELSE cabd.turbine_number END
FROM
   {script.sourceTable} AS {script.datasetName}
WHERE
   cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)