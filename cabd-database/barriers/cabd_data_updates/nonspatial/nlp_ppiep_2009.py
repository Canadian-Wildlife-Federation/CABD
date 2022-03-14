import nonspatial as main

script = main.MappingScript("nlp_ppiep_2009")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2009-01-01',
    'Newfoundland Power, 2009. Potential Projects to Increase Energy Production. pp. 2-25. Accessed from http://pub.nl.ca/applications/NP2010Capital/files/rfi/PUB-NP-009.pdf',
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
   height_m_ds = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.data_source ELSE cabdsource.height_m_ds END,
   length_m_ds = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.data_source ELSE cabdsource.length_m_ds END,
   construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
   use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
   use_electricity_code_ds = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_electricity_code_ds END,
   spillway_type_code_ds = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.spillway_type_code_ds END,
   generating_capacity_mwh_ds = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
   function_code_ds = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
   operating_status_code_ds = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.operating_status_code_ds END,
   removed_year_ds = CASE WHEN ({script.datasetName}.removed_year IS NOT NULL AND {script.datasetName}.removed_year IS DISTINCT FROM cabd.removed_year) THEN {script.datasetName}.data_source ELSE cabdsource.removed_year_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
   owner_dsfid = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
   ownership_type_code_dsfid = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN NULL ELSE cabdsource.ownership_type_code_dsfid END,
   height_m_dsfid = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END,
   length_m_dsfid = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
   construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
   use_code_dsfid = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
   use_electricity_code_dsfid = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN NULL ELSE cabdsource.use_electricity_code_dsfid END,
   spillway_type_code_dsfid = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN NULL ELSE cabdsource.spillway_type_code_dsfid END,
   generating_capacity_mwh_dsfid = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN NULL ELSE cabdsource.generating_capacity_mwh_dsfid END,
   function_code_dsfid = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END,
   operating_status_code_dsfid = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN NULL ELSE cabdsource.operating_status_code_dsfid END,
   removed_year_dsfid = CASE WHEN ({script.datasetName}.removed_year IS NOT NULL AND {script.datasetName}.removed_year IS DISTINCT FROM cabd.removed_year) THEN NULL ELSE cabdsource.removed_year_dsfid END
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
   height_m = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.height_m ELSE cabd.height_m END,
   length_m = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.length_m ELSE cabd.length_m END,
   construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
   use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
   use_electricity_code = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetName}.use_electricity_code ELSE cabd.use_electricity_code END,
   spillway_type_code = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.spillway_type_code ELSE cabd.spillway_type_code END,
   generating_capacity_mwh = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
   function_code = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.function_code ELSE cabd.function_code END,
   operating_status_code = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.operating_status_code ELSE cabd.operating_status_code END,
   removed_year = CASE WHEN ({script.datasetName}.removed_year IS NOT NULL AND {script.datasetName}.removed_year IS DISTINCT FROM cabd.removed_year) THEN {script.datasetName}.removed_year ELSE cabd.removed_year END
FROM
   {script.sourceTable} AS {script.datasetName}
WHERE
   cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)