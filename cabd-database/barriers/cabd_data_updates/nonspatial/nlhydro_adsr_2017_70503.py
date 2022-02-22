import nonspatial as main

script = main.MappingScript("nlhydro_adsr_2017_70503")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}', 
    '{script.datasetname}',
    '2016-02-01',
    'Newfoundland and Labrador Hydro, 2016. RFP # 2017-70503 - Audit Dam Safety Review for Pudops Dam, North Salmon Dam and Spillway; Appendix VI: Inventory of Dams and Dykes. Accessed from https://bids.nalcorenergy.com/Module/Tenders/en/Document/Preview/c33bf56c-20b9-453d-914a-4d779ce9ed0e',
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
   use_code_ds = CASE WHEN ({script.datasetname}.use_code IS NOT NULL AND {script.datasetname}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,
   use_electricity_code_ds = CASE WHEN ({script.datasetname}.use_electricity_code IS NOT NULL AND {script.datasetname}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,
   construction_type_code_ds = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
   construction_year_ds = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
   height_m_ds = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
   length_m_ds = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,
   spillway_type_code_ds = CASE WHEN ({script.datasetname}.spillway_type_code IS NOT NULL AND {script.datasetname}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.spillway_type_code_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   use_code_dsfid = CASE WHEN ({script.datasetname}.use_code IS NOT NULL AND {script.datasetname}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
   use_electricity_code_dsfid = CASE WHEN ({script.datasetname}.use_electricity_code IS NOT NULL AND {script.datasetname}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN NULL ELSE cabdsource.use_electricity_code_dsfid END,
   construction_type_code_dsfid = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
   construction_year_dsfid = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
   height_m_dsfid = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END,
   length_m_dsfid = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
   spillway_type_code_dsfid = CASE WHEN ({script.datasetname}.spillway_type_code IS NOT NULL AND {script.datasetname}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN NULL ELSE cabdsource.spillway_type_code_dsfid END
FROM
   {script.damTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   cabdsource.cabd_id = {script.datasetname}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   use_code = CASE WHEN ({script.datasetname}.use_code IS NOT NULL AND {script.datasetname}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetname}.use_code ELSE cabd.use_code END,
   use_electricity_code = CASE WHEN ({script.datasetname}.use_electricity_code IS NOT NULL AND {script.datasetname}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
   construction_type_code = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
   construction_year = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
   height_m = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetname}.height_m ELSE cabd.height_m END,
   length_m = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetname}.length_m ELSE cabd.length_m END,
   spillway_type_code = CASE WHEN ({script.datasetname}.spillway_type_code IS NOT NULL AND {script.datasetname}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetname}.spillway_type_code ELSE cabd.spillway_type_code END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)