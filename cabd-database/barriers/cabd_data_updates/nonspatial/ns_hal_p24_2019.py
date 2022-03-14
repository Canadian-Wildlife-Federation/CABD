import nonspatial as main

script = main.MappingScript("ns_hal_p24_2019")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2019-08-14',
    'Halifax Water, 2019. RFP #P24.2019 - Halifax Water Dam Safety Review (2019); Appendix E. Accessed from https://procurement.novascotia.ca/pt_files/tenders/P242019.pdf',
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
   function_code_ds = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
   use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
   use_supply_code_ds = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_supply_code_ds END,
   use_pollution_code_ds = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_pollution_code_ds END,
   construction_year_ds = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.data_source ELSE cabdsource.construction_year_ds END,
   spillway_type_code_ds = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.spillway_type_code_ds END,
   construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
   length_m_ds = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.data_source ELSE cabdsource.length_m_ds END,
   height_m_ds = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.data_source ELSE cabdsource.height_m_ds END,
   waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
   reservoir_present_ds = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_present_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
   function_code_dsfid = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END,
   use_code_dsfid = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
   use_supply_code_dsfid = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN NULL ELSE cabdsource.use_supply_code_dsfid END,
   use_pollution_code_dsfid = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN NULL ELSE cabdsource.use_pollution_code_dsfid END,
   construction_year_dsfid = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
   spillway_type_code_dsfid = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN NULL ELSE cabdsource.spillway_type_code_dsfid END,
   construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
   length_m_dsfid = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
   height_m_dsfid = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END,
   waterbody_name_en_dsfid = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN NULL ELSE cabdsource.waterbody_name_en_dsfid END,
   reservoir_present_dsfid = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN NULL ELSE cabdsource.reservoir_present_dsfid END
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
   function_code = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.function_code ELSE cabd.function_code END,
   use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
   use_supply_code = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.use_supply_code ELSE cabd.use_supply_code END,
   use_pollution_code = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.use_pollution_code ELSE cabd.use_pollution_code END,
   construction_year = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.construction_year ELSE cabd.construction_year END,
   spillway_type_code = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.spillway_type_code ELSE cabd.spillway_type_code END,
   construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
   length_m = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.length_m ELSE cabd.length_m END,
   height_m = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.height_m ELSE cabd.height_m END,
   waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,
   reservoir_present = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.reservoir_present ELSE cabd.reservoir_present END
FROM
   {script.sourceTable} AS {script.datasetName}
WHERE
   cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)