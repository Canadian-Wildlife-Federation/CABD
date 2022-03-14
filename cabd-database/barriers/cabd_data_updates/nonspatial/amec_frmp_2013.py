import nonspatial as main

script = main.MappingScript("amec_frmp_2013")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2013-02-27',
    'AMEC Environment & Infrastructure, 2013. Flood Risk Mapping Project - Corner Brook Stream and Petrie''s Brook. pp. 2-12 - 2-15. Accessed from https://www.gov.nl.ca/ecc/files/waterres-flooding-corner-brook-stream-cornerbrook-report.pdf',
    'Accessed February 22, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} SET data_source = '{script.dsUuid}';

--update pilot region features
UPDATE
   {script.liveDamAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    facility_name_en_ds = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_en_ds END,
    height_m_ds = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.data_source ELSE cabdsource.height_m_ds END,
    length_m_ds = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.data_source ELSE cabdsource.length_m_ds END,
    construction_year_ds = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.data_source ELSE cabdsource.construction_year_ds END,
    use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
    use_recreation_code_ds = CASE WHEN ({script.datasetName}.use_recreation_code IS NOT NULL AND {script.datasetName}.use_recreation_code IS DISTINCT FROM cabd.use_recreation_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_recreation_code_ds END,
    spillway_type_code_ds = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.spillway_type_code_ds END,
    function_code_ds = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
    reservoir_present_ds = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_present_ds END,
    reservoir_area_skm_ds = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
    storage_capacity_mcm_ds = CASE WHEN ({script.datasetName}.storage_capacity_mcm IS NOT NULL AND {script.datasetName}.storage_capacity_mcm IS DISTINCT FROM cabd.storage_capacity_mcm) THEN {script.datasetName}.data_source ELSE cabdsource.storage_capacity_mcm_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
    height_m_dsfid = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END,
    length_m_dsfid = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
    construction_year_dsfid = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
    use_code_dsfid = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
    use_recreation_code_dsfid = CASE WHEN ({script.datasetName}.use_recreation_code IS NOT NULL AND {script.datasetName}.use_recreation_code IS DISTINCT FROM cabd.use_recreation_code) THEN NULL ELSE cabdsource.use_recreation_code_dsfid END,
    spillway_type_code_dsfid = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN NULL ELSE cabdsource.spillway_type_code_dsfid END,
    function_code_dsfid = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END,
    reservoir_present_dsfid = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN NULL ELSE cabdsource.reservoir_present_dsfid END,
    reservoir_area_skm_dsfid = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN NULL ELSE cabdsource.reservoir_area_skm_dsfid END,
    storage_capacity_mcm_dsfid = CASE WHEN ({script.datasetName}.storage_capacity_mcm IS NOT NULL AND {script.datasetName}.storage_capacity_mcm IS DISTINCT FROM cabd.storage_capacity_mcm) THEN NULL ELSE cabdsource.storage_capacity_mcm_dsfid END
FROM
    {script.liveDamTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.existing_pilot_region_pt IS TRUE
    AND (cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);

UPDATE
    {script.liveDamTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    facility_name_en = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.facility_name_en ELSE cabd.facility_name_en END,
    height_m = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.height_m ELSE cabd.height_m END,
    length_m = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.length_m ELSE cabd.length_m END,
    construction_year = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.construction_year ELSE cabd.construction_year END,
    use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
    use_recreation_code = CASE WHEN ({script.datasetName}.use_recreation_code IS NOT NULL AND {script.datasetName}.use_recreation_code IS DISTINCT FROM cabd.use_recreation_code) THEN {script.datasetName}.use_recreation_code ELSE cabd.use_recreation_code END,
    spillway_type_code = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.spillway_type_code ELSE cabd.spillway_type_code END,
    function_code = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.function_code ELSE cabd.function_code END,
    reservoir_present = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.reservoir_present ELSE cabd.reservoir_present END,
    reservoir_area_skm = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetName}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    storage_capacity_mcm = CASE WHEN ({script.datasetName}.storage_capacity_mcm IS NOT NULL AND {script.datasetName}.storage_capacity_mcm IS DISTINCT FROM cabd.storage_capacity_mcm) THEN {script.datasetName}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.existing_pilot_region_pt IS TRUE
    AND cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)