import nonspatial as main

script = main.MappingScript("nsp_ihas")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}', 
    '{script.datasetname}',
    '2018-12-01',
    'Nova Scotia Power Inc., 2018. Nova Scotia Power Incorporated Hydro Asset Study - Redacted; Appendix B. pp. 16-68. Accessed from https://irp.nspower.ca/files/key-documents/background-materials/20181221-NS-Power-Hydro-Asset-Study-REDACTED.pdf',
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
   facility_name_en_ds = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetname}.data_source ELSE cabdsource.facility_name_en_ds END,
   owner_ds = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,
   ownership_type_code_ds = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,
   height_m_ds = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
   length_m_ds = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,
   reservoir_area_skm_ds = CASE WHEN ({script.datasetname}.reservoir_area_skm IS NOT NULL AND {script.datasetname}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
   generating_capacity_mwh_ds = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetname}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
   use_code_ds = CASE WHEN ({script.datasetname}.use_code IS NOT NULL AND {script.datasetname}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,
   use_electricity_code_ds = CASE WHEN ({script.datasetname}.use_electricity_code IS NOT NULL AND {script.datasetname}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,
   function_code_ds = CASE WHEN ({script.datasetname}.function_code IS NOT NULL AND {script.datasetname}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END,
   operating_status_code_ds = CASE WHEN ({script.datasetname}.operating_status_code IS NOT NULL AND {script.datasetname}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetname}.data_source ELSE cabdsource.operating_status_code_ds END,
   construction_year_ds = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
   construction_type_code_ds = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
   reservoir_name_en_ds = CASE WHEN ({script.datasetname}.reservoir_name_en IS NOT NULL AND {script.datasetname}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_name_en_ds END,

   dam_name_en_dsfid = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   facility_name_en_dsfid = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
   owner_dsfid = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
   ownership_type_code_dsfid = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN NULL ELSE cabdsource.ownership_type_code_dsfid END,
   height_m_dsfid = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END,
   length_m_dsfid = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
   reservoir_area_skm_dsfid = CASE WHEN ({script.datasetname}.reservoir_area_skm IS NOT NULL AND {script.datasetname}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN NULL ELSE cabdsource.reservoir_area_skm_dsfid END,
   generating_capacity_mwh_dsfid = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN NULL ELSE cabdsource.generating_capacity_mwh_dsfid END,
   use_code_dsfid = CASE WHEN ({script.datasetname}.use_code IS NOT NULL AND {script.datasetname}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
   use_electricity_code_dsfid = CASE WHEN ({script.datasetname}.use_electricity_code IS NOT NULL AND {script.datasetname}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN NULL ELSE cabdsource.use_electricity_code_dsfid END,
   function_code_dsfid = CASE WHEN ({script.datasetname}.function_code IS NOT NULL AND {script.datasetname}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END,
   operating_status_code_dsfid = CASE WHEN ({script.datasetname}.operating_status_code IS NOT NULL AND {script.datasetname}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN NULL ELSE cabdsource.operating_status_code_dsfid END,
   construction_year_dsfid = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
   construction_type_code_dsfid = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
   reservoir_name_en_dsfid = CASE WHEN ({script.datasetname}.reservoir_name_en IS NOT NULL AND {script.datasetname}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN NULL ELSE cabdsource.reservoir_name_en_dsfid END
FROM
   {script.damTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   cabdsource.cabd_id = {script.datasetname}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   facility_name_en = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetname}.facility_name_en ELSE cabd.facility_name_en END,
   "owner" = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetname}.owner ELSE cabd.owner END,
   ownership_type_code = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END,
   height_m = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetname}.height_m ELSE cabd.height_m END,
   length_m = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetname}.length_m ELSE cabd.length_m END,
   reservoir_area_skm = CASE WHEN ({script.datasetname}.reservoir_area_skm IS NOT NULL AND {script.datasetname}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetname}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
   generating_capacity_mwh = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetname}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
   use_code = CASE WHEN ({script.datasetname}.use_code IS NOT NULL AND {script.datasetname}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetname}.use_code ELSE cabd.use_code END,
   use_electricity_code = CASE WHEN ({script.datasetname}.use_electricity_code IS NOT NULL AND {script.datasetname}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
   function_code = CASE WHEN ({script.datasetname}.function_code IS NOT NULL AND {script.datasetname}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetname}.function_code ELSE cabd.function_code END,
   operating_status_code = CASE WHEN ({script.datasetname}.operating_status_code IS NOT NULL AND {script.datasetname}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetname}.operating_status_code ELSE cabd.operating_status_code END,
   construction_year = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
   construction_type_code = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
   reservoir_name_en = CASE WHEN ({script.datasetname}.reservoir_name_en IS NOT NULL AND {script.datasetname}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN {script.datasetname}.reservoir_name_en ELSE cabd.reservoir_name_en END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;


--update pilot region features
UPDATE
   {script.liveDamAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
   facility_name_en_ds = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetname}.data_source ELSE cabdsource.facility_name_en_ds END,
   owner_ds = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,
   ownership_type_code_ds = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,
   height_m_ds = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
   length_m_ds = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,
   reservoir_area_skm_ds = CASE WHEN ({script.datasetname}.reservoir_area_skm IS NOT NULL AND {script.datasetname}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
   generating_capacity_mwh_ds = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetname}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
   use_code_ds = CASE WHEN ({script.datasetname}.use_code IS NOT NULL AND {script.datasetname}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,
   use_electricity_code_ds = CASE WHEN ({script.datasetname}.use_electricity_code IS NOT NULL AND {script.datasetname}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,
   function_code_ds = CASE WHEN ({script.datasetname}.function_code IS NOT NULL AND {script.datasetname}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END,
   operating_status_code_ds = CASE WHEN ({script.datasetname}.operating_status_code IS NOT NULL AND {script.datasetname}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetname}.data_source ELSE cabdsource.operating_status_code_ds END,
   construction_year_ds = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
   construction_type_code_ds = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
   reservoir_name_en_ds = CASE WHEN ({script.datasetname}.reservoir_name_en IS NOT NULL AND {script.datasetname}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_name_en_ds END,

   dam_name_en_dsfid = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   facility_name_en_dsfid = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
   owner_dsfid = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
   ownership_type_code_dsfid = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN NULL ELSE cabdsource.ownership_type_code_dsfid END,
   height_m_dsfid = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END,
   length_m_dsfid = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
   reservoir_area_skm_dsfid = CASE WHEN ({script.datasetname}.reservoir_area_skm IS NOT NULL AND {script.datasetname}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN NULL ELSE cabdsource.reservoir_area_skm_dsfid END,
   generating_capacity_mwh_dsfid = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN NULL ELSE cabdsource.generating_capacity_mwh_dsfid END,
   use_code_dsfid = CASE WHEN ({script.datasetname}.use_code IS NOT NULL AND {script.datasetname}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
   use_electricity_code_dsfid = CASE WHEN ({script.datasetname}.use_electricity_code IS NOT NULL AND {script.datasetname}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN NULL ELSE cabdsource.use_electricity_code_dsfid END,
   function_code_dsfid = CASE WHEN ({script.datasetname}.function_code IS NOT NULL AND {script.datasetname}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END,
   operating_status_code_dsfid = CASE WHEN ({script.datasetname}.operating_status_code IS NOT NULL AND {script.datasetname}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN NULL ELSE cabdsource.operating_status_code_dsfid END,
   construction_year_dsfid = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
   construction_type_code_dsfid = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
   reservoir_name_en_dsfid = CASE WHEN ({script.datasetname}.reservoir_name_en IS NOT NULL AND {script.datasetname}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN NULL ELSE cabdsource.reservoir_name_en_dsfid END
FROM
   {script.liveDamTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   {script.datasetname}.existing_pilot_region_pt IS TRUE
   AND (cabdsource.cabd_id = {script.datasetname}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);

UPDATE
   {script.liveDamTable} AS cabd
SET
   dam_name_en = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   facility_name_en = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetname}.facility_name_en ELSE cabd.facility_name_en END,
   "owner" = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetname}.owner ELSE cabd.owner END,
   ownership_type_code = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END,
   height_m = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetname}.height_m ELSE cabd.height_m END,
   length_m = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetname}.length_m ELSE cabd.length_m END,
   reservoir_area_skm = CASE WHEN ({script.datasetname}.reservoir_area_skm IS NOT NULL AND {script.datasetname}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetname}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
   generating_capacity_mwh = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetname}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
   use_code = CASE WHEN ({script.datasetname}.use_code IS NOT NULL AND {script.datasetname}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetname}.use_code ELSE cabd.use_code END,
   use_electricity_code = CASE WHEN ({script.datasetname}.use_electricity_code IS NOT NULL AND {script.datasetname}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
   function_code = CASE WHEN ({script.datasetname}.function_code IS NOT NULL AND {script.datasetname}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetname}.function_code ELSE cabd.function_code END,
   operating_status_code = CASE WHEN ({script.datasetname}.operating_status_code IS NOT NULL AND {script.datasetname}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetname}.operating_status_code ELSE cabd.operating_status_code END,
   construction_year = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
   construction_type_code = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
   reservoir_name_en = CASE WHEN ({script.datasetname}.reservoir_name_en IS NOT NULL AND {script.datasetname}.reservoir_name_en IS DISTINCT FROM cabd.reservoir_name_en) THEN {script.datasetname}.reservoir_name_en ELSE cabd.reservoir_name_en END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   {script.datasetname}.existing_pilot_region_pt IS TRUE
   AND cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)