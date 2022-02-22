import nonspatial as main

script = main.MappingScript("nlhydro_ca_nlh_155")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetname}',
    '2009-01-01',
    'Newfoundland Hydro, 2009. CA-NLH-155 Attachment 1 - Depreciation Methodology and Asset Service Lives. Accessed from http://www.pub.nl.ca/applications/ARCHIVE/NLH2012Depreciation/files/rfi/CA-NLH-155.pdf',
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
   height_m_ds = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
   length_m_ds = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,
   construction_year_ds = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
   construction_type_code_ds = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
   owner_ds = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,
   ownership_type_code_ds = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   facility_name_en_dsfid = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
   height_m_dsfid = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END,
   length_m_dsfid = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
   construction_year_dsfid = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
   construction_type_code_dsfid = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
   owner_dsfid = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
   ownership_type_code_dsfid = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN NULL ELSE cabdsource.ownership_type_code_dsfid END
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
   height_m = CASE WHEN ({script.datasetname}.height_m IS NOT NULL AND {script.datasetname}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetname}.height_m ELSE cabd.height_m END,
   length_m = CASE WHEN ({script.datasetname}.length_m IS NOT NULL AND {script.datasetname}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetname}.length_m ELSE cabd.length_m END,
   construction_year = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
   construction_type_code = CASE WHEN ({script.datasetname}.construction_type_code IS NOT NULL AND {script.datasetname}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
   "owner" = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetname}.owner ELSE cabd.owner END,
   ownership_type_code = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)