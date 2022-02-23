import nonspatial as main

script = main.MappingScript("nl_mae_permit_alt9067")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2017-02-28',
    'Newfoundland and Labrador Department of Municipal Affairs and Environment - Water Resources Management Division, 2017. Permit to Alter a Body of Water - Permit No. ALT9067-2017. Accessed from https://www.gov.nl.ca/ecc/files/waterres-permits-water-alt-dams-2017-r-alt9067-2017.pdf',
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
    provincial_flow_req_ds = CASE WHEN ({script.datasetName}.provincial_flow_req IS NOT NULL AND {script.datasetName}.provincial_flow_req IS DISTINCT FROM cabd.provincial_flow_req) THEN {script.datasetName}.data_source ELSE cabdsource.provincial_flow_req_ds END,
    height_m_ds = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.data_source ELSE cabdsource.height_m_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
    provincial_flow_req_dsfid = CASE WHEN ({script.datasetName}.provincial_flow_req IS NOT NULL AND {script.datasetName}.provincial_flow_req IS DISTINCT FROM cabd.provincial_flow_req) THEN NULL ELSE cabdsource.provincial_flow_req_dsfid END,
    height_m_dsfid = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END
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
    provincial_flow_req = CASE WHEN ({script.datasetName}.provincial_flow_req IS NOT NULL AND {script.datasetName}.provincial_flow_req IS DISTINCT FROM cabd.provincial_flow_req) THEN {script.datasetName}.provincial_flow_req ELSE cabd.provincial_flow_req END,
    height_m = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.height_m ELSE cabd.height_m END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)