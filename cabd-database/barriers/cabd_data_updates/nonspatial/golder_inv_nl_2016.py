import nonspatial as main

script = main.MappingScript("golder_inv_nl_2016")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2016-11-01',
    'Golder Associates Inc., 2016. Inventory and Assessment of Dams in Eastern Newfoundland. pp. 5-42. Accessed from https://www.gov.nl.ca/ecc/files/waterres-reports-dam-safety-1533903-01-rev1-east-nl-dam-inv-report.pdf',
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
    use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
    use_pollution_code_ds = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_pollution_code_ds END,
    condition_code_ds = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN {script.datasetName}.data_source ELSE cabdsource.condition_code_ds END,
    construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
    height_m_ds = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.data_source ELSE cabdsource.height_m_ds END,
    length_m_ds = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.data_source ELSE cabdsource.length_m_ds END,
    maintenance_last_ds = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.data_source ELSE cabdsource.maintenance_last_ds END,
    operating_status_code_ds = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.operating_status_code_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
    owner_dsfid = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
    use_code_dsfid = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
    use_pollution_code_dsfid = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN NULL ELSE cabdsource.use_pollution_code_dsfid END,
    condition_code_dsfid = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN NULL ELSE cabdsource.condition_code_dsfid END,
    construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
    height_m_dsfid = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END,
    length_m_dsfid = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
    maintenance_last_dsfid = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN NULL ELSE cabdsource.maintenance_last_dsfid END,
    operating_status_code_dsfid = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN NULL ELSE cabdsource.operating_status_code_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.existing_pilot_region_pt IS FALSE
    AND (cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    facility_name_en = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.facility_name_en ELSE cabd.facility_name_en END,
    "owner" = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.owner ELSE cabd.owner END,
    use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
    use_pollution_code = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.use_pollution_code ELSE cabd.use_pollution_code END,
    condition_code = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN {script.datasetName}.condition_code ELSE cabd.condition_code END,
    construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
    height_m = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.height_m ELSE cabd.height_m END,
    length_m = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.length_m ELSE cabd.length_m END,
    maintenance_last = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.maintenance_last ELSE cabd.maintenance_last END,
    operating_status_code = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.operating_status_code ELSE cabd.operating_status_code END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.existing_pilot_region_pt IS FALSE    
    AND cabd.cabd_id = {script.datasetName}.cabd_id;


--update pilot region features
UPDATE
   {script.liveDamAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    facility_name_en_ds = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_en_ds END,
    owner_ds = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.data_source ELSE cabdsource.owner_ds END,
    use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
    use_pollution_code_ds = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_pollution_code_ds END,
    condition_code_ds = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN {script.datasetName}.data_source ELSE cabdsource.condition_code_ds END,
    construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
    height_m_ds = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.data_source ELSE cabdsource.height_m_ds END,
    length_m_ds = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.data_source ELSE cabdsource.length_m_ds END,
    maintenance_last_ds = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.data_source ELSE cabdsource.maintenance_last_ds END,
    operating_status_code_ds = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.operating_status_code_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
    owner_dsfid = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
    use_code_dsfid = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
    use_pollution_code_dsfid = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN NULL ELSE cabdsource.use_pollution_code_dsfid END,
    condition_code_dsfid = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN NULL ELSE cabdsource.condition_code_dsfid END,
    construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
    height_m_dsfid = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN NULL ELSE cabdsource.height_m_dsfid END,
    length_m_dsfid = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
    maintenance_last_dsfid = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN NULL ELSE cabdsource.maintenance_last_dsfid END,
    operating_status_code_dsfid = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN NULL ELSE cabdsource.operating_status_code_dsfid END
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
    "owner" = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.owner ELSE cabd.owner END,
    use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
    use_pollution_code = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.use_pollution_code ELSE cabd.use_pollution_code END,
    condition_code = CASE WHEN ({script.datasetName}.condition_code IS NOT NULL AND {script.datasetName}.condition_code IS DISTINCT FROM cabd.condition_code) THEN {script.datasetName}.condition_code ELSE cabd.condition_code END,
    construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
    height_m = CASE WHEN ({script.datasetName}.height_m IS NOT NULL AND {script.datasetName}.height_m IS DISTINCT FROM cabd.height_m) THEN {script.datasetName}.height_m ELSE cabd.height_m END,
    length_m = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.length_m ELSE cabd.length_m END,
    maintenance_last = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.maintenance_last ELSE cabd.maintenance_last END,
    operating_status_code = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.operating_status_code ELSE cabd.operating_status_code END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.existing_pilot_region_pt IS TRUE
    AND cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)