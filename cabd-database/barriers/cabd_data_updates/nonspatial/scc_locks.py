import nonspatial as main

script = main.MappingScript("scc_locks")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2022-02-08',
    'Shubenacadie Canal Commission, no date. The Locks. Accessed February 8, 2022, from https://www.shubenacadiecanal.ca/the-locks',
    'Accessed February 8, 2022',
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
    waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    construction_year_ds = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.data_source ELSE cabdsource.construction_year_ds END,
    maintenance_last_ds = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.data_source ELSE cabdsource.maintenance_last_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN NULL ELSE cabdsource.waterbody_name_en_dsfid END,
    construction_year_dsfid = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
    maintenance_last_dsfid = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN NULL ELSE cabdsource.maintenance_last_dsfid END
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
    waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    construction_year = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.construction_year ELSE cabd.construction_year END,
    maintenance_last = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.maintenance_last ELSE cabd.maintenance_last END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)