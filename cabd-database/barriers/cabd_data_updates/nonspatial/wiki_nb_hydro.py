import nonspatial as main

script = main.MappingScript("wiki_nb_hydro")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2021-11-05',
    'Wikipedia contributors, 2021. List of generating stations in New Brunswick. Wikipedia. Accessed February 8, 2022, from https://en.wikipedia.org/wiki/List_of_generating_stations_in_New_Brunswick',
    'Accessed February 8, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} SET data_source = '{script.dsUuid}';

--update new features
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND cabd.dam_name_en IS NULL) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND cabd.construction_type_code IS NULL) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
    waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND cabd.waterbody_name_en IS NULL) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    construction_year_ds = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND cabd.construction_year IS NULL) THEN {script.datasetName}.data_source ELSE cabdsource.construction_year_ds END,
    generating_capacity_mwh_ds = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND cabd.generating_capacity_mwh IS NULL) THEN {script.datasetName}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
    owner_ds = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND cabd.owner IS NULL) THEN {script.datasetName}.data_source ELSE cabdsource.owner_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND cabd.dam_name_en IS NULL) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND cabd.construction_type_code IS NULL) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND cabd.waterbody_name_en IS NULL) THEN NULL ELSE cabdsource.waterbody_name_en_dsfid END,
    construction_year_dsfid = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND cabd.construction_year IS NULL) THEN NULL ELSE cabdsource.construction_year_dsfid END,
    generating_capacity_mwh_dsfid = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND cabd.generating_capacity_mwh IS NULL) THEN NULL ELSE cabdsource.generating_capacity_mwh_dsfid END,
    owner_dsfid = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND cabd.owner IS NULL) THEN NULL ELSE cabdsource.owner_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND cabd.dam_name_en IS NULL) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND cabd.construction_type_code IS NULL) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
    waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND cabd.waterbody_name_en IS NULL) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    construction_year = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND cabd.construction_year IS NULL) THEN {script.datasetName}.construction_year ELSE cabd.construction_year END,
    generating_capacity_mwh = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND cabd.generating_capacity_mwh IS NULL) THEN {script.datasetName}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
    "owner" = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND cabd.owner IS NULL) THEN {script.datasetName}.owner ELSE cabd.owner END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)