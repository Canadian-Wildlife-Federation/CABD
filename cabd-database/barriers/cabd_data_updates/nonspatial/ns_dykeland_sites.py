import nonspatial as main

script = main.MappingScript("ns_dykeland_sites")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2022-02-08',
    'Government of Nova Scotia, no date. Working With the Tides - Dykeland Sites. Accessed February 8, 2022, from https://novascotia.ca/dykeland-system-upgrades/dykeland-sites/',
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
    waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN ({script.datasetName}.use_floodcontrol_code IS NOT NULL AND {script.datasetName}.use_floodcontrol_code IS DISTINCT FROM cabd.use_floodcontrol_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    length_m_ds = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.data_source ELSE cabdsource.length_m_ds END,
    construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
    comments_ds = CASE WHEN ({script.datasetName}.comments IS NOT NULL AND {script.datasetName}.comments IS DISTINCT FROM cabd.comments) THEN {script.datasetName}.data_source ELSE cabdsource.comments_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN NULL ELSE cabdsource.waterbody_name_en_dsfid END,
    use_code_dsfid = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
    use_floodcontrol_code_dsfid = CASE WHEN ({script.datasetName}.use_floodcontrol_code IS NOT NULL AND {script.datasetName}.use_floodcontrol_code IS DISTINCT FROM cabd.use_floodcontrol_code) THEN NULL ELSE cabdsource.use_floodcontrol_code_dsfid END,
    length_m_dsfid = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN NULL ELSE cabdsource.length_m_dsfid END,
    construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
    comments_dsfid = CASE WHEN ({script.datasetName}.comments IS NOT NULL AND {script.datasetName}.comments IS DISTINCT FROM cabd.comments) THEN NULL ELSE cabdsource.comments_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
    use_floodcontrol_code = CASE WHEN ({script.datasetName}.use_floodcontrol_code IS NOT NULL AND {script.datasetName}.use_floodcontrol_code IS DISTINCT FROM cabd.use_floodcontrol_code) THEN {script.datasetName}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    length_m = CASE WHEN ({script.datasetName}.length_m IS NOT NULL AND {script.datasetName}.length_m IS DISTINCT FROM cabd.length_m) THEN {script.datasetName}.length_m ELSE cabd.length_m END,
    construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
    "comments" = CASE WHEN ({script.datasetName}.comments IS NOT NULL AND {script.datasetName}.comments IS DISTINCT FROM cabd.comments) THEN {script.datasetName}.comments ELSE cabd.comments END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)