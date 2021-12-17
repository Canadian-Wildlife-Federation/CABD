import nonspatial as main

script = main.MappingScript("wiki_little_bow")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Little Bow Lake Reservoir', now(), null, 'Wikipedia', 'Data update - ' || now());

--add data source to the table
UPDATE TABLE {script.workingTable} ADD COLUMN data_source varchar(512);
UPDATE TABLE {script.workingTable} SET data_source = {script.dsUuid};

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    reservoir_name_en_ds = CASE WHEN (cabd.reservoir_name_en IS NULL AND {script.datasetname}.reservoir_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_name_en_ds END,
    reservoir_present_ds = CASE WHEN (cabd.reservoir_present IS NULL AND {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_present_ds END,
    reservoir_area_skm_ds = CASE WHEN {script.datasetname}.reservoir_area_skm IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
    reservoir_depth_m_ds = CASE WHEN {script.datasetname}.reservoir_depth_m IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_depth_m_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    reservoir_name_en = CASE WHEN (cabd.reservoir_name_en IS NULL AND {script.datasetname}.reservoir_name_en IS NOT NULL) THEN {script.datasetname}.reservoir_name_en ELSE cabd.reservoir_name_en END,
    reservoir_present = CASE WHEN (cabd.reservoir_present IS NULL AND {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.reservoir_present ELSE cabd.reservoir_present END,
    reservoir_area_skm = CASE WHEN {script.datasetname}.reservoir_area_skm IS NOT NULL THEN {script.datasetname}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    reservoir_depth_m = CASE WHEN {script.datasetname}.reservoir_depth_m IS NOT NULL THEN {script.datasetname}.reservoir_depth_m ELSE cabd.reservoir_depth_m END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)