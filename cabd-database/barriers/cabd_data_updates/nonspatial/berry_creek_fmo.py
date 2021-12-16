import nonspatial as main

script = main.MappingScript("berry_creek_fmo")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Berry Creek Reservoir Fisheries Management Objectives', now(), null, 'Government of Alberta', 'Data update - ' || now());

--add data source to the table
UPDATE TABLE {script.workingTable} ADD COLUMN data_source varchar(512);
UPDATE TABLE {script.workingTable} SET data_source = {script.dsUuid};

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
    height_m_ds = CASE WHEN {script.datasetname}.height_m IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
    reservoir_area_skm_ds = CASE WHEN {script.datasetname}.reservoir_area_skm IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_area_skm_ds END 
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    height_m = CASE WHEN {script.datasetname}.height_m IS NOT NULL THEN {script.datasetname}.height_m ELSE cabd.height_m END,
    reservoir_area_skm = CASE WHEN {script.datasetname}.reservoir_area_skm IS NOT NULL THEN {script.datasetname}.reservoir_area_skm ELSE cabd.reservoir_area_skm END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)