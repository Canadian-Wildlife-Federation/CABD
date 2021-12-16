import nonspatial as main

script = main.MappingScript("bow_river_transalta")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Bow River Basin - TransAlta Operations', now(), null, 'Alberta Environment and Parks', 'Data update - ' || now());

--add data source to the table
UPDATE TABLE {script.workingTable} ADD COLUMN data_source varchar(512);
UPDATE TABLE {script.workingTable} SET data_source = {script.dsUuid};

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
    use_floodcontrol_code_ds = CASE WHEN {script.datasetname}.use_floodcontrol_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    operating_notes_ds = CASE WHEN {script.datasetname}.operating_notes IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.operating_notes_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    use_floodcontrol_code = CASE WHEN {script.datasetname}.use_floodcontrol_code IS NOT NULL THEN {script.datasetname}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    operating_notes = CASE WHEN {script.datasetname}.operating_notes IS NOT NULL THEN {script.datasetname}.operating_notes ELSE cabd.operating_notes END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)