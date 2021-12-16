import nonspatial as main

script = main.MappingScript("st_mary_spillway")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'St. Mary Dam Spillway Replacement Project', now(), null, 'Advanced Construction Techniques Ltd.', 'Data update - ' || now());

--add data source to the table
UPDATE TABLE {script.workingTable} ADD COLUMN data_source varchar(512);
UPDATE TABLE {script.workingTable} SET data_source = {script.dsUuid};

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,   
    owner_ds = CASE WHEN {script.datasetname}.owner IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,   
    ownership_type_code_ds = CASE WHEN {script.datasetname}.ownership_type_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END   
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    "owner" = CASE WHEN {script.datasetname}."owner" IS NOT NULL THEN {script.datasetname}."owner" ELSE cabd."owner" END,
    ownership_type_code = CASE WHEN {script.datasetname}.ownership_type_code IS NOT NULL THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)