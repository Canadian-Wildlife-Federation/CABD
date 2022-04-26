import MAP_attributes_main as main

script = main.MappingScript("wid_fishwerks")

mappingquery = f"""

--find CABD IDs
UPDATE {script.workingTable} SET cabd_id = NULL;

UPDATE
    {script.workingTable} AS {script.datasetname}
SET
    cabd_id = duplicates.cabd_id
FROM
    {script.damTable} AS duplicates
WHERE
    ({script.datasetname}.data_source_id = duplicates.data_source_id AND duplicates.data_source_text = '{script.datasetname}') 
    OR {script.datasetname}.data_source_id = duplicates.{script.datasetname};

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    use_invasivespecies_code_ds = CASE WHEN (cabd.use_invasivespecies_code IS NULL AND {script.datasetname}.use_invasivespecies_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_invasivespecies_code_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    use_invasivespecies_code = CASE WHEN (cabd.use_invasivespecies_code IS NULL AND {script.datasetname}.use_invasivespecies_code IS NOT NULL) THEN {script.datasetname}.use_invasivespecies_code ELSE cabd.use_invasivespecies_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
