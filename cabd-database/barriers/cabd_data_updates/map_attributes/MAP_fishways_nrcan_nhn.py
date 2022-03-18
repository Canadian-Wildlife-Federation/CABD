import MAP_attributes_main as main

script = main.MappingScript("nrcan_nhn")

mappingquery = f"""

--find CABD IDs
UPDATE {script.workingTable} SET cabd_id = NULL;

UPDATE
    {script.workingTable} AS {script.datasetname}
SET
    cabd_id = duplicates.cabd_id
FROM
    {script.fishwayTable} AS duplicates
WHERE
    ({script.datasetname}.data_source_id = duplicates.data_source_id AND duplicates.data_source_text = '{script.datasetname}') 
    OR {script.datasetname}.data_source_id = duplicates.{script.datasetname};  
    
--update existing features
UPDATE 
    {script.fishwayAttributeTable} AS cabdsource
SET    
    structure_name_en_ds = CASE WHEN (cabd.structure_name_en IS NULL AND {script.datasetname}.structure_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.structure_name_en_ds END
FROM
    {script.fishwayTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;


UPDATE
    {script.fishwayTable} AS cabd
SET
    structure_name_en = CASE WHEN (cabd.structure_name_en IS NULL AND {script.datasetname}.structure_name_en IS NOT NULL) THEN {script.datasetname}.structure_name_en ELSE cabd.structure_name_en END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
