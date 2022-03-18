import MAP_attributes_main as main

script = main.MappingScript("nrcan_canvec_hyf")

mappingquery = f"""

--find CABD IDs
UPDATE {script.workingTable} SET cabd_id = NULL;

UPDATE
	{script.workingTable} AS {script.datasetname}
SET
	cabd_id = duplicates.cabd_id
FROM
	{script.waterfallTable} AS duplicates
WHERE
	({script.datasetname}.data_source_id = duplicates.data_source_id AND duplicates.data_source_text = '{script.datasetname}') 
	OR {script.datasetname}.data_source_id = duplicates.{script.datasetname};    

--update existing features
UPDATE 
    {script.waterfallAttributeTable} AS cabdsource
SET    
    fall_name_en_ds = CASE WHEN (cabd.fall_name_en IS NULL AND {script.datasetname}.fall_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.fall_name_en_ds END,   
    fall_name_fr_ds = CASE WHEN (cabd.fall_name_fr IS NULL AND {script.datasetname}.fall_name_fr IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.fall_name_fr_ds END
FROM
    {script.waterfallTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.waterfallTable} AS cabd
SET
    fall_name_en = CASE WHEN (cabd.fall_name_en IS NULL AND {script.datasetname}.fall_name_en IS NOT NULL) THEN {script.datasetname}.fall_name_en ELSE cabd.fall_name_en END,
    fall_name_fr = CASE WHEN (cabd.fall_name_fr IS NULL AND {script.datasetname}.fall_name_fr IS NOT NULL) THEN {script.datasetname}.fall_name_fr ELSE cabd.fall_name_fr END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;
    
"""

script.do_work(mappingquery)
