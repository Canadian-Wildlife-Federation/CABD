import MAP_attributes_main as main

script = main.MappingScript("ab_basefeatures")

mappingquery = f"""

--update existing features 
UPDATE 
    {script.waterfallAttributeTable} AS cabdsource
SET    
    fall_name_en_ds = CASE WHEN (cabd.fall_name_en IS NULL AND {script.datasetname}.fall_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.fall_name_en_ds END,   
    fall_name_en_dsfid = CASE WHEN (cabd.fall_name_en IS NULL AND {script.datasetname}.fall_name_en IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.fall_name_en_dsfid END
FROM
    {script.waterfallTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.waterfallTable} AS cabd
SET
    fall_name_en = CASE WHEN (cabd.fall_name_en IS NULL AND {script.datasetname}.fall_name_en IS NOT NULL) THEN origin.fall_name_en ELSE cabd.fall_name_en END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;
"""

script.do_work(mappingquery)