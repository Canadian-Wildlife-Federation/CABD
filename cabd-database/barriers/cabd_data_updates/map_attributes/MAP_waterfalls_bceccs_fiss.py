import MAP_attributes_main as main

script = main.MappingScript("fiss")

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
    OR {script.datasetname}.data_source_id = duplicates.dups_{script.datasetname};       

--update existing features
UPDATE 
    {script.waterfallAttributeTable} AS cabdsource
SET    
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    fall_height_m_ds = CASE WHEN (cabd.fall_height_m IS NULL AND {script.datasetname}.fall_height_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.fall_height_m_ds END,

    waterbody_name_en_dsfid = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.waterbody_name_en_dsfid END,
    fall_height_m_dsfid = CASE WHEN (cabd.fall_height_m IS NULL AND {script.datasetname}.fall_height_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.fall_height_m_dsfid END
FROM
    {script.waterfallTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.waterfallTable} AS cabd
SET
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    fall_height_m = CASE WHEN (cabd.fall_height_m IS NULL AND {script.datasetname}.fall_height_m IS NOT NULL) THEN {script.datasetname}.fall_height_m ELSE cabd.fall_height_m END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
