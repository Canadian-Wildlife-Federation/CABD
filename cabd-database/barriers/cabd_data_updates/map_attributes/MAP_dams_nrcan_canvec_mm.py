import MAP_attributes_main as main

script = main.MappingScript("nrcan_canvec_mm")

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
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,   
    dam_name_fr_ds = CASE WHEN (cabd.dam_name_fr IS NULL AND {script.datasetname}.dam_name_fr IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_fr_ds END,
    operating_status_code_ds = CASE WHEN (cabd.operating_status_code IS NULL AND {script.datasetname}.operating_status_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.operating_status_code_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    dam_name_fr = CASE WHEN (cabd.dam_name_fr IS NULL AND {script.datasetname}.dam_name_fr IS NOT NULL) THEN {script.datasetname}.dam_name_fr ELSE cabd.dam_name_fr END,
    operating_status_code = CASE WHEN (cabd.operating_status_code IS NULL AND {script.datasetname}.operating_status_code IS NOT NULL) THEN {script.datasetname}.operating_status_code ELSE cabd.operating_status_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;
    
"""
script.do_work(mappingquery)
