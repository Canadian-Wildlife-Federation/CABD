import MAP_attributes_main as main

script = main.MappingScript("bcflnrord_kml_pubdams")
    
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
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
    owner_ds = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,
    ownership_type_code_ds = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,
    operating_status_code_ds = CASE WHEN (cabd.operating_status_code IS NULL AND {script.datasetname}.operating_status_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.operating_status_code_ds END,    
    construction_type_code_ds = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,
    function_code_ds = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
    "owner" = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END,
    operating_status_code = CASE WHEN (cabd.operating_status_code IS NULL AND {script.datasetname}.operating_status_code IS NOT NULL) THEN {script.datasetname}.operating_status_code ELSE cabd.operating_status_code END,        
    construction_type_code = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.height_m ELSE cabd.height_m END,
    length_m = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.length_m ELSE cabd.length_m END,
    function_code = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.function_code ELSE cabd.function_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
