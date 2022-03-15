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
    {script.damTable} AS duplicates
WHERE
    ({script.datasetname}.data_source_id = duplicates.data_source_id AND duplicates.data_source_text = '{script.datasetname}') 
    OR {script.datasetname}.data_source_id = duplicates.dups_{script.datasetname};  

--update existing features
UPDATE 
    {script.damAttributeTable} AS cabdsource
SET    
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    use_code_ds = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,
    function_code_ds = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END,
    use_irrigation_code_ds = CASE WHEN (cabd.use_irrigation_code IS NULL AND {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_irrigation_code_ds END,
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,
    use_fish_code_ds = CASE WHEN (cabd.use_fish_code IS NULL AND {script.datasetname}.use_fish_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_fish_code_ds END,
    use_other_code_ds = CASE WHEN (cabd.use_other_code IS NULL AND {script.datasetname}.use_other_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_other_code_ds END,

    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.height_m_dsfid END,
    length_m_dsfid = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.length_m_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.waterbody_name_en_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_code_dsfid END,
    function_code_dsfid = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.function_code_dsfid END,
    use_irrigation_code_dsfid = CASE WHEN (cabd.use_irrigation_code IS NULL AND {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_irrigation_code_dsfid END,
    use_electricity_code_dsfid = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_electricity_code_dsfid END,
    use_fish_code_dsfid = CASE WHEN (cabd.use_fish_code IS NULL AND {script.datasetname}.use_fish_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_fish_code_dsfid END,
    use_other_code_dsfid = CASE WHEN (cabd.use_other_code IS NULL AND {script.datasetname}.use_other_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_other_code_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    height_m = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.height_m ELSE cabd.height_m END,
    length_m = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.length_m ELSE cabd.length_m END,        
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.waterbody_name_en ELSE cabd.waterbody_name_en END,        
    use_code = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.use_code ELSE cabd.use_code END,
    function_code = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.function_code ELSE cabd.function_code END,
    use_irrigation_code = CASE WHEN (cabd.use_irrigation_code IS NULL AND {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_electricity_code = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
    use_fish_code = CASE WHEN (cabd.use_fish_code IS NULL AND {script.datasetname}.use_fish_code IS NOT NULL) THEN {script.datasetname}.use_fish_code ELSE cabd.use_fish_code END,
    use_other_code = CASE WHEN (cabd.use_other_code IS NULL AND {script.datasetname}.use_other_code IS NOT NULL) THEN {script.datasetname}.use_other_code ELSE cabd.use_other_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
