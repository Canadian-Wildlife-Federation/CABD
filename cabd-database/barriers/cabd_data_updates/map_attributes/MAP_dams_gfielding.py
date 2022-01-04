import MAP_attributes_main as main

script = main.MappingScript("gfielding")

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
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
    use_code_ds = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,         
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,
    use_supply_code_ds = CASE WHEN (cabd.use_supply_code IS NULL AND {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_supply_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    use_recreation_code_ds = CASE WHEN (cabd.use_recreation_code IS NULL AND {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_recreation_code_ds END,
    use_navigation_code_ds = CASE WHEN (cabd.use_navigation_code IS NULL AND {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_navigation_code_ds END,
    use_pollution_code_ds = CASE WHEN (cabd.use_pollution_code IS NULL AND {script.datasetname}.use_pollution_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_pollution_code_ds END,
    function_code_ds = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END,
    operating_status_code_ds = CASE WHEN (cabd.operating_status_code IS NULL AND {script.datasetname}.operating_status_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.operating_status_code_ds END,
    comments_ds = CASE WHEN (cabd.comments IS NULL AND {script.datasetname}.comments IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.comments_ds END,
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.dam_name_en_dsfid END, 
    construction_year_dsfid = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.construction_year_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_code_dsfid END,         
    use_electricity_code_dsfid = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_electricity_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN (cabd.use_supply_code IS NULL AND {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_supply_code_dsfid END,
    use_floodcontrol_code_dsfid = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_floodcontrol_code_dsfid END,
    use_recreation_code_dsfid = CASE WHEN (cabd.use_recreation_code IS NULL AND {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_recreation_code_dsfid END,
    use_navigation_code_dsfid = CASE WHEN (cabd.use_navigation_code IS NULL AND {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_navigation_code_dsfid END,
    use_pollution_code_dsfid = CASE WHEN (cabd.use_pollution_code IS NULL AND {script.datasetname}.use_pollution_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_pollution_code_dsfid END,
    function_code_dsfid = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.function_code_dsfid END,
    operating_status_code_dsfid = CASE WHEN (cabd.operating_status_code IS NULL AND {script.datasetname}.operating_status_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.operating_status_code_dsfid END,
    comments_dsfid = CASE WHEN (cabd.comments IS NULL AND {script.datasetname}.comments IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.comments_dsfid END
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
    use_code = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.use_code ELSE cabd.use_code END,         
    use_electricity_code = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
    use_supply_code = CASE WHEN (cabd.use_supply_code IS NULL AND {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.use_supply_code ELSE cabd.use_supply_code END,
    use_floodcontrol_code = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    use_recreation_code = CASE WHEN (cabd.use_recreation_code IS NULL AND {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.use_recreation_code ELSE cabd.use_recreation_code END,
    use_navigation_code = CASE WHEN (cabd.use_navigation_code IS NULL AND {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.use_navigation_code ELSE cabd.use_navigation_code END,
    use_pollution_code = CASE WHEN (cabd.use_pollution_code IS NULL AND {script.datasetname}.use_pollution_code IS NOT NULL) THEN {script.datasetname}.use_pollution_code ELSE cabd.use_pollution_code END,
    function_code = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.function_code ELSE cabd.function_code END,
    operating_status_code = CASE WHEN (cabd.operating_status_code IS NULL AND {script.datasetname}.operating_status_code IS NOT NULL) THEN {script.datasetname}.operating_status_code ELSE cabd.operating_status_code END,
    "comments" = CASE WHEN (cabd.comments IS NULL AND {script.datasetname}.comments IS NOT NULL) THEN {script.datasetname}.comments ELSE cabd.comments END   
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)