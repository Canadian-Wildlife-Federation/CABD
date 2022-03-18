import MAP_attributes_main as main

script = main.MappingScript("su_npdp")

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
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    construction_type_code_ds = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,
    use_code_ds = CASE WHEN (cabd.use_code is null and {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,
    use_irrigation_code_ds = CASE WHEN (cabd.use_irrigation_code is null and {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_irrigation_code_ds END,
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code is null and {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,
    use_supply_code_ds = CASE WHEN (cabd.use_supply_code is null and {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_supply_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN (cabd.use_floodcontrol_code is null and {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    use_recreation_code_ds = CASE WHEN (cabd.use_recreation_code is null and {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_recreation_code_ds END,
    use_navigation_code_ds = CASE WHEN (cabd.use_navigation_code is null and {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_navigation_code_ds END,
    maintenance_last_ds = CASE WHEN (cabd.maintenance_last is null and {script.datasetname}.maintenance_last IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.maintenance_last_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year is null and {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
    storage_capacity_mcm_ds = CASE WHEN (cabd.storage_capacity_mcm is null and {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.storage_capacity_mcm_ds END,
    reservoir_present_ds = CASE WHEN (cabd.reservoir_present is null and {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_present_ds END,
    generating_capacity_mwh_ds = CASE WHEN (cabd.generating_capacity_mwh is null and {script.datasetname}.generating_capacity_mwh IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.generating_capacity_mwh_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    construction_type_code = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.height_m ELSE cabd.height_m END,
    length_m = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.length_m ELSE cabd.length_m END,    
    use_code = CASE WHEN (cabd.use_code is null and {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.use_code ELSE cabd.use_code END,
    use_irrigation_code = CASE WHEN (cabd.use_irrigation_code is null and {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_electricity_code = CASE WHEN (cabd.use_electricity_code is null and {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
    use_supply_code = CASE WHEN (cabd.use_supply_code is null and {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.use_supply_code ELSE cabd.use_supply_code END,
    use_floodcontrol_code = CASE WHEN (cabd.use_floodcontrol_code is null and {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    use_recreation_code = CASE WHEN (cabd.use_recreation_code is null and {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.use_recreation_code ELSE cabd.use_recreation_code END,
    use_navigation_code = CASE WHEN (cabd.use_navigation_code is null and {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.use_navigation_code ELSE cabd.use_navigation_code END,
    maintenance_last = CASE WHEN (cabd.maintenance_last is null and {script.datasetname}.maintenance_last IS NOT NULL) THEN {script.datasetname}.maintenance_last ELSE cabd.maintenance_last END,
    construction_year = CASE WHEN (cabd.construction_year is null and {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
    storage_capacity_mcm = CASE WHEN (cabd.storage_capacity_mcm is null and {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,
    reservoir_present = CASE WHEN (cabd.reservoir_present is null and {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.reservoir_present ELSE cabd.reservoir_present END,
    generating_capacity_mwh = CASE WHEN (cabd.generating_capacity_mwh is null and {script.datasetname}.generating_capacity_mwh IS NOT NULL) THEN {script.datasetname}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END        
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
