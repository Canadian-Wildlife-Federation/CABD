import MAP_attributes_main as main

script = main.MappingScript("grand")

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
    reservoir_name_en_ds = CASE WHEN (cabd.reservoir_name_en IS NULL AND {script.datasetname}.reservoir_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_name_en_ds END,
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,         
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,         
    reservoir_area_skm_ds = CASE WHEN (cabd.reservoir_area_skm IS NULL AND {script.datasetname}.reservoir_area_skm IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
    reservoir_depth_m_ds = CASE WHEN (cabd.reservoir_depth_m IS NULL AND {script.datasetname}.reservoir_depth_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_depth_m_ds END,
    reservoir_present_ds = CASE WHEN (cabd.reservoir_present IS NULL AND {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_present_ds END,
    storage_capacity_mcm_ds = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.storage_capacity_mcm_ds END,
    avg_rate_of_discharge_ls_ds = CASE WHEN (cabd.avg_rate_of_discharge_ls IS NULL AND {script.datasetname}.avg_rate_of_discharge_ls IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.avg_rate_of_discharge_ls_ds END,
    degree_of_regulation_pc_ds = CASE WHEN (cabd.degree_of_regulation_pc IS NULL AND {script.datasetname}.degree_of_regulation_pc IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.degree_of_regulation_pc_ds END,
    catchment_area_skm_ds = CASE WHEN (cabd.catchment_area_skm IS NULL AND {script.datasetname}.catchment_area_skm IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.catchment_area_skm_ds END,
    use_code_ds = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,         
    use_irrigation_code_ds = CASE WHEN (cabd.use_irrigation_code IS NULL AND {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_irrigation_code_ds END,
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,
    use_supply_code_ds = CASE WHEN (cabd.use_supply_code IS NULL AND {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_supply_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    use_recreation_code_ds = CASE WHEN (cabd.use_recreation_code IS NULL AND {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_recreation_code_ds END,
    use_navigation_code_ds = CASE WHEN (cabd.use_navigation_code IS NULL AND {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_navigation_code_ds END,
    use_fish_code_ds = CASE WHEN (cabd.use_fish_code IS NULL AND {script.datasetname}.use_fish_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_fish_code_ds END,
    use_pollution_code_ds = CASE WHEN (cabd.use_pollution_code IS NULL AND {script.datasetname}.use_pollution_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_pollution_code_ds END,
    use_other_code_ds = CASE WHEN (cabd.use_other_code IS NULL AND {script.datasetname}.use_other_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_other_code_ds END,
    lake_control_code_ds = CASE WHEN (cabd.lake_control_code IS NULL AND {script.datasetname}.lake_control_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.lake_control_code_ds END,
    "comments_ds" = CASE WHEN (cabd."comments" IS NULL AND {script.datasetname}."comments" IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.comments_ds END,
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.dam_name_en_dsfid END,
    reservoir_name_en_dsfid = CASE WHEN (cabd.reservoir_name_en IS NULL AND {script.datasetname}.reservoir_name_en IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.reservoir_name_en_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.waterbody_name_en_dsfid END,
    construction_year_dsfid = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.construction_year_dsfid END,
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.height_m_dsfid END,         
    length_m_dsfid = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.length_m_dsfid END,         
    reservoir_area_skm_dsfid = CASE WHEN (cabd.reservoir_area_skm IS NULL AND {script.datasetname}.reservoir_area_skm IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.reservoir_area_skm_dsfid END,
    reservoir_depth_m_dsfid = CASE WHEN (cabd.reservoir_depth_m IS NULL AND {script.datasetname}.reservoir_depth_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.reservoir_depth_m_dsfid END,
    reservoir_present_dsfid = CASE WHEN (cabd.reservoir_present IS NULL AND {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.reservoir_present_dsfid END,
    storage_capacity_mcm_dsfid = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.storage_capacity_mcm_dsfid END,
    avg_rate_of_discharge_ls_dsfid = CASE WHEN (cabd.avg_rate_of_discharge_ls IS NULL AND {script.datasetname}.avg_rate_of_discharge_ls IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.avg_rate_of_discharge_ls_dsfid END,
    degree_of_regulation_pc_dsfid = CASE WHEN (cabd.degree_of_regulation_pc IS NULL AND {script.datasetname}.degree_of_regulation_pc IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.degree_of_regulation_pc_dsfid END,
    catchment_area_skm_dsfid = CASE WHEN (cabd.catchment_area_skm IS NULL AND {script.datasetname}.catchment_area_skm IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.catchment_area_skm_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_code_dsfid END,         
    use_irrigation_code_dsfid = CASE WHEN (cabd.use_irrigation_code IS NULL AND {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_irrigation_code_dsfid END,
    use_electricity_code_dsfid = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_electricity_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN (cabd.use_supply_code IS NULL AND {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_supply_code_dsfid END,
    use_floodcontrol_code_dsfid = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_floodcontrol_code_dsfid END,
    use_recreation_code_dsfid = CASE WHEN (cabd.use_recreation_code IS NULL AND {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_recreation_code_dsfid END,
    use_navigation_code_dsfid = CASE WHEN (cabd.use_navigation_code IS NULL AND {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_navigation_code_dsfid END,
    use_fish_code_dsfid = CASE WHEN (cabd.use_fish_code IS NULL AND {script.datasetname}.use_fish_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_fish_code_dsfid END,
    use_pollution_code_dsfid = CASE WHEN (cabd.use_pollution_code IS NULL AND {script.datasetname}.use_pollution_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_pollution_code_dsfid END,
    use_other_code_dsfid = CASE WHEN (cabd.use_other_code IS NULL AND {script.datasetname}.use_other_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_other_code_dsfid END,
    lake_control_code_dsfid = CASE WHEN (cabd.lake_control_code IS NULL AND {script.datasetname}.lake_control_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.lake_control_code_dsfid END,
    "comments_dsfid" = CASE WHEN (cabd."comments" IS NULL AND {script.datasetname}."comments" IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.comments_dsfid END  
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;


UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    reservoir_name_en = CASE WHEN (cabd.reservoir_name_en IS NULL AND {script.datasetname}.reservoir_name_en IS NOT NULL) THEN {script.datasetname}.reservoir_name_en ELSE cabd.reservoir_name_en END,
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.height_m ELSE cabd.height_m END,         
    length_m = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.length_m ELSE cabd.length_m END,         
    reservoir_area_skm = CASE WHEN (cabd.reservoir_area_skm IS NULL AND {script.datasetname}.reservoir_area_skm IS NOT NULL) THEN {script.datasetname}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    reservoir_depth_m = CASE WHEN (cabd.reservoir_depth_m IS NULL AND {script.datasetname}.reservoir_depth_m IS NOT NULL) THEN {script.datasetname}.reservoir_depth_m ELSE cabd.reservoir_depth_m END,
    reservoir_present = CASE WHEN (cabd.reservoir_present IS NULL AND {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.reservoir_present ELSE cabd.reservoir_present END,
    storage_capacity_mcm = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,
    avg_rate_of_discharge_ls = CASE WHEN (cabd.avg_rate_of_discharge_ls IS NULL AND {script.datasetname}.avg_rate_of_discharge_ls IS NOT NULL) THEN {script.datasetname}.avg_rate_of_discharge_ls ELSE cabd.avg_rate_of_discharge_ls END,
    degree_of_regulation_pc = CASE WHEN (cabd.degree_of_regulation_pc IS NULL AND {script.datasetname}.degree_of_regulation_pc IS NOT NULL) THEN {script.datasetname}.degree_of_regulation_pc ELSE cabd.degree_of_regulation_pc END,
    catchment_area_skm = CASE WHEN (cabd.catchment_area_skm IS NULL AND {script.datasetname}.catchment_area_skm IS NOT NULL) THEN {script.datasetname}.catchment_area_skm ELSE cabd.catchment_area_skm END,
    use_code = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.use_code ELSE cabd.use_code END,         
    use_irrigation_code = CASE WHEN (cabd.use_irrigation_code IS NULL AND {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_electricity_code = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
    use_supply_code = CASE WHEN (cabd.use_supply_code IS NULL AND {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.use_supply_code ELSE cabd.use_supply_code END,
    use_floodcontrol_code = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    use_recreation_code = CASE WHEN (cabd.use_recreation_code IS NULL AND {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.use_recreation_code ELSE cabd.use_recreation_code END,
    use_navigation_code = CASE WHEN (cabd.use_navigation_code IS NULL AND {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.use_navigation_code ELSE cabd.use_navigation_code END,
    use_fish_code = CASE WHEN (cabd.use_fish_code IS NULL AND {script.datasetname}.use_fish_code IS NOT NULL) THEN {script.datasetname}.use_fish_code ELSE cabd.use_fish_code END,
    use_pollution_code = CASE WHEN (cabd.use_pollution_code IS NULL AND {script.datasetname}.use_pollution_code IS NOT NULL) THEN {script.datasetname}.use_pollution_code ELSE cabd.use_pollution_code END,
    use_other_code = CASE WHEN (cabd.use_other_code IS NULL AND {script.datasetname}.use_other_code IS NOT NULL) THEN {script.datasetname}.use_other_code ELSE cabd.use_other_code END,
    lake_control_code = CASE WHEN (cabd.lake_control_code IS NULL AND {script.datasetname}.lake_control_code IS NOT NULL) THEN {script.datasetname}.lake_control_code ELSE cabd.lake_control_code END,
    "comments" = CASE WHEN (cabd."comments" IS NULL AND {script.datasetname}."comments" IS NOT NULL) THEN {script.datasetname}."comments" ELSE cabd."comments" END   
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
