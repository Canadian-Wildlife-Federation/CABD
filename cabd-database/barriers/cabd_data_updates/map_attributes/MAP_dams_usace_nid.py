import MAP_attributes_main as main

script = main.MappingScript("usace_nid")

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
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_en_ds END,   
    owner_ds = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,   
    ownership_type_code_ds = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,   
    provincial_compliance_status_ds = CASE WHEN (cabd.provincial_compliance_status IS NULL AND {script.datasetname}.provincial_compliance_status IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.provincial_compliance_status_ds END,   
    federal_compliance_status_ds = CASE WHEN (cabd.federal_compliance_status IS NULL AND {script.datasetname}.federal_compliance_status IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.federal_compliance_status_ds END,   
    use_code_ds = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,   
    use_irrigation_code_ds = CASE WHEN (cabd.use_irrigation_code IS NULL AND {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_irrigation_code_ds END,   
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,   
    use_supply_code_ds = CASE WHEN (cabd.use_supply_code IS NULL AND {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_supply_code_ds END,   
    use_floodcontrol_code_ds = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_floodcontrol_code_ds END,   
    use_recreation_code_ds = CASE WHEN (cabd.use_recreation_code IS NULL AND {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_recreation_code_ds END,   
    use_navigation_code_ds = CASE WHEN (cabd.use_navigation_code IS NULL AND {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_navigation_code_ds END,   
    use_pollution_code_ds = CASE WHEN (cabd.use_pollution_code IS NULL AND {script.datasetname}.use_pollution_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_pollution_code_ds END,   
    use_other_code_ds = CASE WHEN (cabd.use_other_code IS NULL AND {script.datasetname}.use_other_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_other_code_ds END,   
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,   
    function_code_ds = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END,   
    construction_type_code_ds = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,   
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,   
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,   
    spillway_capacity_ds = CASE WHEN (cabd.spillway_capacity IS NULL AND {script.datasetname}.spillway_capacity IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.spillway_capacity_ds END,   
    spillway_type_code_ds = CASE WHEN (cabd.spillway_type_code IS NULL AND {script.datasetname}.spillway_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.spillway_type_code_ds END,   
    reservoir_area_skm_ds = CASE WHEN (cabd.reservoir_area_skm IS NULL AND {script.datasetname}.reservoir_area_skm IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_area_skm_ds END,   
    storage_capacity_mcm_ds = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.storage_capacity_mcm_ds END,   
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.dam_name_en_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.waterbody_name_en_dsfid END,
    owner_dsfid = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.owner_dsfid END,
    ownership_type_code_dsfid = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.ownership_type_code_dsfid END,
    provincial_compliance_status_dsfid = CASE WHEN (cabd.provincial_compliance_status IS NULL AND {script.datasetname}.provincial_compliance_status IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.provincial_compliance_status_dsfid END,
    federal_compliance_status_dsfid = CASE WHEN (cabd.federal_compliance_status IS NULL AND {script.datasetname}.federal_compliance_status IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.federal_compliance_status_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_code_dsfid END,
    use_irrigation_code_dsfid = CASE WHEN (cabd.use_irrigation_code IS NULL AND {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_irrigation_code_dsfid END,
    use_electricity_code_dsfid = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_electricity_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN (cabd.use_supply_code IS NULL AND {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_supply_code_dsfid END,
    use_floodcontrol_code_dsfid = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_floodcontrol_code_dsfid END,
    use_recreation_code_dsfid = CASE WHEN (cabd.use_recreation_code IS NULL AND {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_recreation_code_dsfid END,
    use_navigation_code_dsfid = CASE WHEN (cabd.use_navigation_code IS NULL AND {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_navigation_code_dsfid END,
    use_pollution_code_dsfid = CASE WHEN (cabd.use_pollution_code IS NULL AND {script.datasetname}.use_pollution_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_pollution_code_dsfid END,
    use_other_code_dsfid = CASE WHEN (cabd.use_other_code IS NULL AND {script.datasetname}.use_other_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_other_code_dsfid END,
    construction_year_dsfid = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.construction_year_dsfid END,
    function_code_dsfid = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.function_code_dsfid END,
    construction_type_code_dsfid = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.construction_type_code_dsfid END,
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.height_m_dsfid END,
    length_m_dsfid = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.length_m_dsfid END,
    spillway_capacity_dsfid = CASE WHEN (cabd.spillway_capacity IS NULL AND {script.datasetname}.spillway_capacity IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.spillway_capacity_dsfid END,
    spillway_type_code_dsfid = CASE WHEN (cabd.spillway_type_code IS NULL AND {script.datasetname}.spillway_type_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.spillway_type_code_dsfid END,
    reservoir_area_skm_dsfid = CASE WHEN (cabd.reservoir_area_skm IS NULL AND {script.datasetname}.reservoir_area_skm IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.reservoir_area_skm_dsfid END,
    storage_capacity_mcm_dsfid = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.storage_capacity_mcm_dsfid END
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
    "owner" = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END,
    provincial_compliance_status = CASE WHEN (cabd.provincial_compliance_status IS NULL AND {script.datasetname}.provincial_compliance_status IS NOT NULL) THEN {script.datasetname}.provincial_compliance_status ELSE cabd.provincial_compliance_status END,
    federal_compliance_status = CASE WHEN (cabd.federal_compliance_status IS NULL AND {script.datasetname}.federal_compliance_status IS NOT NULL) THEN {script.datasetname}.federal_compliance_status ELSE cabd.federal_compliance_status END,
    use_code = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.use_code ELSE cabd.use_code END,
    use_irrigation_code = CASE WHEN (cabd.use_irrigation_code IS NULL AND {script.datasetname}.use_irrigation_code IS NOT NULL) THEN {script.datasetname}.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_electricity_code = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
    use_supply_code = CASE WHEN (cabd.use_supply_code IS NULL AND {script.datasetname}.use_supply_code IS NOT NULL) THEN {script.datasetname}.use_supply_code ELSE cabd.use_supply_code END,
    use_floodcontrol_code = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND {script.datasetname}.use_floodcontrol_code IS NOT NULL) THEN {script.datasetname}.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    use_recreation_code = CASE WHEN (cabd.use_recreation_code IS NULL AND {script.datasetname}.use_recreation_code IS NOT NULL) THEN {script.datasetname}.use_recreation_code ELSE cabd.use_recreation_code END,
    use_navigation_code = CASE WHEN (cabd.use_navigation_code IS NULL AND {script.datasetname}.use_navigation_code IS NOT NULL) THEN {script.datasetname}.use_navigation_code ELSE cabd.use_navigation_code END,
    use_pollution_code = CASE WHEN (cabd.use_pollution_code IS NULL AND {script.datasetname}.use_pollution_code IS NOT NULL) THEN {script.datasetname}.use_pollution_code ELSE cabd.use_pollution_code END,
    use_other_code = CASE WHEN (cabd.use_other_code IS NULL AND {script.datasetname}.use_other_code IS NOT NULL) THEN {script.datasetname}.use_other_code ELSE cabd.use_other_code END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
    function_code = CASE WHEN (cabd.function_code IS NULL AND {script.datasetname}.function_code IS NOT NULL) THEN {script.datasetname}.function_code ELSE cabd.function_code END,
    construction_type_code = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.height_m ELSE cabd.height_m END,
    length_m = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.length_m ELSE cabd.length_m END,
    spillway_capacity = CASE WHEN (cabd.spillway_capacity IS NULL AND {script.datasetname}.spillway_capacity IS NOT NULL) THEN {script.datasetname}.spillway_capacity ELSE cabd.spillway_capacity END,
    spillway_type_code = CASE WHEN (cabd.spillway_type_code IS NULL AND {script.datasetname}.spillway_type_code IS NOT NULL) THEN {script.datasetname}.spillway_type_code ELSE cabd.spillway_type_code END,
    reservoir_area_skm = CASE WHEN (cabd.reservoir_area_skm IS NULL AND {script.datasetname}.reservoir_area_skm IS NOT NULL) THEN {script.datasetname}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    storage_capacity_mcm = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)