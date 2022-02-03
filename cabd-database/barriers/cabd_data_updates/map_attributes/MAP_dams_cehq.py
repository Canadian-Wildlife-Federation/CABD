import MAP_attributes_main as main

script = main.MappingScript("cehq")

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
    dam_name_fr_ds = CASE WHEN (cabd.dam_name_fr IS NULL AND {script.datasetname}.dam_name_fr IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_fr_ds END,
    reservoir_name_fr_ds = CASE WHEN (cabd.reservoir_name_fr IS NULL AND {script.datasetname}.reservoir_name_fr IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_name_fr_ds END,
    reservoir_present_ds = CASE WHEN (cabd.reservoir_present IS NULL AND {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_present_ds END,
    waterbody_name_fr_ds = CASE WHEN (cabd.waterbody_name_fr IS NULL AND {script.datasetname}.waterbody_name_fr IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_fr_ds END,
    use_code_ds = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,         
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,         
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,         
    construction_type_code_ds = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_type_code_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
    storage_capacity_mcm_ds = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.storage_capacity_mcm_ds END,
    reservoir_area_skm_ds = CASE WHEN (cabd.reservoir_area_skm IS NULL AND {script.datasetname}.reservoir_area_skm IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
    owner_ds = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,
    ownership_type_code_ds = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,
    maintenance_next_ds = CASE WHEN (cabd.maintenance_next IS NULL AND {script.datasetname}.maintenance_next IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.maintenance_next_ds END,
    maintenance_last_ds = CASE WHEN (cabd.maintenance_last IS NULL AND {script.datasetname}.maintenance_last IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.maintenance_last_ds END,
    
    dam_name_fr_dsfid = CASE WHEN (cabd.dam_name_fr IS NULL AND {script.datasetname}.dam_name_fr IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.dam_name_fr_dsfid END,
    reservoir_name_fr_dsfid = CASE WHEN (cabd.reservoir_name_fr IS NULL AND {script.datasetname}.reservoir_name_fr IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.reservoir_name_fr_dsfid END,
    reservoir_present_dsfid = CASE WHEN (cabd.reservoir_present IS NULL AND {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.reservoir_present_dsfid END,
    waterbody_name_fr_dsfid = CASE WHEN (cabd.waterbody_name_fr IS NULL AND {script.datasetname}.waterbody_name_fr IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.waterbody_name_fr_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.use_code_dsfid END,         
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.height_m_dsfid END,         
    length_m_dsfid = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.length_m_dsfid END,             
    construction_type_code_dsfid = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.construction_type_code_dsfid END,
    construction_year_dsfid = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.construction_year_dsfid END,
    storage_capacity_mcm_dsfid = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.storage_capacity_mcm_dsfid END,
    reservoir_area_skm_dsfid = CASE WHEN (cabd.reservoir_area_skm IS NULL AND {script.datasetname}.reservoir_area_skm IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.reservoir_area_skm_dsfid END,
    owner_dsfid = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.owner_dsfid END,
    ownership_type_code_dsfid = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.ownership_type_code_dsfid END,
    maintenance_next_dsfid = CASE WHEN (cabd.maintenance_next IS NULL AND {script.datasetname}.maintenance_next IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.maintenance_next_dsfid END,
    maintenance_last_dsfid = CASE WHEN (cabd.maintenance_last IS NULL AND {script.datasetname}.maintenance_last IS NOT NULL) THEN {script.datasetname}.data_source_id ELSE cabdsource.maintenance_last_dsfid END
        
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_fr = CASE WHEN (cabd.dam_name_fr IS NULL AND {script.datasetname}.dam_name_fr IS NOT NULL) THEN {script.datasetname}.dam_name_fr ELSE cabd.dam_name_fr END,
    reservoir_name_fr = CASE WHEN (cabd.reservoir_name_fr IS NULL AND {script.datasetname}.reservoir_name_fr IS NOT NULL) THEN {script.datasetname}.reservoir_name_fr ELSE cabd.reservoir_name_fr END,
    reservoir_present = CASE WHEN (cabd.reservoir_present IS NULL AND {script.datasetname}.reservoir_present IS NOT NULL) THEN {script.datasetname}.reservoir_present ELSE cabd.reservoir_present END,
    waterbody_name_fr = CASE WHEN (cabd.waterbody_name_fr IS NULL AND {script.datasetname}.waterbody_name_fr IS NOT NULL) THEN {script.datasetname}.waterbody_name_fr ELSE cabd.waterbody_name_fr END,
    use_code = CASE WHEN (cabd.use_code IS NULL AND {script.datasetname}.use_code IS NOT NULL) THEN {script.datasetname}.use_code ELSE cabd.use_code END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND {script.datasetname}.height_m IS NOT NULL) THEN {script.datasetname}.height_m ELSE cabd.height_m END,         
    length_m = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.length_m ELSE cabd.length_m END,         
    construction_type_code = CASE WHEN (cabd.construction_type_code IS NULL AND {script.datasetname}.construction_type_code IS NOT NULL) THEN {script.datasetname}.construction_type_code ELSE cabd.construction_type_code END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
    storage_capacity_mcm = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND {script.datasetname}.storage_capacity_mcm IS NOT NULL) THEN {script.datasetname}.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,
    reservoir_area_skm = CASE WHEN (cabd.reservoir_area_skm IS NULL AND {script.datasetname}.reservoir_area_skm IS NOT NULL) THEN {script.datasetname}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    "owner" = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END,
    maintenance_next = CASE WHEN (cabd.maintenance_next IS NULL AND {script.datasetname}.maintenance_next IS NOT NULL) THEN {script.datasetname}.maintenance_next ELSE cabd.maintenance_next END,
    maintenance_last = CASE WHEN (cabd.maintenance_last IS NULL AND {script.datasetname}.maintenance_next IS NOT NULL) THEN {script.datasetname}.maintenance_last ELSE cabd.maintenance_last END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)