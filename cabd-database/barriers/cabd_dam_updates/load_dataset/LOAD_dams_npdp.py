import LOAD_dams_main as main

script = main.DamLoadingScript("npdp")

query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = npdp_id;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN length_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN use_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN maintenance_last date;
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_present bool;
ALTER TABLE {script.tempTable} ADD COLUMN generating_capacity_mwh float8;

UPDATE {script.tempTable} SET dam_name_en = dam_name;
UPDATE {script.tempTable} SET waterbody_name_en = river;
UPDATE {script.tempTable} SET construction_type_code =
    CASE
    WHEN dam_type = 'Arch' THEN 1
    WHEN dam_type = 'Arch; Rockfill' THEN 1
    WHEN dam_type = 'Arch; Rockfill; Gravity' THEN 1
    WHEN dam_type = 'Buttress' THEN 2
    WHEN dam_type = 'Buttress; Earth' THEN 2
    WHEN dam_type = 'Earth' THEN 3
    WHEN dam_type = 'Earth; Gravity' THEN 3
    WHEN dam_type = 'Earth; Gravity; Rockfill' THEN 3
    WHEN dam_type = 'Earth; Rockfill' THEN 3
    WHEN dam_type = 'Earth; Rockfill; Arch' THEN 3
    WHEN dam_type = 'Earth; Rockfill; Gravity' THEN 3
    WHEN dam_type = 'Earth;Gravity' THEN 3
    WHEN dam_type = 'Earth;Rockfill' THEN 3
    WHEN dam_type = 'Gavity' THEN 4
    WHEN dam_type = 'Gravity' THEN 4
    WHEN dam_type = 'Gravity; Earth' THEN 4
    WHEN dam_type = 'Gravity; Rockfill' THEN 4
    WHEN dam_type = 'Gravity; Rockfill; Earth' THEN 4
    WHEN dam_type = 'Multi-Arch' THEN 5
    WHEN dam_type = 'Multiple Arch' THEN 5
    WHEN dam_type = 'Rockfill' THEN 6
    WHEN dam_type = 'Rockfill; Earth' THEN 6
    WHEN dam_type = 'Rockfill; Earth; Gravity' THEN 6
    WHEN dam_type = 'Rockfill; Gravity' THEN 6
    ELSE NULL END;
UPDATE {script.tempTable} SET height_m = dam_height_m;
UPDATE {script.tempTable} SET length_m = dam_length_m;
UPDATE {script.tempTable} SET use_code =
    CASE 
    WHEN main_purpose = 'Irrigation' THEN 1
    WHEN main_purpose = 'Hydroelectricity' THEN 2
    WHEN main_purpose = 'Water Supply' THEN 3
    WHEN main_purpose = 'Flood Control' THEN 4
    WHEN main_purpose = 'Recreation' THEN 5
    WHEN main_purpose = 'Navigation' THEN 6
    WHEN main_purpose = 'Fishery' THEN 7
    WHEN main_purpose = 'Pollution control' THEN 8
    WHEN main_purpose = 'Invasive species control' THEN 9
    WHEN main_purpose = 'Other' THEN 10
    ELSE NULL END;
UPDATE {script.tempTable} SET use_irrigation_code =
    CASE
    WHEN main_purpose = 'Irrigation' THEN 1
    WHEN other_purposes = 'Irrigation' THEN 2
    WHEN other_purposes = 'Irrigation; Recreation' THEN 2
    WHEN other_purposes = 'Water Supply; Irrigation; Recreation' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_electricity_code =
    CASE
    WHEN main_purpose = 'Hydroelectricity' THEN 1
    WHEN other_purposes = 'Hydroelectricity' THEN 2
    WHEN other_purposes = 'Hydroelectricity; Recreation' THEN 2 
    WHEN other_purposes = 'Hydroelectricity; Water Supply' THEN 2
    WHEN other_purposes = 'Hydropower; Recreation' THEN 2
    WHEN other_purposes = 'Water Supply; Hydroelectricity' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_supply_code =
    CASE
    WHEN main_purpose = 'Water Supply' THEN 1
    WHEN other_purposes = 'Water Supply' THEN 2
    WHEN other_purposes = 'Water Supply; Flood Control; Recreation' THEN 2
    WHEN other_purposes = 'Water Supply; Irrigation; Recreation' THEN 2 
    WHEN other_purposes = 'Water Supply; Hydroelectricity' THEN 2
    WHEN other_purposes = 'Water Supply; Recreation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation; Water Supply' THEN 3 
    WHEN other_purposes = 'Hydroelectricity; Water Supply' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_floodcontrol_code =
    CASE
    WHEN main_purpose = 'Flood Control' THEN 1
    WHEN other_purposes = 'Flood Control' THEN 2
    WHEN other_purposes = 'Flood Control, Navigation' THEN 2
    WHEN other_purposes = 'Flood Control; Navigation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation; Water Supply' THEN 2
    WHEN other_purposes = 'Water Supply; Flood Control; Recreation' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_recreation_code =
    CASE
    WHEN main_purpose = 'Recreation' THEN 1
    WHEN other_purposes = 'Recreation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation' THEN 3
    WHEN other_purposes = 'Flood Control; Recreation; Water Supply' THEN 3 
    WHEN other_purposes = 'Hydroelectricity; Recreation' THEN 3
    WHEN other_purposes = 'Hydropower; Recreation' THEN 3
    WHEN other_purposes = 'Irrigation; Recreation' THEN 3
    WHEN other_purposes = 'Navigation; Recreation' THEN 3
    WHEN other_purposes = 'Water Supply; Flood Control; Recreation' THEN 3
    WHEN other_purposes = 'Water Supply; Irrigation; Recreation' THEN 3 
    WHEN other_purposes = 'Water Supply; Recreation' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_navigation_code =
    CASE
    WHEN other_purposes = 'Navigation' THEN 2
    WHEN other_purposes = 'Navigation; Recreation' THEN 2
    WHEN other_purposes = 'Flood Control, Navigation' THEN 3
    WHEN other_purposes = 'Flood Control; Navigation' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET maintenance_last = ('01-01-' || year_modified)::date;
UPDATE {script.tempTable} SET construction_year = year_completed;
UPDATE {script.tempTable} SET storage_capacity_mcm = (regexp_replace(normal_reservoir_storage_m3, '[^0-9.]+', '', 'g'))::float8;
UPDATE {script.tempTable} SET storage_capacity_mcm = storage_capacity_mcm / 1000000;
UPDATE {script.tempTable} SET reservoir_present =
    CASE
    WHEN storage_capacity_mcm IS NOT NULL AND storage_capacity_mcm > 0 THEN TRUE
    ELSE FALSE END;
UPDATE {script.tempTable} SET generating_capacity_mwh = electric_capacity;


ALTER TABLE {script.tempTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    waterbody_name_en varchar(512),
    construction_type_code int2,
    height_m float4,
    length_m float4,
    use_code int2,
    use_irrigation_code int2,
    use_electricity_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_recreation_code int2,
    use_navigation_code int2,
    maintenance_last date,
    construction_year numeric,
    storage_capacity_mcm float8,
    reservoir_present bool,
    generating_capacity_mwh float8,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    waterbody_name_en,
    construction_type_code,
    height_m,
    length_m,
    use_code,
    use_irrigation_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    maintenance_last,
    construction_year,
    storage_capacity_mcm,
    reservoir_present,
    generating_capacity_mwh,
    data_source,
    data_source_id
)
SELECT
    dam_name_en,
    waterbody_name_en,
    construction_type_code,
    height_m,
    length_m,
    use_code,
    use_irrigation_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    maintenance_last,
    construction_year,
    storage_capacity_mcm,
    reservoir_present,
    generating_capacity_mwh,
    data_source,
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN waterbody_name_en,
    DROP COLUMN construction_type_code,
    DROP COLUMN height_m,
    DROP COLUMN length_m,
    DROP COLUMN use_code,
    DROP COLUMN use_irrigation_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_recreation_code,
    DROP COLUMN use_navigation_code,
    DROP COLUMN maintenance_last,
    DROP COLUMN construction_year,
    DROP COLUMN storage_capacity_mcm,
    DROP COLUMN reservoir_present,
    DROP COLUMN generating_capacity_mwh;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS npdp
SET
	cabd_id = duplicates.cabd_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
    (npdp.data_source_id = duplicates.data_source_id AND duplicates.data_source = 'npdp') 
    OR npdp.data_source_id = duplicates.dups_npdp;       
"""

#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'npdp', now(), null, null, 'Data update - ' || now());

--update existing features
UPDATE 
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source ELSE cabdsource.dam_name_en_ds END,
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source ELSE cabdsource.waterbody_name_en_ds END,
    construction_type_code_ds = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.construction_type_code_ds END,
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source ELSE cabdsource.height_m_ds END,
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source ELSE cabdsource.length_m_ds END,
    use_code_ds = CASE WHEN (cabd.use_code is null and origin.use_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_code_ds END,
    use_irrigation_code_ds = CASE WHEN (cabd.use_irrigation_code is null and origin.use_irrigation_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_irrigation_code_ds END,
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code is null and origin.use_electricity_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_electricity_code_ds END,
    use_supply_code_ds = CASE WHEN (cabd.use_supply_code is null and origin.use_supply_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_supply_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN (cabd.use_floodcontrol_code is null and origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    use_recreation_code_ds = CASE WHEN (cabd.use_recreation_code is null and origin.use_recreation_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_recreation_code_ds END,
    use_navigation_code_ds = CASE WHEN (cabd.use_navigation_code is null and origin.use_navigation_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_navigation_code_ds END,
    maintenance_last_ds = CASE WHEN (cabd.maintenance_last is null and origin.maintenance_last IS NOT NULL) THEN origin.data_source ELSE cabdsource.maintenance_last_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year is null and origin.construction_year IS NOT NULL) THEN origin.data_source ELSE cabdsource.construction_year_ds END,
    storage_capacity_mcm_ds = CASE WHEN (cabd.storage_capacity_mcm is null and origin.storage_capacity_mcm IS NOT NULL) THEN origin.data_source ELSE cabdsource.storage_capacity_mcm_ds END,
    reservoir_present_ds = CASE WHEN (cabd.reservoir_present is null and origin.reservoir_present IS NOT NULL) THEN origin.data_source ELSE cabdsource.reservoir_present_ds END,
    generating_capacity_mwh_ds = CASE WHEN (cabd.generating_capacity_mwh is null and origin.generating_capacity_mwh IS NOT NULL) THEN origin.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.dam_name_en_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.waterbody_name_en_dsfid END,
    construction_type_code_dsfid = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.construction_type_code_dsfid END,
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.height_m_dsfid END,
    length_m_dsfid = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.length_m_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code is null and origin.use_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_code_dsfid END,
    use_irrigation_code_dsfid = CASE WHEN (cabd.use_irrigation_code is null and origin.use_irrigation_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_irrigation_code_dsfid END,
    use_electricity_code_dsfid = CASE WHEN (cabd.use_electricity_code is null and origin.use_electricity_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_electricity_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN (cabd.use_supply_code is null and origin.use_supply_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_supply_code_dsfid END,
    use_floodcontrol_code_dsfid = CASE WHEN (cabd.use_floodcontrol_code is null and origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_floodcontrol_code_dsfid END,
    use_recreation_code_dsfid = CASE WHEN (cabd.use_recreation_code is null and origin.use_recreation_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_recreation_code_dsfid END,
    use_navigation_code_dsfid = CASE WHEN (cabd.use_navigation_code is null and origin.use_navigation_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_navigation_code_dsfid END,
    maintenance_last_dsfid = CASE WHEN (cabd.maintenance_last is null and origin.maintenance_last IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.maintenance_last_dsfid END,
    construction_year_dsfid = CASE WHEN (cabd.construction_year is null and origin.construction_year IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.construction_year_dsfid END,
    storage_capacity_mcm_dsfid = CASE WHEN (cabd.storage_capacity_mcm is null and origin.storage_capacity_mcm IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.storage_capacity_mcm_dsfid END,
    reservoir_present_dsfid = CASE WHEN (cabd.reservoir_present is null and origin.reservoir_present IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.reservoir_present_dsfid END,
    generating_capacity_mwh_dsfid = CASE WHEN (cabd.generating_capacity_mwh is null and origin.generating_capacity_mwh IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.generating_capacity_mwh_dsfid END     
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS origin
WHERE
    cabdsource.cabd_id = origin.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabdsource.dam_name_en END,
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.waterbody_name_en ELSE cabdsource.waterbody_name_en END,
    construction_type_code = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.construction_type_code ELSE cabdsource.construction_type_code END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.height_m ELSE cabdsource.height_m END,
    length_m = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.length_m ELSE cabdsource.length_m END,    
    use_code = CASE WHEN (cabd.use_code is null and origin.use_code IS NOT NULL) THEN origin.use_code ELSE cabdsource.use_code END,
    use_irrigation_code = CASE WHEN (cabd.use_irrigation_code is null and origin.use_irrigation_code IS NOT NULL) THEN origin.use_irrigation_code ELSE cabdsource.use_irrigation_code END,
    use_electricity_code = CASE WHEN (cabd.use_electricity_code is null and origin.use_electricity_code IS NOT NULL) THEN origin.use_electricity_code ELSE cabdsource.use_electricity_code END,
    use_supply_code = CASE WHEN (cabd.use_supply_code is null and origin.use_supply_code IS NOT NULL) THEN origin.use_supply_code ELSE cabdsource.use_supply_code END,
    use_floodcontrol_code = CASE WHEN (cabd.use_floodcontrol_code is null and origin.use_floodcontrol_code IS NOT NULL) THEN origin.use_floodcontrol_code ELSE cabdsource.use_floodcontrol_code END,
    use_recreation_code = CASE WHEN (cabd.use_recreation_code is null and origin.use_recreation_code IS NOT NULL) THEN origin.use_recreation_code ELSE cabdsource.use_recreation_code END,
    use_navigation_code = CASE WHEN (cabd.use_navigation_code is null and origin.use_navigation_code IS NOT NULL) THEN origin.use_navigation_code ELSE cabdsource.use_navigation_code END,
    maintenance_last = CASE WHEN (cabd.maintenance_last is null and origin.maintenance_last IS NOT NULL) THEN origin.maintenance_last ELSE cabdsource.maintenance_last END,
    construction_year = CASE WHEN (cabd.construction_year is null and origin.construction_year IS NOT NULL) THEN origin.construction_year ELSE cabdsource.construction_year END,
    storage_capacity_mcm = CASE WHEN (cabd.storage_capacity_mcm is null and origin.storage_capacity_mcm IS NOT NULL) THEN origin.storage_capacity_mcm ELSE cabdsource.storage_capacity_mcm END,
    reservoir_present = CASE WHEN (cabd.reservoir_present is null and origin.reservoir_present IS NOT NULL) THEN origin.reservoir_present ELSE cabdsource.reservoir_present END,
    generating_capacity_mwh = CASE WHEN (cabd.generating_capacity_mwh is null and origin.generating_capacity_mwh IS NOT NULL) THEN origin.generating_capacity_mwh ELSE cabdsource.generating_capacity_mwh END        
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)