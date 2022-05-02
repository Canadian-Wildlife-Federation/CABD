import LOAD_main as main

script = main.LoadingScript("usace_nid")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = federal_id;
UPDATE {script.sourceTable} SET data_source = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}');
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--add new columns and map attributes
DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT 
        dam_name,
        other_names,
        river_stream_name,
        owner_names,
        primary_owner_type,
        state_regulatory_agency,
        federal_agency_involvement_regulatory,
        primary_purpose,
        purposes,
        year_completed,
        inspection_frequency,
        year_modified,
        primary_dam_type,
        nid_height_ft,
        dam_length_ft,
        max_discharge_ft3_s,
        spillway_type,
        surface_area_ac,
        nid_storage_acre_ft,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN provincial_compliance_status varchar(64);
ALTER TABLE {script.damWorkingTable} ADD COLUMN federal_compliance_status varchar(64);
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_other_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN function_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN spillway_capacity float8;
ALTER TABLE {script.damWorkingTable} ADD COLUMN spillway_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN storage_capacity_mcm float8;

UPDATE {script.damWorkingTable} SET dam_name_en =
    CASE
    WHEN dam_name IS NOT NULL THEN dam_name
    WHEN dam_name IS NULL AND other_names IS NOT NULL THEN other_names
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET waterbody_name_en = 
    CASE
    WHEN (river_stream_name NOT IN (
        '#NAME?',
        '(DRY WATER COURSE)',
        '.',
        '0',
        'Unkknown'
        )
        AND river_stream_name NOT ILIKE '%unkn%' 
        AND river_stream_name NOT ILIKE '%trib%')
        THEN initcap(river_stream_name)
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET "owner" = 
    CASE WHEN (owner_names NOT IN ('-','.','0', '198') OR owner_names NOT ILIKE '%unknown%') THEN initcap(owner_names)
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET ownership_type_code = 
    CASE
    WHEN primary_owner_type = 'Federal' THEN 2
    WHEN primary_owner_type = 'Local Government' THEN 3
    WHEN primary_owner_type = 'Private' THEN 4
    WHEN primary_owner_type = 'Public Utility' THEN 4
    WHEN primary_owner_type = 'State' THEN 5
    WHEN primary_owner_type = 'Tribe' THEN 6
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET provincial_compliance_status = state_regulatory_agency WHERE state_regulatory_agency != '';
UPDATE {script.damWorkingTable} SET federal_compliance_status = federal_agency_involvement_regulatory;
UPDATE {script.damWorkingTable} SET use_code =
    CASE
    WHEN primary_purpose = 'Irrigation' THEN 1
    WHEN primary_purpose = 'Hydroelectric' THEN 2
    WHEN primary_purpose = 'Water Supply' THEN 3
    WHEN primary_purpose = 'Flood Risk Reduction' THEN 4
    WHEN primary_purpose = 'Recreation' THEN 5
    WHEN primary_purpose = 'Navigation' THEN 6
    WHEN primary_purpose = 'Tailings' THEN 8
    WHEN primary_purpose IS NULL THEN NULL
    ELSE 10 END;
UPDATE {script.damWorkingTable} SET use_irrigation_code = 
    CASE
    WHEN use_code = 1 THEN 1
    WHEN purposes ILIKE '%Irrigation%' THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_electricity_code = 
    CASE
    WHEN use_code = 2 THEN 1
    WHEN purposes ILIKE '%Hydroelectric%' THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_supply_code =
    CASE
    WHEN use_code = 3 THEN 1
    WHEN purposes ILIKE '%Water Supply%' THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_floodcontrol_code = 
    CASE
    WHEN use_code = 4 THEN 1
    WHEN purposes ILIKE '%Flood Risk Reduction%' THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_recreation_code =
    CASE
    WHEN use_code = 5 THEN 1
    WHEN purposes ILIKE '%Recreation%' THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_navigation_code =
    CASE
    WHEN use_code = 6 THEN 1
    WHEN purposes ILIKE '%Navigation%' THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_pollution_code =
    CASE
    WHEN use_code = 8 THEN 1
    WHEN purposes ILIKE '%Tailings%' THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_other_code =
    CASE
    WHEN use_code = 10 THEN 1
    WHEN (regexp_match(purposes, '(?i)(Debris)|(Fish)|(Grade)|(Other)') IS NOT NULL) THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET construction_year = year_completed::numeric WHERE year_completed != '0';
UPDATE {script.damWorkingTable} SET function_code = 4 WHERE primary_purpose = 'Debris Control';
UPDATE {script.damWorkingTable} SET construction_type_code = 
    CASE
    WHEN primary_dam_type = 'Arch' THEN 1
    WHEN primary_dam_type = 'Buttress' THEN 2
    WHEN primary_dam_type = 'Earth' THEN 3
    WHEN primary_dam_type = 'Gravity' THEN 4
    WHEN primary_dam_type = 'Multi-Arch' THEN 5
    WHEN primary_dam_type = 'Rockfill' THEN 6
    WHEN primary_dam_type = 'Timber Crib' THEN 8
    WHEN primary_dam_type IN (
        'Concrete',
        'Masonry',
        'Other',
        'Roller-Compacted Concrete',
        'Stone'
        )
        THEN 10
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET height_m = ((nid_height_ft::float4)/3.2808) WHERE nid_height_ft != '0';
UPDATE {script.damWorkingTable} SET length_m = ((dam_length_ft::float4)/3.2808) WHERE dam_length_ft != '0';
UPDATE {script.damWorkingTable} SET spillway_capacity = ((max_discharge_ft3_s::float8)/35.315) WHERE max_discharge_ft3_s != '0';
UPDATE {script.damWorkingTable} SET spillway_type_code = 
    CASE
    WHEN spillway_type = 'Controlled' THEN 3
    WHEN spillway_type = 'None' THEN 5
    WHEN spillway_type = 'Uncontrolled' THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET reservoir_area_skm = ((surface_area_ac::float4)/247.11) WHERE surface_area_ac != '0';
UPDATE {script.damWorkingTable} SET storage_capacity_mcm = ((nid_storage_acre_ft::float8)*1233.4818375475/1000000) WHERE nid_storage_acre_ft != '0';

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN dam_name,
    DROP COLUMN other_names,
    DROP COLUMN river_stream_name,
    DROP COLUMN owner_names,
    DROP COLUMN primary_owner_type,
    DROP COLUMN state_regulatory_agency,
    DROP COLUMN federal_agency_involvement_regulatory,
    DROP COLUMN primary_purpose,
    DROP COLUMN purposes,
    DROP COLUMN year_completed,
    DROP COLUMN inspection_frequency,
    DROP COLUMN year_modified,
    DROP COLUMN primary_dam_type,
    DROP COLUMN nid_height_ft,
    DROP COLUMN dam_length_ft,
    DROP COLUMN max_discharge_ft3_s,
    DROP COLUMN spillway_type,
    DROP COLUMN surface_area_ac,
    DROP COLUMN nid_storage_acre_ft;

"""

script.do_work(query)