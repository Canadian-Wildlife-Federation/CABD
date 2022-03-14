import LOAD_main as main

script = main.LoadingScript("wrispublicdams")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = objectid;
UPDATE {script.sourceTable} SET data_source = 'eb1d2553-7535-4b8c-99c3-06487214ccae';
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
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
        alternate_dam_name,
        dam_owner,
        dam_type,
        dam_function,
        commissioned_year,
        dam_height_m,
        crest_length_m,
        dam_operation_code,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN function_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN operating_status_code int2;

UPDATE {script.damWorkingTable} SET dam_name_en = 
    CASE
    WHEN dam_name IS NOT NULL THEN dam_name
    WHEN dam_name IS NULL AND alternate_dam_name IS NOT NULL THEN alternate_dam_name
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET "owner" = dam_owner; 

--TO DO: add case statements to handle ownership_type_code assignments
UPDATE {script.damWorkingTable} SET ownership_type_code =
    CASE
    WHEN 
        (regexp_match("owner", '(?i)(Ducks Unlimited)|(Nature Conservan)|(Nature Trust)|(Greenbelt)') IS NOT NULL)
        OR "owner" IN('NORTHWEST WILDLIFE PRESERVATION SOCIETY', 'SAANICH PENINSULA HOSPITAL FOUNDATION')
        THEN 1
    WHEN "owner" IN(
        'CROWN LAND OPPORTUNITIES AND RESTORATION',
        'FISHERIES & OCEANS CANADA',
        'INDIAN & NORTHERN AFFAIRS CANADA',
        'PARKS CANADA AGENCY',
        'VICTORIA AIRPORT AUTHORITY'
        )
        THEN 2
    WHEN
        (regexp_match("owner", '(?i)(City of)|(Town of)|(Village of)|(Regional)|(District of)|(Irrigation)|(Improvement)|(Waterworks Dist)|(Water District)|(Metro Vancouver)|(Municipality)') IS NOT NULL)
        THEN 3
    WHEN
        (regexp_match("owner", '(?i)(Minist)|(Protected)|(Fish & Wild)|(Branch)|(Division)|(Section)|(Power Auth)') IS NOT NULL)
        OR "owner" IN(
            'BC ENVIRONMENT',
            'FOREST DISTRICT - VANDERHOOF',
            'LAND & WATER BC',
            'NATURAL RESOURCE DISTRICT - CASCADES',
            'TRANSPORTATION & INFRASTRUCTURE MIN OF'
            )
        THEN 5
    WHEN
        (regexp_match("owner", '(?i)(Cattle)|(Coal)|(Pulp)|(Resort)|(Enter)|(Develop)|(Farms)|(Golf)|(Holdings)|(Corpor)|(Ltd)|(Limited)|(Corp)\.?') IS NOT NULL)
        OR (regexp_match("owner", '(?i)(Invest)|(Energy)|(Estate)|(Properties)|(Projec)|(Homes)|(Mines)|(Equity Mine)|(Utilit)|(Canadian Nat)|(Pacific)|(Ranch)|(Ranches)|(Inc)\.*?') IS NOT NULL)
        OR ("owner" ILIKE '%Resource%' AND "owner" != 'NATURAL RESOURCE DISTRICT - CASCADES')
        OR "owner" IN(
            'SOUTHVIEW PROPERTY MGMT (APPLICATION)',
            'ROCK RIDGE CANYON, A YOUNG LIFE OF CANAD',
            'CAYOOSE CREEK POWER LP',
            'GRANBY CONSOLIDATED MINING',
            'NEW GOLD INC. (DO NOT USE - USE 63341)',
            'THE CRESTON VALLEY ROD & GUN CLUB',
            'ARROWSMITH WATER SERVICE'
            )
        THEN 4

    WHEN
        ("owner" IS NOT NULL
            AND "owner" != ''
            AND "owner" NOT IN('DO NOT CHANGE TO BE DETERMINED AND', 'NO DAM OWNER OR LICENSEE')
        )
        THEN 6
    ELSE NULL END;

UPDATE {script.damWorkingTable} SET construction_type_code =
    CASE 
    WHEN dam_type = 'Concrete–arch' THEN 1
    WHEN dam_type = 'Concrete–slab/buttress' THEN 2
    WHEN dam_type = 'Earthfill' THEN 3
    WHEN dam_type ILIKE 'Embankment%' THEN 3
    WHEN dam_type = 'Concrete–gravity' THEN 4
    WHEN dam_type = 'Rockfill' THEN 6
    WHEN dam_type = 'Steel' THEN 7
    WHEN dam_type = 'Log crib' THEN 8
    WHEN dam_type IN ('Combination', 'Concrete', 'Other') THEN 10
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET function_code = 
    CASE
    WHEN dam_function = 'SADDLE' THEN 6
    ELSE NULL END; 
UPDATE {script.damWorkingTable} SET construction_year = commissioned_year::numeric; 
UPDATE {script.damWorkingTable} SET height_m = dam_height_m; 
UPDATE {script.damWorkingTable} SET length_m = crest_length_m; 
UPDATE {script.damWorkingTable} SET operating_status_code =
    CASE 
    WHEN dam_operation_code = 'Abandoned' THEN 1
    WHEN dam_operation_code = 'Active' THEN 2
    WHEN dam_operation_code = 'Removed' THEN 3
    WHEN dam_operation_code IN ('Deactivated', 'Decommissioned') THEN 4
    WHEN dam_operation_code IN ('Application', 'Not Constructed', 'Breached (Failed)') THEN 5
    ELSE NULL END; 

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN dam_name,
    DROP COLUMN alternate_dam_name,
    DROP COLUMN dam_owner,
    DROP COLUMN dam_type,
    DROP COLUMN dam_function,
    DROP COLUMN commissioned_year,
    DROP COLUMN dam_height_m,
    DROP COLUMN crest_length_m,
    DROP COLUMN dam_operation_code;

"""
script.do_work(query)
