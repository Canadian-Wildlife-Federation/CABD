import LOAD_main as main

script = main.LoadingScript("bcflnrord_kml_pubdams")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = objectid;
UPDATE {script.sourceTable} SET data_source = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}');
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
        commissioned_year,
        dam_owner,
        dam_operation_code,
        dam_type,
        height_m,
        crest_length_m,
        dam_function,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} RENAME COLUMN height_m TO height_m_orig;
ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN function_code int2;


UPDATE {script.damWorkingTable} SET dam_name_en = dam_name;
UPDATE {script.damWorkingTable} SET construction_year = commissioned_year::numeric;
UPDATE {script.damWorkingTable} SET "owner" = dam_owner;

--TO DO: test case statements to handle ownership_type_code assignments
UPDATE {script.damWorkingTable} SET ownership_type_code =
    CASE
    WHEN 
        (regexp_match("owner", '(?i)(Ducks Unlimited)|(Nature Conservan)|(Nature Trust)') IS NOT NULL)
        OR "owner" IN('NORTHWEST WILDLIFE P', 'SAANICH PENINSULA HO')
        THEN 1
    WHEN "owner" IN(
        'CROWN LAND OPPORTUNI',
        'FISHERIES &amp; OCEA',
        'INDIAN &amp; NORTHER',
        'VICTORIA AIRPORT AUT'
        )
        THEN 2
    WHEN
        (regexp_match("owner", '(?i)(City of)|(Town of)|(Village of)|(District of)|(Regi)|(Dist)|(Irrig)|(Impr)|(Waterwo)|(Vancouver)') IS NOT NULL)
        OR "owner" IN(
            'MISSEZULA LAKE WATER',
            'BOWEN ISLAND MUNICIP',
            'COMOX-STRATHCONA REG',
            'NORTH SALTSPRING WAT',
            'SOUTH EAST KELOWNA I',
            'SOUTH PENDER HARBOUR',
            'WILDERNESS MOUNTAIN'
            )
        THEN 3
    WHEN
        (regexp_match("owner", '(?i)(Cattl)|(Coal)|(Pulp)|(Reso)|(Enter)|(Devel)|(Farm)|(Golf)|(Holdings)|(BC Ltd)|(Ltd)|(Inc)\.*$') IS NOT NULL)
        OR (regexp_match("owner", '(?i)(Invest)|(Ener)|(Esta)|(Projec)|(Homes)|(Mines)|(Utilit)|(Canadian Nat)|(Pacific)|(Forest Pro)') IS NOT NULL)
        OR ("owner" ILIKE '%Ranc%' AND "owner" != 'WILDLIFE BRANCH')
        OR ("owner" ILIKE '%Power%' AND "owner" != 'BC HYDRO &amp; POWER')
        OR ("owner" ILIKE '%Prop%' AND "owner" != 'RUTH LAKE PROPERTY O')
        OR ("owner" ILIKE '%Hydro%' AND "owner" != 'BC HYDRO &amp; POWER')
        OR ("owner" ILIKE '%Corp%' AND "owner" != 'CORP OF THE DISTRICT')
        OR "owner" IN(
            'ANDY MEINTS CONTRACT',
            'B C WILDERNESS TOURS',
            'BEDWELL HARBOUR HOTE',
            'BLACKDOME EXPLORATIO',
            'BLANEY TERMINALS LIM',
            'BURNS LAKE SPECIALTY',
            'CASTLEGAR MACHINE &a',
            'CRESTON VALLEY ROD &',
            'DALE ARDEN LOG HAULI',
            'DOUBLE DIAMOND FURLO',
            'FAIRMONT HOT SPRINGS',
            'GOLDEN HILLS STRATA',
            'GOLDEN WATER HOLDING',
            'GRANBY CONSOLIDATED',
            'GUARANTEE CONSTRUCTI',
            'INDUSTRIAL FORESTRY',
            'INNERGEX RENEWABLE E',
            'INSIDE PASSAGE MARIN',
            'JOCKO CREEK LAND &am',
            'MOUNT LEHMAN FRUIT G',
            'MT WASHINGTON SKI RE',
            'NEUCEL SPECIALTY CEL',
            'NOR AM AQUACULTURES',
            'NORTH AMERICAN METAL',
            'NYRSTAR MYRA FALLS L',
            'PLACER DOME (CLA) -',
            'PRT GROWING SERVICES',
            'RIVERVIEW FEED LOT (',
            'ROCK RIDGE CANYON, A',
            'SALT SPRING RECREATI',
            'SAM GEOFFREY D &amp;',
            'SARANAGATI VILLAGE H',
            'SECREST MOUNTAIN VIN',
            'SHAY''S LIVESTOCK CON',
            'STRATHCONA PARK LODG',
            'TECK HIGHLAND VALLEY',
            'THOMSON LAND &amp; C',
            'UNITED KENO HILL MIN',
            'WESTERN DELTA LANDS',
            'WIG IMMOBILIEN A.G.'
            )
        THEN 4
    WHEN
        (regexp_match("owner", '(?i)(Minist)|(Protected)') IS NOT NULL)
        OR "owner" IN(
        'BC ENVIRONMENT',
        'BC HYDRO &amp; POWER',
        'ECOSYSTEMS SECTION',
        'FISH &amp; WILDLIFE',
        'FISH, WILDLIFE AND H',
        'FLOODPLAIN MANAGEMEN',
        'LAND &amp; WATER BC',
        'TRANSPORTATION &amp;',
        'WATER STEWARDSHIP DI',
        'WILDLIFE BRANCH'
        )
        THEN 5
    WHEN
        ("owner" IS NOT NULL
        AND "owner" != ''
        AND "owner" NOT IN('DETERMINED TO BE', 'NO DAM OWNER OR LICE')
        )
        THEN 6
    ELSE NULL END;

UPDATE {script.damWorkingTable} SET operating_status_code =
    CASE 
    WHEN dam_operation_code = 'Abandoned' THEN 1
    WHEN dam_operation_code = 'Active' THEN 2
    WHEN dam_operation_code IN ('Decommissioned', 'Removed') THEN 3
    WHEN dam_operation_code = 'Deactivated' THEN 4
    WHEN dam_operation_code IN ('Application', 'Not Constructed', 'Breached (Failed)') THEN 5
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET construction_type_code =
    CASE 
    WHEN dam_type = 'Concrete Arch' THEN 1
    WHEN dam_type = 'Buttress' THEN 2
    WHEN dam_type = 'Earthfill' THEN 3
    WHEN dam_type = 'Concrete Gravity' THEN 4
    WHEN dam_type = 'Rockfill' THEN 6
    WHEN dam_type = 'Steel' THEN 7
    WHEN dam_type = 'Log Crib' THEN 8
    WHEN dam_type IN ('Concrete', 'Dugout/Pond', 'Other') THEN 10
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET height_m = height_m_orig::float4;
UPDATE {script.damWorkingTable} SET length_m = crest_length_m::float4;
UPDATE {script.damWorkingTable} SET function_code = 
    CASE
    WHEN dam_function = 'SADDLE' THEN 6
    ELSE NULL END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN dam_name,
    DROP COLUMN commissioned_year,
    DROP COLUMN dam_owner,
    DROP COLUMN dam_operation_code,
    DROP COLUMN dam_type,
    DROP COLUMN height_m_orig,
    DROP COLUMN crest_length_m,
    DROP COLUMN dam_function;

"""
script.do_work(query)
