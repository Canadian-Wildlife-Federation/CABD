--This script should be run as the last step after populating attributes.
--This script updates DAMS only.
--RUN THIS SCRIPT AFTER FISHWAYS FINAL ATTRIBUTE UPDATE

--Ensure various fields are populated with most up-to-date info after mapping from multiple sources
-- BEGIN;

UPDATE
    dams.dams
SET 
    size_class_code =
    CASE
    WHEN "height_m" < 5 THEN (SELECT code FROM dams.size_codes WHERE name_en = 'Small')
    WHEN "height_m" >= 5 AND  "height_m" < 15 THEN (SELECT code FROM dams.size_codes WHERE name_en = 'Medium')
    WHEN "height_m" >= 15 THEN (SELECT code FROM dams.size_codes WHERE name_en = 'Large')
    ELSE (SELECT code FROM dams.size_codes WHERE name_en = 'Unknown') END,

    reservoir_present =
    CASE
    WHEN 
        reservoir_area_skm IS NOT NULL 
        OR reservoir_depth_m IS NOT NULL
        OR reservoir_name_en IS NOT NULL
        OR reservoir_name_fr IS NOT NULL
        THEN TRUE
    ELSE reservoir_present END;

UPDATE dams.dams AS cabd SET passability_status_code = 
    (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Partial Barrier')
    FROM fishways.fishways AS f WHERE f.dam_id = cabd.cabd_id;
UPDATE dams.dams SET passability_status_code = 
    (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Barrier') WHERE passability_status_code IS NULL;

--Various spatial joins/queries to populate fields
UPDATE dams.dams AS dams SET province_territory_code = n.code FROM cabd.province_territory_codes AS n WHERE st_contains(n.geometry, dams.snapped_point);
UPDATE dams.dams SET province_territory_code = 'us' WHERE province_territory_code IS NULL;
UPDATE dams.dams AS dams SET nhn_watershed_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, dams.snapped_point);
UPDATE dams.dams AS dams SET municipality = n.csdname FROM cabd.census_subdivisions AS n WHERE st_contains(n.geometry, dams.snapped_point);

--TO DO: Add foreign table to reference ecatchment and eflowpath tables, make sure 2 lines below work
--Should waterbody name simply be overwritten here as long as we have a value from the chyf networks?
--UPDATE dams.dams AS cabd SET waterbody_name_en = c.name FROM fpoutput.ecatchment AS c WHERE st_contains(c.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;
--UPDATE dams.dams AS cabd SET waterbody_name_en = f.name FROM fpoutput.eflowpath AS f WHERE st_contains(f.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;

-- COMMIT;

--Change null values to "unknown" for user benefit
-- BEGIN;

UPDATE dams.dams SET structure_type_code = (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Unknown') 
    WHERE structure_type_code IS NULL;
UPDATE dams.dams SET ownership_type_code = (SELECT code FROM dams.barrier_ownership_type_codes WHERE name_en = 'Unknown')
    WHERE ownership_type_code IS NULL;
UPDATE dams.dams SET use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown')
    WHERE use_code IS NULL;
UPDATE dams.dams SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
    WHERE function_code IS NULL;
UPDATE dams.dams SET turbine_type_code = (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Unknown')
    WHERE turbine_type_code IS NULL AND (use_code = 2 OR use_electricity_code IS NOT NULL);
UPDATE dams.dams SET operating_status_code = (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown')
    WHERE operating_status_code IS NULL;

-- COMMIT;

--Set completeness level code
-- BEGIN;

UPDATE dams.dams SET complete_level_code = 
    CASE 
    WHEN
        ((dam_name_en IS NOT NULL OR dam_name_fr IS NOT NULL)
        AND (waterbody_name_en IS NOT NULL OR waterbody_name_fr IS NOT NULL) 
        AND operating_status_code <> (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown')
        AND use_code <> (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown')
        AND function_code <> (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
        AND structure_type_code <> (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Unknown') 
        AND construction_year IS NOT NULL 
        AND height_m IS NOT NULL
        AND ("owner" IS NOT NULL OR ownership_type_code <> ((SELECT code FROM dams.barrier_ownership_type_codes WHERE name_en = 'Unknown')))
        AND condition_code IS NOT NULL 
        AND reservoir_present IS NOT NULL 
        AND expected_life IS NOT NULL
        AND up_passage_type_code <> (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown') 
        AND down_passage_route_code IS NOT NULL
        AND length_m IS NOT NULL)
        THEN (SELECT code FROM dams.dam_complete_level_codes WHERE name_en = 'Complete')

    WHEN
        ((dam_name_en IS NOT NULL OR dam_name_fr IS NOT NULL)
        AND (waterbody_name_en IS NOT NULL OR waterbody_name_fr IS NOT NULL))
        AND operating_status_code <> (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown')
        AND use_code <> (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown')
        AND function_code <> (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
        AND construction_year IS NOT NULL 
        AND height_m IS NOT NULL
        AND ("owner" IS NOT NULL OR ownership_type_code <> (SELECT code FROM dams.barrier_ownership_type_codes WHERE name_en = 'Unknown'))
        THEN (SELECT code FROM dams.dam_complete_level_codes WHERE name_en = 'Moderate')

    WHEN
        (((dam_name_en IS NULL AND dam_name_fr IS NULL)
        OR (waterbody_name_en IS NULL AND waterbody_name_fr IS NULL)
        OR operating_status_code = (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown')
        OR height_m IS NULL 
        OR use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown')) 
        AND (function_code <> (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
        OR structure_type_code <> (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Unknown') 
        OR construction_year IS NOT NULL 
        OR ("owner" IS NOT NULL OR ownership_type_code <> (SELECT code FROM dams.barrier_ownership_type_codes WHERE name_en = 'Unknown'))))
        THEN (SELECT code FROM dams.dam_complete_level_codes WHERE name_en = 'Minimal')

    ELSE (SELECT code FROM dams.dam_complete_level_codes WHERE name_en = 'Unverified') END;

-- COMMIT;