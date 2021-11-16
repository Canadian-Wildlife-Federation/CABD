--This script should be run as the last step after populating attributes.
--This script updates DAMS only.

--Ensure various fields are populated with most up-to-date info after mapping from multiple sources
BEGIN;

UPDATE
    featurecopy.dams
SET 
    size_class_code =
    CASE
    WHEN "height_m" < 5 THEN 1
    WHEN "height_m" >= 5 AND  "height_m" < 15 THEN 2
    WHEN "height_m" >= 15 THEN 3
    ELSE 4 END,

    use_irrigation_code = 
    CASE
    WHEN use_code = 1 THEN 1
    ELSE use_irrigation_code END,

    use_electricity_code = 
    CASE
    WHEN use_code = 2 THEN 1
    ELSE use_electricity_code END,

    use_supply_code = 
    CASE
    WHEN use_code = 3 THEN 1
    ELSE use_supply_code END,

    use_floodcontrol_code = 
    CASE
    WHEN use_code = 4 THEN 1
    ELSE use_floodcontrol_code END,

    use_recreation_code =
    CASE
    WHEN use_code = 5 THEN 1
    ELSE use_recreation_code END,

    use_navigation_code =
    CASE
    WHEN use_code = 6 THEN 1
    ELSE use_navigation_code END,

    use_fish_code =
    CASE
    WHEN use_code = 7 THEN 1
    ELSE use_fish_code END,

    use_pollution_code =
    CASE
    WHEN use_code = 8 THEN 1
    ELSE use_pollution_code END,

    use_invasivespecies_code = 
    CASE
    WHEN use_code = 9 THEN 1
    ELSE use_invasivespecies_code END,

    use_other_code =
    CASE
    WHEN use_code = 10 THEN 1
    ELSE use_other_code END,

    reservoir_present =
    CASE
    WHEN reservoir_area_skm IS NOT NULL OR reservoir_depth_m IS NOT NULL THEN TRUE
    ELSE reservoir_present END;

UPDATE featurecopy.dams AS cabd SET passability_status_code = 2 FROM fishways.fishways AS f WHERE cabd.cabd_id = f.dam_id;
UPDATE featurecopy.dams SET passability_status_code = 1 WHERE passability_status_code IS NULL;

--Various spatial joins to populate fields
UPDATE featurecopy.dams AS dc SET province_territory_code = n.code FROM cabd.province_territory_codes AS n WHERE st_contains(n.geometry, dc.snapped_point);
UPDATE featurecopy.dams AS dc SET nhn_workunit_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, dc.snapped_point);
UPDATE featurecopy.dams AS dc SET sub_sub_drainage_name = n.sub_sub_drainage_area FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, dc.snapped_point);
UPDATE featurecopy.dams AS dc SET municipality = n.csdname FROM cabd.census_subdivisions AS n WHERE st_contains(n.geometry, dc.snapped_point);

--TO DO: Add foreign table to reference ecatchment and eflowpath tables, make sure 2 lines below work
--Should waterbody name simply be overwritten here as long as we have a value from the chyf networks?
--UPDATE featurecopy.dams AS cabd SET waterbody_name_en = c.name FROM fpoutput.ecatchment AS c WHERE st_contains(c.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;
--UPDATE featurecopy.dams AS cabd SET waterbody_name_en = f.name FROM fpoutput.eflowpath AS f WHERE st_contains(f.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;

COMMIT;

--Change null values to "unknown" for user benefit
BEGIN;

UPDATE featurecopy.dams SET construction_type_code = 9 WHERE construction_type_code IS NULL;
UPDATE featurecopy.dams SET ownership_type_code = 7 WHERE ownership_type_code IS NULL;
UPDATE featurecopy.dams SET use_code = 11 WHERE use_code IS NULL;
UPDATE featurecopy.dams SET function_code = 13 WHERE function_code IS NULL;
UPDATE featurecopy.dams SET turbine_type_code = 5 WHERE turbine_type_code IS NULL;
UPDATE featurecopy.dams SET operating_status_code = 5 WHERE operating_status_code IS NULL;
UPDATE featurecopy.dams SET up_passage_type_code = 9 WHERE up_passage_type_code IS NULL;

COMMIT;

--Set completeness level code
BEGIN;

UPDATE featurecopy.dams SET complete_level_code = 
    CASE 
    WHEN
        ((dam_name_en IS NOT NULL OR dam_name_fr IS NOT NULL)
        AND (waterbody_name_en IS NOT NULL OR waterbody_name_fr IS NOT NULL) 
        AND operating_status_code <> 5
        AND use_code <> 11
        AND function_code <> 13
        AND construction_type_code <> 9
        AND construction_year IS NOT NULL 
        AND height_m IS NOT NULL
        AND ("owner" IS NOT NULL OR ownership_type_code <> 7)
        AND condition_code IS NOT NULL 
        AND reservoir_present IS NOT NULL 
        AND expected_life IS NOT NULL
        AND up_passage_type_code <> 9
        AND down_passage_route_code IS NOT NULL
        AND length_m IS NOT NULL)
        THEN 4
    WHEN
        ((dam_name_en IS NOT NULL OR dam_name_fr IS NOT NULL)
        AND (waterbody_name_en IS NOT NULL OR waterbody_name_fr IS NOT NULL) 
        AND operating_status_code <> 5
        AND use_code <> 11
        AND function_code <> 13
        AND construction_year IS NOT NULL 
        AND height_m IS NOT NULL
        AND ("owner" IS NOT NULL OR ownership_type_code <> 7))
        THEN 3
    WHEN
        (((dam_name_en IS NULL AND dam_name_fr IS NULL)
        OR (waterbody_name_en IS NULL AND waterbody_name_fr IS NULL)
        OR operating_status_code = 5
        OR height_m IS NULL 
        OR use_code = 11) 
        AND (function_code <> 13 
        OR construction_type_code <> 9 
        OR construction_year IS NOT NULL 
        OR ("owner" IS NOT NULL OR ownership_type_code <> 7)))
        THEN 2
    ELSE 1 END;

COMMIT;