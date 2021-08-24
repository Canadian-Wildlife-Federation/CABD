--This script should be run as the last step after populating attributes.
--This script updates DAMS only.

BEGIN;

UPDATE
    damcopy.dams
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
    ELSE reservoir_present END
;

--Assign passability_status_code based on presence of fishway
--Dams without fishways will be marked as barriers, we should remove the unknown value in the public UI
--And also run a check to make sure no dams have a passability_status_code of Unknown
UPDATE damcopy.dams AS cabd SET passability_status_code = 2 FROM fishways.fishways AS f WHERE cabd.cabd_id = f.dam_id;
UPDATE damcopy.dams SET passability_status_code = 1 WHERE passability_status_code IS NULL;

--Various spatial joins to populate fields
UPDATE damcopy.dams AS dc SET province_territory_code = n.code FROM cabd.province_territory_codes AS n WHERE st_contains(n.geometry, dc.snapped_point);
UPDATE damcopy.dams AS dc SET nhn_workunit_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, dc.snapped_point);
UPDATE damcopy.dams AS dc SET sub_sub_drainage_name = n.sub_sub_drainage_area FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, dc.snapped_point);
UPDATE damcopy.dams AS dc SET municipality = n.csdname FROM cabd.census_subdivisions AS n WHERE st_contains(n.geometry, dc.snapped_point);
--Note the following line will only assign waterbody names for barriers on CHyF catchments and not flowpaths
--UPDATE damcopy.dams AS cabd SET waterbody_name_en = c.lakeName1 FROM load.[name_of_chyf_data] AS c WHERE st_contains(c.polygon, cabd.geometry) AND waterbody_name_en IS NOT NULL;

--TO DO: spatial join for municipality based on municipal boundary polygons from Statistics Canada

--TO DO: set complete_level_code

COMMIT;

BEGIN;

--Change null values to "unknown" for user benefit
--Only for fields that support unknown value
--Size_class_code is already dealt with above to populate unknown
UPDATE damcopy.dams SET construction_type_code = 9 WHERE construction_type_code IS NULL;
UPDATE damcopy.dams SET ownership_type_code = 7 WHERE ownership_type_code IS NULL;
UPDATE damcopy.dams SET use_code = 11 WHERE use_code IS NULL;
UPDATE damcopy.dams SET function_code = 13 WHERE function_code IS NULL;
UPDATE damcopy.dams SET turbine_type_code = 5 WHERE turbine_type_code IS NULL;
UPDATE damcopy.dams SET operating_status_code = 5 WHERE operating_status_code IS NULL;
UPDATE damcopy.dams SET up_passage_type_code = 9 WHERE up_passage_type_code IS NULL;

COMMIT;
