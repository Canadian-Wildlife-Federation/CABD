--This script should be run as the last step after populating attributes.
--This script updates DAMS only.

BEGIN;

UPDATE
    export.dams_cabd
SET 
    size_class_code =
    CASE
    WHEN "height_m" < 5 THEN 1
    WHEN "height_m" >= 5 AND  "height_m" < 15 THEN 2
    WHEN "height_m" >= 15 THEN 3
    ELSE 4 END

    use_irrigation_code = 
    CASE
    WHEN use_code = 1 THEN 1
    ELSE use_irrigation_code END

    use_electricity_code = 
    CASE
    WHEN use_code = 2 THEN 1
    ELSE use_electricity_code END

    use_supply_code = 
    CASE
    WHEN use_code = 3 THEN 1
    ELSE use_supply_code END

    use_floodcontrol_code = 
    CASE
    WHEN use_code = 4 THEN 1
    ELSE use_floodcontrol_code END

    use_recreation_code =
    CASE
    WHEN use_code = 5 THEN 1
    ELSE use_recreation_code END

    use_navigation_code =
    CASE
    WHEN use_code = 6 THEN 1
    ELSE use_navigation_code END

    use_fish_code =
    CASE
    WHEN use_code = 7 THEN 1
    ELSE use_fish_code END

    use_pollution_code =
    CASE
    WHEN use_code = 8 THEN 1
    ELSE use_pollution_code END

    use_invasivespecies_code = 
    CASE
    WHEN use_code = 9 THEN 1
    ELSE use_invasivespecies_code END

    use_other_code =
    CASE
    WHEN use_code = 10 THEN 1
    ELSE use_other_code END

    reservoir_present =
    CASE
    WHEN reservoir_area_skm IS NOT NULL OR reservoir_depth_m IS NOT NULL THEN TRUE
    ELSE reservoir_present END
;

--Assign passability_status_code based on presence of fishway
--Dams without fishways will be marked as barriers, we should remove the unknown value in the public UI
--And also run a check to make sure no dams have a passability_status_code of Unknown
UPDATE public.test_cabd AS cabd SET passability_status_code = 2 FROM public.test_fishways AS f WHERE cabd.cabd_id = f.dam_id;
UPDATE public.test_cabd SET passability_status_code = 1 WHERE passability_status_code IS NULL;

--Various spatial joins to populate fields
UPDATE export.dams_cabd AS cabd SET province_territory_code = n.code FROM load.province_territory_codes AS n WHERE st_contains(n.geometry, cabd.geometry);
UPDATE export.dams_cabd AS cabd SET nhn_workunit_id = n.DATASETNAM FROM load.nhn_workunit AS n WHERE st_contains(n.polygon, cabd.geometry);
UPDATE export.dams_cabd AS cabd SET watershed_group_name = n.WSCSSDANAM FROM load.nhn_workunit AS n WHERE st_contains(n.polygon, cabd.geometry);
--Note the following line will only assign waterbody names for barriers on CHyF catchments and not flowpaths
--UPDATE export.dams_cabd AS cabd SET waterbody_name_en = c.lakeName1 FROM load.[name_of_chyf_data] AS c WHERE st_contains(c.polygon, cabd.geometry) AND waterbody_name_en IS NOT NULL;

--TO DO: spatial join for municipality based on municipal boundary polygons from Statistics Canada

--TO DO: set reservoir_present based on associated reservoir fields

--TO DO: set complete_level_code

COMMIT;

BEGIN;

--Change null values to "unknown" for user benefit
--Only for fields that support unknown value
--Size_class_code is already dealt with above to populate unknown
UPDATE export.dams_cabd SET construction_type_code = 9 WHERE construction_type_code IS NULL;
UPDATE export.dams_cabd SET ownership_type_code = 7 WHERE ownership_type_code IS NULL;
UPDATE export.dams_cabd SET use_code = 11 WHERE use_code IS NULL;
UPDATE export.dams_cabd SET function_code = 13 WHERE function_code IS NULL;
UPDATE export.dams_cabd SET turbine_type_code = 5 WHERE turbine_type_code IS NULL;
UPDATE export.dams_cabd SET operating_status_code = 5 WHERE operating_status_code IS NULL;
UPDATE export.dams_cabd SET up_passage_type_code = 9 WHERE up_passage_type_code IS NULL;

COMMIT;

