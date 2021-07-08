--This script should be run as the last step after populating attributes.
--This script updates DAMS only.
--It updates the following columns:
--passability_status_code, size_class_code, use_irrigation_code, 
--use_electricity_code, use_supply_code, use_floodcontrol_code, 
--use_recreation_code, use_navigation_code, use_fish_code, 
--use_pollution_code, use_invasivespecies_code, use_other_code, 
--up_passage_type_code, province_territory_code, nhn_workunit_id, 
--watershed_group_name, waterbody_name_en, complete_level_code.

--It also sets all null values to "Unknown" after these operations are complete.

BEGIN;

UPDATE
    export.dams_cabd
SET 
    size_class_code =
    CASE
    WHEN "height_m" < 5 THEN 1
    WHEN "height_m" >= 5 AND  "height_m" < 15 THEN 2
    WHEN "height_m" >= 15 THEN 3
    ELSE 4 END --we need code value 4 to map to unknown

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
;

UPDATE public.test_cabd AS cabd SET passability_status_code = 2 FROM public.test_fishways AS f WHERE cabd.cabd_id = f.dam_id;
UPDATE public.test_cabd SET passability_status_code = 4 WHERE passability_status_code IS NULL;
--we are using barrier or unknown for dams without fishways
--remove unknown value in public UI
--and just run a check to make sure no dams have the value "unknown"

UPDATE export.dams_cabd AS cabd SET province_territory_code = n.code FROM load.province_territory_codes AS n WHERE st_contains(n.geometry, cabd.geometry);
UPDATE export.dams_cabd AS cabd SET nhn_workunit_id = n.DATASETNAM FROM load.nhn_workunit AS n WHERE st_contains(n.polygon, cabd.geometry);
UPDATE export.dams_cabd AS cabd SET watershed_group_name = n.WSCSSDANAM FROM load.nhn_workunit AS n WHERE st_contains(n.polygon, cabd.geometry);

--how can we deal with barriers on streams and not CHyF catchments?
UPDATE export.dams_cabd AS cabd SET waterbody_name_en = c.lakeName1 FROM load.[name_of_chyf_data] AS c WHERE st_contains(c.polygon, cabd.geometry) AND waterbody_name_en IS NOT NULL;

--sequential updates to this value seem easiest to avoid having extremely long where clauses
UPDATE export.dams_cabd SET complete_level_code = 2 WHERE ([col1] IS NOT NULL AND [col2] IS NOT NULL AND [col3] IS NOT NULL) AND complete_level_code IS NULL;
UPDATE export.dams_cabd SET complete_level_code = 3 WHERE (complete_level_code = 2) AND ([col4] IS NOT NULL AND [col5] IS NOT NULL AND [col6] IS NOT NULL);
UPDATE export.dams_cabd SET complete_level_code = 4 WHERE (complete_level_code = 3) AND ([col7] IS NOT NULL AND [col8] IS NOT NULL AND [col9] IS NOT NULL);
UPDATE export.dams_cabd SET complete_level_code = 1 WHERE complete_level_code IS NULL;
--make sure we update the data dictionary accordingly
--1 - unverified
--2 - minimal
--3 - moderate
--4 - complete (we may need to rename this)

COMMIT;

BEGIN;

--TO DO: CHANGE ALL NULL VALUES TO UNKNOWN
--TEMPLATE BELOW
UPDATE export.dams_cabd SET [col1] = 'Unknown' WHERE [col1] IS NULL --text or varchar
UPDATE export.dams_cabd SET [col2] = [integer] WHERE [col2] IS NULL --int2

--For numeric, float, and date fields like construction_year, I think we should just leave as "NULL"?
--It will get confusing if we use a dummy value and I don't like the idea of changing their type.
--We can also leave out cabd_id, latitude, longitude, province_territory_code, geometry columns.
--Are there any other fields that should be allowed to remain as NULL? 
    --E.g., generating capacity should be allowed to be null as not all dams are used for hydroelectricity.

COMMIT;

