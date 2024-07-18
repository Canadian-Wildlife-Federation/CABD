--This script should be run as the last step after populating attributes.
--This script updates WATERFALLS only.

--Ensure passability status is updated with most up-to-date info after mapping from multiple sources
BEGIN;

UPDATE featurecopy.waterfalls SET passability_status_code = 
    CASE
    WHEN fall_height_m >= 5 THEN 1
    WHEN fall_height_m < 5 THEN 2
    WHEN fall_height_m IS NULL THEN 4
    ELSE NULL END
    WHERE passability_status_code IS NULL;

--Various spatial joins/queries to populate fields
UPDATE featurecopy.waterfalls AS falls SET province_territory_code = n.code FROM cabd.province_territory_codes AS n WHERE st_contains(n.geometry, falls.snapped_point);
UPDATE featurecopy.waterfalls SET province_territory_code = 'us' WHERE province_territory_code IS NULL;
UPDATE featurecopy.waterfalls AS falls SET nhn_watershed_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, falls.snapped_point);
UPDATE featurecopy.waterfalls AS falls SET municipality = n.csdname FROM cabd.census_subdivisions AS n WHERE st_contains(n.geometry, falls.snapped_point);

--TO DO: Add foreign table to reference ecatchment and eflowpath tables, make sure 2 lines below work
--Should waterbody name simply be overwritten here as long as we have a value from the chyf networks?
--UPDATE featurecopy.waterfalls AS cabd SET waterbody_name_en = c.name FROM fpoutput.ecatchment AS c WHERE st_contains(c.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;
--UPDATE featurecopy.waterfalls AS cabd SET waterbody_name_en = f.name FROM fpoutput.eflowpath AS f WHERE st_contains(f.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;

COMMIT;

--Set completeness level code
BEGIN;

UPDATE featurecopy.waterfalls SET complete_level_code = 
    CASE
    WHEN ((fall_name_en IS NOT NULL OR fall_name_fr IS NOT NULL) AND fall_height_m IS NOT NULL) THEN 3
    WHEN ((fall_name_en IS NOT NULL OR fall_name_fr IS NOT NULL) OR fall_height_m IS NOT NULL) THEN 2
    ELSE 1 END;

COMMIT;