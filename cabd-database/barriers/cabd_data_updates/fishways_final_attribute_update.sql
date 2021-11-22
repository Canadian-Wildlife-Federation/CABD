--This script should be run as the last step after populating attributes.
--This script updates fishways only.

BEGIN;
--Various spatial joins/queries to populate fields
--TO DO: Change original_point to EPSG:4326, populate lat/long fields, drop column geometry
UPDATE featurecopy.fishways AS fish SET original_point = ST_Transform(fish.original_point, 4326);
UPDATE featurecopy.fishways AS fish SET longitude = ST_X(fish.snapped_point);
UPDATE featurecopy.fishways AS fish SET latitude = ST_Y(fish.snapped_point);
UPDATE featurecopy.fishways AS fish DROP INDEX fishways_geometry_geom_idx;
UPDATE featurecopy.fishways AS fish DROP COLUMN geometry;
UPDATE featurecopy.fishways AS fish REINDEX INDEX fishways_idx;
UPDATE featurecopy.fishways AS fish SET province_territory_code = n.code FROM cabd.province_territory_codes AS n WHERE st_contains(n.geometry, fish.snapped_point);
UPDATE featurecopy.fishways AS fish SET nhn_workunit_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, fish.snapped_point);
UPDATE featurecopy.fishways AS fish SET sub_sub_drainage_name = n.sub_sub_drainage_area FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, fish.snapped_point);
UPDATE featurecopy.fishways AS fish SET municipality = n.csdname FROM cabd.census_subdivisions AS n WHERE st_contains(n.geometry, fish.snapped_point);

--TO DO: Add foreign table to reference ecatchment and eflowpath tables, make sure 2 lines below work
--Should waterbody name simply be overwritten here as long as we have a value from the chyf networks?
--UPDATE featurecopy.fishways AS cabd SET waterbody_name_en = c.name FROM fpoutput.ecatchment AS c WHERE st_contains(c.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;
--UPDATE featurecopy.fishways AS cabd SET waterbody_name_en = f.name FROM fpoutput.eflowpath AS f WHERE st_contains(f.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;

COMMIT;

--associate fishways with dams
BEGIN;

UPDATE featurecopy.fishways SET dam_id = foo.dam_id FROM (SELECT DISTINCT ON (cabd_id) cabd_id, dam_id
    FROM (
        SELECT
            fish.cabd_id,
            dam.cabd_id as dam_id
       FROM
            featurecopy.fishways AS fish,
            featurecopy.dams AS dams,
            ST_Distance(fish.original_point, dams.original_point) as distance
       WHERE
            ST_Distance(fish.original_point, dams.original_point) < 0.01 and
            ST_Distance(fish.original_point::geography,
            dam.original_point::geography) < 150
       ORDER BY cabd_id, distance
   ) bar
   ) foo
   WHERE foo.cabd_id = featurecopy.fishways.cabd_id;

--update dams attribute source table
UPDATE
    featurecopy.dams_attribute_source AS cabdsource
SET    
    up_passage_type_code_ds = CASE WHEN (cabd.up_passage_type_code = 9 AND fish.fishpass_type_code IS NOT NULL) THEN fishsource.fishpass_type_code_ds ELSE cabdsource.up_passage_type_code_ds END, 

    up_passage_type_code_dsfid = CASE WHEN (cabd.up_passage_type_code = 9 AND fish.fishpass_type_code IS NOT NULL) THEN fish.cabd_id::varchar ELSE cabdsource.up_passage_type_code_dsfid END    
FROM
    featurecopy.dams AS cabd,
    featurecopy.fishways AS fish,
    featurecopy.fishways_attribute_source AS fishsource
WHERE
    cabdsource.cabd_id = fish.dam_id AND cabd.cabd_id = cabdsource.cabd_id;

--assign an upstream passage type based on presence of a fishway
--TO DO: need to consider dams with multiple fishways - map anything with an eel ladder to reflect this in comments field
UPDATE 
    featurecopy.dams AS cabd
SET 
    up_passage_type_code = CASE WHEN (cabd.up_passage_type_code = 9 AND fish.fishpass_type_code IS NOT NULL) THEN fish.fishpass_type_code ELSE cabd.up_passage_type_code END
FROM
    featurecopy.fishways AS fish
WHERE 
    cabd.cabd_id = fish.dam_id;

COMMIT;

--TO DO: Change null values to "unknown" for user benefit - are there any cases where we want to do this for fishways?
BEGIN;

UPDATE featurecopy.fishways SET ...

COMMIT;

--TO DO: Establish criteria for fishways completeness level code
BEGIN;

UPDATE featurecopy.fishways SET complete_level_code = 
    CASE 
    WHEN
    WHEN
    WHEN
    ELSE 1 END;

COMMIT;