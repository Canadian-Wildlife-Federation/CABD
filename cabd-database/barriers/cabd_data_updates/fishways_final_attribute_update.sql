--This script should be run as the last step after populating attributes.
--This script updates FISHWAYS only.
--RUN THIS SCRIPT BEFORE DAMS FINAL ATTRIBUTE UPDATE

BEGIN;
--Various spatial joins/queries to populate fields
UPDATE featurecopy.fishways AS fish SET province_territory_code = n.code FROM cabd.province_territory_codes AS n WHERE st_contains(n.geometry, fish.original_point);
UPDATE featurecopy.fishways SET province_territory_code = 'us' WHERE province_territory_code IS NULL;
UPDATE featurecopy.fishways AS fish SET nhn_watershed_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, fish.original_point);
UPDATE featurecopy.fishways AS fish SET municipality = n.csdname FROM cabd.census_subdivisions AS n WHERE st_contains(n.geometry, fish.original_point);

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
            dams.cabd_id as dam_id
       FROM
            featurecopy.fishways AS fish,
            featurecopy.dams AS dams,
            ST_Distance(fish.original_point, dams.snapped_point) as distance
       WHERE
            ST_Distance(fish.original_point, dams.snapped_point) < 0.01 and
            ST_Distance(fish.original_point::geography,
            dams.snapped_point::geography) < 200
       ORDER BY cabd_id, distance
   ) bar
   ) foo
   WHERE foo.cabd_id = featurecopy.fishways.cabd_id;

--update dams attribute source and feature source tables
UPDATE
    featurecopy.dams_attribute_source AS cabdsource
SET    
    up_passage_type_code_ds = CASE WHEN ((cabd.up_passage_type_code IS NULL OR cabd.up_passage_type_code = 9) AND fish.fishpass_type_code IS NOT NULL) THEN fishsource.fishpass_type_code_ds ELSE cabdsource.up_passage_type_code_ds END
FROM
    featurecopy.dams AS cabd,
    featurecopy.fishways AS fish,
    featurecopy.fishways_attribute_source AS fishsource
WHERE
    cabdsource.cabd_id = fish.dam_id AND cabd.cabd_id = cabdsource.cabd_id;

INSERT INTO featurecopy.dams_feature_source (cabd_id, datasource_id, datasource_feature_id)
SELECT dams.cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'cwf_canfish'), fish.data_source_id
FROM
	featurecopy.dams AS dams,
    featurecopy.fishways AS fish
WHERE
    dams.cabd_id = fish.dam_id
    AND fish.data_source_text = 'cwf_canfish'
ON CONFLICT DO NOTHING;

--assign an upstream passage type based on presence of a fishway
UPDATE 
    featurecopy.dams AS cabd
SET 
    up_passage_type_code = CASE WHEN ((cabd.up_passage_type_code IS NULL OR cabd.up_passage_type_code = 9) AND fish.fishpass_type_code IS NOT NULL) THEN fish.fishpass_type_code ELSE cabd.up_passage_type_code END
FROM
    featurecopy.fishways AS fish
WHERE 
    cabd.cabd_id = fish.dam_id;

COMMIT;

--Change null values to "unknown" for user benefit
BEGIN;

UPDATE featurecopy.fishways SET fishpass_type_code = 9 WHERE fishpass_type_code IS NULL;

COMMIT;