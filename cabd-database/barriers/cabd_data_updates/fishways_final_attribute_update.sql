--This script should be run as the last step after populating attributes.
--This script updates FISHWAYS only.
--RUN THIS SCRIPT BEFORE DAMS FINAL ATTRIBUTE UPDATE

--Various spatial joins/queries to populate fields
UPDATE fishways.fishways AS fish SET province_territory_code = n.code FROM cabd.province_territory_codes AS n WHERE st_contains(n.geometry, fish.original_point);
UPDATE fishways.fishways SET province_territory_code = 'us' WHERE province_territory_code IS NULL;
UPDATE fishways.fishways AS fish SET nhn_watershed_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, fish.original_point);
UPDATE fishways.fishways AS fish SET municipality = n.csdname FROM cabd.census_subdivisions AS n WHERE st_contains(n.geometry, fish.original_point);

--TO DO: Add foreign table to reference ecatchment and eflowpath tables, make sure 2 lines below work
--Should waterbody name simply be overwritten here as long as we have a value from the chyf networks?
--UPDATE fishways.fishways AS cabd SET waterbody_name_en = c.name FROM fpoutput.ecatchment AS c WHERE st_contains(c.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;
--UPDATE fishways.fishways AS cabd SET waterbody_name_en = f.name FROM fpoutput.eflowpath AS f WHERE st_contains(f.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;

--associate fishways with dams

UPDATE fishways.fishways SET dam_id = foo.dam_id FROM (SELECT DISTINCT ON (cabd_id) cabd_id, dam_id
    FROM (
        SELECT
            fish.cabd_id,
            dams.cabd_id as dam_id
       FROM
            fishways.fishways AS fish,
            dams.dams AS dams,
            ST_Distance(fish.original_point, dams.snapped_point) as distance
       WHERE
            ST_Distance(fish.original_point, dams.snapped_point) < 0.01 and
            ST_Distance(fish.original_point::geography,
            dams.snapped_point::geography) < 200
       ORDER BY cabd_id, distance
   ) bar
   ) foo
   WHERE foo.cabd_id = fishways.fishways.cabd_id;

--update dams attribute source table
WITH temp AS (
    SELECT 
        cabd.cabd_id AS dam_cabd_id,
        cabd.up_passage_type_code AS dam_up_passage,
        fish.cabd_id AS fishway_cabd_id,
        fish.dam_id AS fishway_dam_id,
        fish.fishpass_type_code AS fishway_up_passage
    FROM
        dams.dams AS cabd,
        fishways.fishways AS fish
    WHERE
        cabd.cabd_id = fish.dam_id
        AND (fish.fishpass_type_code IS NOT NULL AND fish.fishpass_type_code != (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown')
        AND fish.fishpass_type_code IS DISTINCT FROM cabd.up_passage_type_code)
)

UPDATE
    dams.dams_attribute_source AS cabdsource
SET
    up_passage_type_code_ds = fishsource.fishpass_type_code_ds
FROM
    fishways.fishways_attribute_source AS fishsource
WHERE
    cabdsource.cabd_id IN (SELECT dam_cabd_id FROM temp);

--assign an upstream passage type based on presence of a fishway
WITH temp AS (
    SELECT 
        cabd.cabd_id AS dam_cabd_id,
        cabd.up_passage_type_code AS dam_up_passage,
        fish.cabd_id AS fishway_cabd_id,
        fish.dam_id AS fishway_dam_id,
        fish.fishpass_type_code AS fishway_up_passage
    FROM
        dams.dams AS cabd,
        fishways.fishways AS fish
    WHERE
        cabd.cabd_id = fish.dam_id
        AND (fish.fishpass_type_code IS NOT NULL AND fish.fishpass_type_code != (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown')
        AND fish.fishpass_type_code IS DISTINCT FROM cabd.up_passage_type_code)
)

UPDATE
    dams.dams AS cabd
SET
    up_passage_type_code = t.fishway_up_passage
FROM
    temp t
WHERE
    t.dam_cabd_id = cabd.cabd_id;
    

--Change null values to "unknown" for user benefit

UPDATE fishways.fishways SET fishpass_type_code = (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown') WHERE fishpass_type_code IS NULL;