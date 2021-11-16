--associate fishways with dams
UPDATE fishways.fishways SET dam_id = foo.dam_id FROM (SELECT DISTINCT ON (cabd_id) cabd_id, dam_id
   FROM (
       SELECT
           fish.cabd_id,
           dam.cabd_id as dam_id
       FROM
         fishways.fishways AS fish,
         dams.dams AS dams,
         ST_Distance(fish.original_point, dams.original_point) as distance
       WHERE
           ST_Distance(fish.original_point, dams.original_point) < 0.01 and
           ST_Distance(fish.original_point::geography,
dam.original_point::geography) < 150
       ORDER BY cabd_id, distance
   ) bar
   ) foo
   WHERE foo.cabd_id = fishways.fishways.cabd_id;

--update dams attribute source table
UPDATE
    dams.dams_attribute_source AS cabdsource
SET    
    up_passage_type_code_ds = CASE WHEN (cabd.up_passage_type_code = 9 AND fish.fishpass_type_code IS NOT NULL) THEN fishsource.fishpass_type_code_ds ELSE cabdsource.up_passage_type_code_ds END, 

    up_passage_type_code_dsfid = CASE WHEN (cabd.up_passage_type_code = 9 AND fish.fishpass_type_code IS NOT NULL) THEN fish.cabd_id::varchar ELSE cabdsource.up_passage_type_code_dsfid END    
FROM
    dams.dams AS cabd,
    fishways.fishways AS fish,
    fishways.fishways_attribute_source AS fishsource
WHERE
    cabdsource.cabd_id = fish.dam_id AND cabd.cabd_id = cabdsource.cabd_id;

--assign an upstream passage type based on presence of a fishway
--TO DO: need to consider dams with multiple fishways - map anything with an eel ladder to reflect this in comments field
UPDATE 
    dams.dams AS cabd
SET 
    up_passage_type_code = CASE WHEN (cabd.up_passage_type_code = 9 AND fish.fishpass_type_code IS NOT NULL) THEN fish.fishpass_type_code ELSE cabd.up_passage_type_code END
FROM
    fishways.fishways AS fish
WHERE 
    cabd.cabd_id = fish.dam_id;