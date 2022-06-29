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

--update dams attribute source and feature source tables
UPDATE
    dams.dams_attribute_source AS cabdsource
SET    
    up_passage_type_code_ds = CASE WHEN ((cabd.up_passage_type_code IS NULL OR cabd.up_passage_type_code = 9) AND fish.fishpass_type_code IS NOT NULL) THEN fishsource.fishpass_type_code_ds ELSE cabdsource.up_passage_type_code_ds END
FROM
    dams.dams AS cabd,
    fishways.fishways AS fish,
    fishways.fishways_attribute_source AS fishsource
WHERE
    cabdsource.cabd_id = fish.dam_id AND cabd.cabd_id = cabdsource.cabd_id;

INSERT INTO dams.dams_feature_source (cabd_id, datasource_id, datasource_feature_id)
SELECT dams.cabd_id, fishsource.datasource_id, fishsource.datasource_feature_id
FROM
	dams.dams AS dams,
    fishways.fishways AS fish,
    fishways.fishways_feature_source AS fishsource
WHERE
    (dams.cabd_id = fish.dam_id AND fish.cabd_id = fishsource.cabd_id)
    AND fishsource.datasource_id = (SELECT id FROM cabd.data_source WHERE "name" = 'cwf_canfish')
ON CONFLICT DO NOTHING;

--assign an upstream passage type based on presence of a fishway
UPDATE 
    dams.dams AS cabd
SET 
    up_passage_type_code = CASE WHEN ((cabd.up_passage_type_code IS NULL OR cabd.up_passage_type_code = 9) AND fish.fishpass_type_code IS NOT NULL) THEN fish.fishpass_type_code ELSE cabd.up_passage_type_code END
FROM
    fishways.fishways AS fish
WHERE 
    cabd.cabd_id = fish.dam_id;

--Change null values to "unknown" for user benefit
UPDATE fishways.fishways SET fishpass_type_code = 9 WHERE fishpass_type_code IS NULL;

--Give dams a passability_status_code
UPDATE dams.dams AS cabd SET passability_status_code = 2 FROM fishways.fishways AS f WHERE f.dam_id = cabd.cabd_id;
UPDATE dams.dams SET passability_status_code = 1 WHERE passability_status_code IS NULL;

--Update completeness level for dams
UPDATE dams.dams SET complete_level_code = 
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