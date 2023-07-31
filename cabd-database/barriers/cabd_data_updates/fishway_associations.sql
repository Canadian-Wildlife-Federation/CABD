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
UPDATE
    dams.dams_attribute_source AS cabdsource
SET    
    up_passage_type_code_ds = 
        CASE
        WHEN ((cabd.up_passage_type_code IS NULL OR cabd.up_passage_type_code = (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown')) AND fish.fishpass_type_code IS NOT NULL) 
        THEN fishsource.fishpass_type_code_ds
        ELSE cabdsource.up_passage_type_code_ds END
FROM
    dams.dams AS cabd,
    fishways.fishways AS fish,
    fishways.fishways_attribute_source AS fishsource
WHERE
    cabdsource.cabd_id = fish.dam_id AND cabd.cabd_id = cabdsource.cabd_id;

--assign an upstream passage type based on presence of a fishway
UPDATE 
    dams.dams AS cabd
SET 
    up_passage_type_code =
    CASE
    WHEN ((cabd.up_passage_type_code IS NULL OR cabd.up_passage_type_code = (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown'))
        AND fish.fishpass_type_code IS NOT NULL AND fish.fishpass_type_code != (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown'))
        THEN fish.fishpass_type_code
    ELSE cabd.up_passage_type_code END
FROM
    fishways.fishways AS fish
WHERE 
    cabd.cabd_id = fish.dam_id;

--Change null values to "unknown" for user benefit
UPDATE fishways.fishways SET fishpass_type_code = (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown') WHERE fishpass_type_code IS NULL;

--Set NULL passability status to barrier

UPDATE dams.dams SET passability_status_code = 
    (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Barrier') WHERE passability_status_code IS NULL;

--Set passability status based on fishways / upstream passage type code
UPDATE dams.dams AS cabd 
    SET 
        passability_status_code = (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Partial Barrier'),
        passability_status_note = 'Marked as a partial barrier due to upstream passage measures from associated fishway.'
    FROM fishways.fishways AS f WHERE f.dam_id = cabd.cabd_id
    AND f.fishpass_type_code != (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Trap and truck');

UPDATE dams.dams AS cabd 
    SET 
        passability_status_code = (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Barrier'),
        passability_status_note = 'Marked as a barrier since the only upstream passage measure is a trap and truck method.'
    WHERE
        up_passage_type_code = (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Trap and truck');

UPDATE
    dams.dams_attribute_source AS cabdsource
    SET    
        passability_status_code_ds = 
        CASE
        WHEN (cabd.passability_status_code = (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Partial Barrier')) THEN fishsource.fishpass_type_code_ds
        ELSE cabdsource.passability_status_code_ds END
    FROM
    dams.dams AS cabd,
    fishways.fishways AS fish,
    fishways.fishways_attribute_source AS fishsource
    WHERE
    cabdsource.cabd_id = fish.dam_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE dams.dams
    SET 
        passability_status_note = 'Marked as a partial barrier due to upstream passage measures from associated fishway.'
    WHERE up_passage_type_code IS NOT NULL
	AND up_passage_type_code != (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Trap and truck')
	AND up_passage_type_code != (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'No structure');

--Update completeness level for dams
UPDATE dams.dams SET complete_level_code = 
    CASE 
    WHEN
        ((dam_name_en IS NOT NULL OR dam_name_fr IS NOT NULL)
        AND (waterbody_name_en IS NOT NULL OR waterbody_name_fr IS NOT NULL) 
        AND operating_status_code <> (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown')
        AND use_code <> (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown')
        AND function_code <> (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
        AND structure_type_code <> (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Unknown') 
        AND construction_year IS NOT NULL 
        AND height_m IS NOT NULL
        AND ("owner" IS NOT NULL OR ownership_type_code <> ((SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Unknown')))
        AND condition_code IS NOT NULL 
        AND reservoir_present IS NOT NULL 
        AND expected_end_of_life IS NOT NULL
        AND up_passage_type_code <> (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown') 
        AND down_passage_route_code IS NOT NULL
        AND length_m IS NOT NULL)
        THEN (SELECT code FROM dams.dam_complete_level_codes WHERE name_en = 'Complete')

    WHEN
        (((dam_name_en IS NOT NULL OR dam_name_fr IS NOT NULL)
        AND (waterbody_name_en IS NOT NULL OR waterbody_name_fr IS NOT NULL))
        AND operating_status_code <> (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown')
        AND use_code <> (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown')
        AND function_code <> (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
        AND construction_year IS NOT NULL 
        AND height_m IS NOT NULL
        AND ("owner" IS NOT NULL OR ownership_type_code <> (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Unknown')))
        THEN (SELECT code FROM dams.dam_complete_level_codes WHERE name_en = 'Moderate')

    WHEN
        (((dam_name_en IS NULL AND dam_name_fr IS NULL)
        OR (waterbody_name_en IS NULL AND waterbody_name_fr IS NULL)
        OR operating_status_code = (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown')
        OR height_m IS NULL 
        OR use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown')) 
        AND (function_code <> (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
        OR structure_type_code <> (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Unknown') 
        OR construction_year IS NOT NULL 
        OR ("owner" IS NOT NULL OR ownership_type_code <> (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Unknown'))))
        THEN (SELECT code FROM dams.dam_complete_level_codes WHERE name_en = 'Minimal')

    ELSE (SELECT code FROM dams.dam_complete_level_codes WHERE name_en = 'Unverified') END;