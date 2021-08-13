UPDATE damcopy.dams SET complete_level_code = 
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
        AND ownership_type_code <> 7 -- or "owner" is not null
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
        AND operating_status_code <> 5 -- evaluate in level 4
        AND use_code <> 11
        AND function_code <> 13 -- evaluate in level 4
        AND construction_type_code <> 9 -- evaluate in level 4
        AND construction_year IS NOT NULL 
        AND height_m IS NOT NULL
        AND ownership_type_code <> 7) -- + or "owner" is not null, or evaluate in level 4.
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
        OR ownership_type_code <> 7)) -- + or "owner" is not null, or evaluate in level 4 only.
        THEN 2
    ELSE 1 END;
    