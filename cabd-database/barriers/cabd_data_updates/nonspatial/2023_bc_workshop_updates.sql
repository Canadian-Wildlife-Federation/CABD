------------------------------------------------------------------------------------------------------
-- BC updates
------------------------------------------------------------------------------------------------------

-- create new data source records
INSERT INTO cabd.data_source (id, name, source, source_type)
VALUES
    (
        gen_random_uuid(),
        'pcom_bm_bces_2023',
        'Mills, B., Baker Creek Enhancement Society, 2023. Personal communication.',
        'non-spatial'
    ),
    (
        gen_random_uuid(),
        'pcom_jn_ldn_2023',
        'Neustater, J., Lhtako Dene Nation, 2023. Personal communication.',
        'non-spatial'
    )
    ;

INSERT INTO dams.dams_attribute_source (cabd_id)
    SELECT cabd_id FROM dams.dams WHERE cabd_id NOT IN (SELECT cabd_id FROM dams.dams_attribute_source);

UPDATE waterfalls.waterfalls_attribute_source
SET
    fall_name_en_ds = (SELECT id FROM cabd.data_source WHERE name = 'pcom_bm_bces_2023'),
    waterbody_name_en_ds = (SELECT id FROM cabd.data_source WHERE name = 'pcom_bm_bces_2023'),
    passability_status_code_ds = (SELECT id FROM cabd.data_source WHERE name = 'pcom_bm_bces_2023')
WHERE cabd_id = '1f95b15d-f8d3-4287-934a-5c4adc0aa9b8';

UPDATE dams.dams_attribute_source
SET
    structure_type_code_ds = (SELECT id FROM cabd.data_source WHERE name = 'pcom_bm_bces_2023'),
    height_m_ds = (SELECT id FROM cabd.data_source WHERE name = 'pcom_bm_bces_2023'),
    construction_material_code_ds = (SELECT id FROM cabd.data_source WHERE name = 'pcom_bm_bces_2023')
WHERE cabd_id = 'dcc50803-7f7a-4f1c-9c34-7b99358ef873';

UPDATE dams.dams_attribute_source
SET
    comments_ds = (SELECT id FROM cabd.data_source WHERE name = 'pcom_jn_ldn_2023'),
    condition_code_ds = (SELECT id FROM cabd.data_source WHERE name = 'pcom_jn_ldn_2023')
WHERE cabd_id = 'c0115f78-8f0c-4790-8484-e2f00cc80266';

UPDATE dams.dams_attribute_source
SET
    dam_name_en_ds = (SELECT id FROM cabd.data_source WHERE name = 'cwf'),
    dam_name_fr_ds = (SELECT id FROM cabd.data_source WHERE name = 'cwf'),
    passability_status_code_ds = (SELECT id FROM cabd.data_source WHERE name = 'cwf')
WHERE cabd_id = '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d';

-- run final attribute updates for new dams

UPDATE dams.dams
SET 
    size_class_code =
    CASE
    WHEN "height_m" < 5 THEN (SELECT code FROM dams.size_codes WHERE name_en = 'Small')
    WHEN "height_m" >= 5 AND  "height_m" < 15 THEN (SELECT code FROM dams.size_codes WHERE name_en = 'Medium')
    WHEN "height_m" >= 15 THEN (SELECT code FROM dams.size_codes WHERE name_en = 'Large')
    ELSE (SELECT code FROM dams.size_codes WHERE name_en = 'Unknown') END,

    reservoir_present =
    CASE
    WHEN 
        (reservoir_area_skm IS NOT NULL 
        OR reservoir_depth_m IS NOT NULL
        OR reservoir_name_en IS NOT NULL
        OR reservoir_name_fr IS NOT NULL)
        THEN TRUE
    ELSE reservoir_present END
WHERE cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);

UPDATE dams.dams
SET 
    passability_status_code =  (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Barrier')
WHERE
    passability_status_code IS NULL
    AND cabd_id = 'dcc50803-7f7a-4f1c-9c34-7b99358ef873';

UPDATE dams.dams AS dams
SET 
    province_territory_code = n.code FROM cabd.province_territory_codes AS n 
WHERE
    st_contains(n.geometry, dams.snapped_point)
    AND cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);

UPDATE dams.dams AS dams
SET 
    nhn_watershed_id = n.id FROM cabd.nhn_workunit AS n
WHERE 
    st_contains(n.polygon, dams.snapped_point)
    AND cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);


UPDATE dams.dams AS dams
SET 
    municipality = n.csdname FROM cabd.census_subdivisions AS n
WHERE
    st_contains(n.geometry, dams.snapped_point)
    AND dams.cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);

UPDATE dams.dams SET structure_type_code = (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Unknown') 
    WHERE structure_type_code IS NULL
    AND cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);

UPDATE dams.dams SET ownership_type_code = (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Unknown')
    WHERE ownership_type_code IS NULL
    AND cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);

UPDATE dams.dams SET use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown')
    WHERE use_code IS NULL
    AND cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);

UPDATE dams.dams SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
    WHERE function_code IS NULL
    AND cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);

UPDATE dams.dams SET operating_status_code = (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown')
    WHERE operating_status_code IS NULL
    AND cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);

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

    ELSE (SELECT code FROM dams.dam_complete_level_codes WHERE name_en = 'Unverified') END

WHERE cabd_id IN (
    'dcc50803-7f7a-4f1c-9c34-7b99358ef873',
    '36b0bcda-4671-45ff-8dea-d1fc8abe3a8d'
);