--on_sudbury_br_ct
--this is run after the dataset is imported into a table in the DB
--assumes schema name is source_data and table name is on_sudbury_br_ct

-- PART ONE - ADD UNIQUE ID AND MATCH --

ALTER TABLE source_data.on_sudbury_br_ct ADD COLUMN unique_id uuid;
UPDATE source_data.on_sudbury_br_ct SET unique_id = gen_random_uuid();

ALTER TABLE source_data.on_sudbury_br_ct DROP COLUMN IF EXISTS crossing_distance;
ALTER TABLE source_data.on_sudbury_br_ct ADD COLUMN crossing_distance double precision;

with match as (
    SELECT
    a.cabd_id AS id,
    nn.unique_id AS unique_id,
    nn.dist
    FROM stream_crossings.nontidal_sites a
    CROSS JOIN LATERAL (
        SELECT
        unique_id,
        ST_Distance(a.geometry_m, b.geometry) as dist
        FROM source_data.on_sudbury_br_ct b
        WHERE crossingtype NOT ILIKE '%nw' -- this is new - I think we can exclude these crossings at this point
        ORDER BY a.geometry_m <-> b.geometry
        LIMIT 1
    ) as nn
    WHERE nn.dist < {dist} -- this needs to be specified
),

match_distinct AS (
    select distinct on(unique_id) unique_id, dist, id
    from match
    order by unique_id, dist asc
)

UPDATE source_data.on_sudbury_br_ct a
SET crossing_id = m.id,
    crossing_distance = m.dist
FROM match_distinct AS m WHERE m.unique_id = a.unique_id;

-- TO DO: think about how to add points from assessment data that don't match
-- a current point, but still affect a stream
-- could just give these points a generated crossing_id value?
-- idea below
-- note we will need to snap these to the stream network as well

UPDATE source_data.on_sudbury_br_ct a
SET crossing_id = gen_random_uuid()
WHERE crossing_id IS NULL
AND ST_Distance(crossing.geometry, stream.geometry) >= {somevalue} -- this needs to be specified
AND crossingtype NOT ILIKE '%nw'; -- add any other criteria you want here

-- PART TWO - CREATE AND MAP TO SITES TABLE --

CREATE TABLE source_data.on_sudbury_br_ct_sites AS (
    SELECT
        unique_id,
        crossing_id,
        structurename,
        skewangle,
        geometry
    FROM source_data.on_sudbury_br_ct
    WHERE crossing_id IS NOT NULL --this will ensure only the matched features are brought in
);

ALTER TABLE source_data.on_sudbury_br_ct_sites
    ADD COLUMN crossing_type_code integer,
    ADD COLUMN crossing_subtype_code integer,
    ADD COLUMN alignment_code integer;
    ADD COLUMN cabd_id uuid;

UPDATE source_data.on_sudbury_br_ct_sites SET cabd_id = crossing_id;

ALTER TABLE source_data.on_sudbury_br_ct_sites ADD PRIMARY KEY (cabd_id);

UPDATE source_data.on_sudbury_br_ct_sites SET crossing_type_code =
    CASE
    WHEN structurename ilike '%culvert%' THEN (SELECT code FROM stream_crossings.crossing_type_codes WHERE name_en = 'CBS') --closed-bottom structure
    WHEN structurename ilike '%bridge%' THEN (SELECT code FROM stream_crossings.crossing_type_codes WHERE name_en = 'OBS') --open-bottom structure
    ELSE NULL END;

UPDATE source_data.on_sudbury_br_ct_sites SET crossing_subtype_code =
    CASE
    WHEN structurename ilike '%culvert%' THEN (SELECT code FROM stream_crossings.crossing_subtype_codes WHERE name_en = 'culvert')
    WHEN structurename ilike '%bridge%' THEN (SELECT code FROM stream_crossings.crossing_subtype_codes WHERE name_en = 'bridge')
    ELSE NULL END;

UPDATE source_data.on_sudbury_br_ct_sites SET alignment_code =
    CASE
    WHEN skewangle < 45 THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'flow-aligned')
    WHEN skewangle >= 45 THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'skewed')
    ELSE NULL END;

-- PART THREE - CREATE AND MAP TO STRUCTURES TABLE --

CREATE TABLE source_data.on_sudbury_br_ct_structures AS (
    SELECT
        unique_id,
        crossing_id,
        structurename,
        stype
    FROM source_data.on_sudbury_br_ct
    WHERE crossing_id IS NOT NULL --this will ensure only the matched features are brought in
);

ALTER TABLE source_data.on_sudbury_br_ct_structures
    ADD COLUMN passability_status_code integer,
    ADD COLUMN site_id uuid,
    ADD COLUMN structure_id uuid;

UPDATE source_data.on_sudbury_br_ct_structures SET site_id = crossing_id; --we can do this because the sites table uses crossing_id for their PK
UPDATE source_data.on_sudbury_br_ct_structures SET structure_id = gen_random_uuid();

ALTER TABLE source_data.on_sudbury_br_ct_structures ADD PRIMARY KEY (structure_id);

UPDATE source_data.on_sudbury_br_ct_structures SET passability_status_code =
    CASE
    WHEN structurename ilike '%culvert%' THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
    WHEN structurename ilike '%bridge%' THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Passable')
    ELSE NULL END;

-- PART FOUR - CREATE AND MAP TO MATERIAL MAPPING TABLES --
-- this is only needed if there are materials to be mapped, skip otherwise

CREATE TABLE source_data.on_sudbury_br_ct_material AS (
    SELECT * FROM stream_crossings.nontidal_material_mapping WITH NO DATA --this table structure is worth looking at btw
);

INSERT INTO source_data.on_sudbury_br_ct_material (
    structure_id,
    material_code
)
SELECT
    foo.structure_id,
    CASE
    WHEN stype = 'Bailey Bridge' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
    WHEN (regexp_match("stype", '(?i)(concrete)|(prestressed)|(slab on prestress)|(slab)') IS NOT NULL) THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
    WHEN (regexp_match("stype", '(?i)(steel)|(girder)|(truss)|(tunnel)') IS NOT NULL) THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
    WHEN stype ILIKE '%slab on steel%' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
    WHEN stype ILIKE '%soil-steel%' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal (corrugated)')
    WHEN stype ILIKE '%timber%' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
    END AS material_code
FROM source_data.on_sudbury_br_ct_structures AS foo;

--deal with some cases where structures had multiple materials
INSERT INTO source_data.on_sudbury_br_ct_material (
    structure_id,
    material_code
)
SELECT
    foo.structure_id,
    CASE
    WHEN stype = 'Bailey Bridge' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
    WHEN stype ILIKE '%slab on steel%' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
    END AS material_code
FROM source_data.on_sudbury_br_ct_structures AS foo;

-- OUTPUT --
-- At the end of this script you should have the following tables:
-- nontidal sites from assessment data
-- nontidal structures from assessment data (linked to sites)
-- material mapping table that is linked to the nontidal structures table
-- for other datasets, you may only end up with the first two