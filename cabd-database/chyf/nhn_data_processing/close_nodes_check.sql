-- See: https://github.com/Canadian-Wildlife-Federation/CHYF/issues/30
-- Notes: in the sql below you need to configure the AOI's of interest

---------------------------------------------------------------
-- Close nodes check for fpoutput and across fpoutput + chyf2
---------------------------------------------------------------

CREATE TABLE fpoutput.new_nodes_temp AS
WITH aoiids AS (
    SELECT id
    FROM fpoutput.aoi
    WHERE name IN ('XXXX', 'YYYY')
),
all_records AS (
    SELECT ST_StartPoint(geometry) AS geometry
    FROM fpoutput.eflowpath
    WHERE aoi_id IN (SELECT id FROM aoiids)

    UNION ALL

    SELECT ST_EndPoint(geometry) AS geometry
    FROM fpoutput.eflowpath
    WHERE aoi_id IN (SELECT id FROM aoiids)
)
SELECT
    row_number() OVER () AS id,
    geometry
FROM (
    SELECT DISTINCT geometry
    FROM all_records
) d;

-- add index
CREATE INDEX new_nodes_gix
ON fpoutput.new_nodes_temp
USING gist (geometry);

ANALYZE fpoutput.new_nodes_temp;

-- Search for close nodes within these AOIs
CREATE TABLE fpoutput.new_node_temp_inaois AS
SELECT
    a.id       AS a_id,
    b.id       AS b_id,
    a.geometry AS a_geometry,
    b.geometry AS b_geometry
FROM fpoutput.new_nodes_temp a
JOIN fpoutput.new_nodes_temp b
    ON a.id < b.id
   AND ST_DWithin(a.geometry, b.geometry, 0.0000001);

   -- Search for close nodes between these new data and the existing nexus data
CREATE TABLE fpoutput.new_node_temp_chyf2 AS
SELECT
    a.id       AS a_id,
    b.id       AS b_id,
    a.geometry AS a_geometry,
    b.geometry AS b_geometry
FROM fpoutput.new_nodes_temp a
JOIN chyf2.nexus b
    ON ST_DWithin(a.geometry, b.geometry, 0.0000001)
   AND NOT ST_Equals(a.geometry, b.geometry);

alter table fpoutput.new_node_temp_chyf2 add column id serial primary key;
alter table fpoutput.new_node_temp_chyf2 add column checked boolean;

SELECT Populate_Geometry_Columns('fpoutput.new_node_temp_chyf2'::regclass);

alter table fpoutput.new_node_temp_inaois add column id serial primary key;
alter table fpoutput.new_node_temp_inaois add column checked boolean;

SELECT Populate_Geometry_Columns('fpoutput.new_node_temp_inaois'::regclass);

---------------------------------------------------------------
-- Close nodes checks for chyf2 - runs on entire schema
---------------------------------------------------------------
---------------------------------------------------------------
-- NEXUS MISMATCHES
-- Does the geometry endpoint of the flowpath actually coincide
-- with the nexus referenced by from_nexus_id / to_nexus_id?
-- Find where endpoint geometry and referenced nexus
-- geometry are different
---------------------------------------------------------------

DROP TABLE IF EXISTS fpoutput.chyf2_endpoint_nexus_mismatches;

CREATE TABLE fpoutput.chyf2_endpoint_nexus_mismatches AS

SELECT
    e.id AS eflowpath_id,
    'from_nexus'::text AS issue_type,
    e.from_nexus_id AS nexus_id,
    ST_StartPoint(e.geometry) AS endpoint_geometry,
    n.geometry AS nexus_geometry,
    ST_Distance(
        ST_StartPoint(e.geometry),
        n.geometry
    ) AS distance_degrees
FROM chyf2.eflowpath e
JOIN chyf2.nexus n
    ON n.id = e.from_nexus_id
WHERE NOT ST_Equals(
    ST_StartPoint(e.geometry),
    n.geometry
)

UNION ALL

SELECT
    e.id AS eflowpath_id,
    'to_nexus'::text AS issue_type,
    e.to_nexus_id AS nexus_id,
    ST_EndPoint(e.geometry) AS endpoint_geometry,
    n.geometry AS nexus_geometry,
    ST_Distance(
        ST_EndPoint(e.geometry),
        n.geometry
    ) AS distance_degrees
FROM chyf2.eflowpath e
JOIN chyf2.nexus n
    ON n.id = e.to_nexus_id
WHERE NOT ST_Equals(
    ST_EndPoint(e.geometry),
    n.geometry
);

ALTER TABLE fpoutput.chyf2_endpoint_nexus_mismatches
    ADD COLUMN id BIGSERIAL PRIMARY KEY;

ALTER TABLE fpoutput.chyf2_endpoint_nexus_mismatches
    ADD COLUMN checked BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX chyf2_endpoint_nexus_mismatches_endpoint_gix
    ON fpoutput.chyf2_endpoint_nexus_mismatches
    USING GIST (endpoint_geometry);

ANALYZE fpoutput.chyf2_endpoint_nexus_mismatches;

---------------------------------------------------------------
-- CORRECT NEXUS CANDIDATES
-- If the referenced nexus is wrong, is there another nexus
-- sitting exactly on the flowpath endpoint?
---------------------------------------------------------------

DROP TABLE IF EXISTS fpoutput.chyf2_endpoint_nexus_candidates;

CREATE TABLE fpoutput.chyf2_endpoint_nexus_candidates AS

WITH mismatches AS (

    SELECT DISTINCT
        eflowpath_id,
        issue_type
    FROM fpoutput.chyf2_endpoint_nexus_mismatches

)

SELECT
    e.id AS eflowpath_id,

    m.issue_type,

    CASE
        WHEN m.issue_type = 'from_nexus'
            THEN e.from_nexus_id
        ELSE e.to_nexus_id
    END AS current_nexus_id,

    n_current.geometry AS current_nexus_geometry,

    n_candidate.id AS candidate_nexus_id,
    n_candidate.nexus_type AS candidate_nexus_type,
    n_candidate.geometry AS candidate_nexus_geometry,

    CASE
        WHEN m.issue_type = 'from_nexus'
            THEN ST_StartPoint(e.geometry)
        ELSE ST_EndPoint(e.geometry)
    END AS endpoint_geometry,

    ST_Distance(
        n_current.geometry,
        CASE
            WHEN m.issue_type = 'from_nexus'
                THEN ST_StartPoint(e.geometry)
            ELSE ST_EndPoint(e.geometry)
        END
    )::double precision AS current_distance

FROM mismatches m

JOIN chyf2.eflowpath e
    ON e.id = m.eflowpath_id

JOIN chyf2.nexus n_current
    ON n_current.id =
       CASE
           WHEN m.issue_type = 'from_nexus'
               THEN e.from_nexus_id
           ELSE e.to_nexus_id
       END

JOIN chyf2.nexus n_candidate
    ON ST_Equals(
        n_candidate.geometry,
        CASE
            WHEN m.issue_type = 'from_nexus'
                THEN ST_StartPoint(e.geometry)
            ELSE ST_EndPoint(e.geometry)
        END
    );

ALTER TABLE fpoutput.chyf2_endpoint_nexus_candidates
    ADD COLUMN id BIGSERIAL PRIMARY KEY;

ALTER TABLE fpoutput.chyf2_endpoint_nexus_candidates
    ADD COLUMN checked BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX chyf2_endpoint_nexus_candidates_endpoint_gix
    ON fpoutput.chyf2_endpoint_nexus_candidates
    USING GIST (endpoint_geometry);

ANALYZE fpoutput.chyf2_endpoint_nexus_candidates;
