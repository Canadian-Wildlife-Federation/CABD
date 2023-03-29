import LOAD_crossings_main as main

script = main.LoadingScript("kwrc_bridges")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} RENAME COLUMN original_source TO data_source_name;
ALTER TABLE {script.sourceTable} RENAME COLUMN id TO cabd_assessment_id;
ALTER TABLE {script.sourceTable} ALTER COLUMN cabd_assessment_id TYPE uuid USING cabd_assessment_id::uuid;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id uuid;
UPDATE {script.sourceTable} SET data_source_id = (SELECT id FROM cabd.data_source WHERE name = 'kennebecasis_wrc');
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_id_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (cabd_assessment_id);

ALTER TABLE {script.sourceTable} ALTER COLUMN geometry TYPE geometry(POINT, 2953) USING ST_Transform(geometry, 2953);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;

------------------------------------------
--nontidal crossings
------------------------------------------

--add information to sites table
DROP TABLE IF EXISTS {script.nonTidalSites};
CREATE TABLE {script.nonTidalSites} AS
    SELECT
        cabd_assessment_id::uuid,
        "crossing number" AS original_assessment_id,
        data_source_name,
        data_source_id,
        geometry
    FROM {script.sourceTable}
    WHERE latitude IS NOT NULL
    AND longitude IS NOT NULL;

ALTER TABLE {script.nonTidalSites} ALTER COLUMN cabd_assessment_id SET NOT NULL;
ALTER TABLE {script.nonTidalSites} ADD PRIMARY KEY (cabd_assessment_id);
ALTER TABLE {script.nonTidalSites} ADD CONSTRAINT {script.datasetname}_data_source_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.nonTidalSites}
    ADD COLUMN cabd_id uuid,
    ADD COLUMN crossing_type_code integer,
    ADD COLUMN original_point geometry(Point,4617);

UPDATE {script.nonTidalSites} SET original_point = ST_Transform(geometry, 4617);

UPDATE {script.nonTidalSites} SET crossing_type_code = (SELECT code FROM stream_crossings.crossing_type_codes WHERE name_en = 'bridge');

UPDATE {script.nonTidalSites} SET cabd_id = r.modelled_crossing_id::uuid
FROM {script.reviewTable} AS r
WHERE
    (r.source_1 = 'Bridges' AND cabd_assessment_id = r.id_1::uuid)
    OR 
    (r.source_2 = 'Bridges' AND cabd_assessment_id = r.id_2::uuid)
    OR
    (r.source_3 = 'Bridges' AND cabd_assessment_id = r.id_3::uuid);

ALTER TABLE {script.nonTidalSites} ADD COLUMN entry_classification varchar;
UPDATE {script.nonTidalSites} SET entry_classification =
    CASE
    WHEN cabd_id IS NULL THEN 'new feature'
    WHEN cabd_id IS NOT NULL THEN 'update feature'
    ELSE NULL END;

------------------------------------------
-- nontidal structures
------------------------------------------

DROP TABLE IF EXISTS {script.nonTidalStructures};

CREATE TABLE {script.nonTidalStructures} AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.original_assessment_id
    FROM
        {script.nonTidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
);

ALTER TABLE {script.nonTidalStructures} ADD COLUMN structure_id uuid;

UPDATE {script.nonTidalStructures} SET structure_id = gen_random_uuid();
ALTER TABLE {script.nonTidalStructures} ALTER COLUMN structure_id SET NOT NULL;
ALTER TABLE {script.nonTidalStructures} ADD PRIMARY KEY (structure_id);

"""

script.do_work(query)