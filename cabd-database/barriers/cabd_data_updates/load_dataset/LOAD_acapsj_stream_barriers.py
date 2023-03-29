import LOAD_crossings_main as main # this sets the script. variables referenced below

script = main.LoadingScript("acapsj_stream_barriers") # this will be the datasetname variable referenced below

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} RENAME COLUMN original_source TO data_source_name;
ALTER TABLE {script.sourceTable} RENAME COLUMN id TO cabd_assessment_id;
ALTER TABLE {script.sourceTable} ALTER COLUMN cabd_assessment_id TYPE uuid USING cabd_assessment_id::uuid;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id uuid;

UPDATE {script.sourceTable} SET data_source_id = (SELECT id FROM cabd.data_source WHERE name = 'acap_saint_john'); -- make sure to add a record for the organization to the cabd.data_source table ahead of time
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_id_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (cabd_assessment_id);

ALTER TABLE {script.sourceTable} ALTER COLUMN geometry TYPE geometry(POINT, 2953) USING ST_Transform(geometry, 2953); --reprojects to a meters based CRS
ALTER TABLE {script.sourceTable} DROP COLUMN fid;

------------------------------------------
-- nontidal crossings
-------------------------------------------

--add information to sites table
DROP TABLE IF EXISTS {script.nonTidalSites};
CREATE TABLE {script.nonTidalSites} AS
    SELECT
        cabd_assessment_id,
        data_source_name,
        data_source_id,
        crossing_type_code,
        location as stream_name,
        notes as crossing_comments,
        site_type_code,
        crossing_condition_code,
        flow_condition_code,
        geometry
    FROM {script.sourceTable}
    WHERE include = 'TRUE' AND site_type_code = 99;

ALTER TABLE {script.nonTidalSites} ALTER COLUMN cabd_assessment_id SET NOT NULL;
ALTER TABLE {script.nonTidalSites} ADD PRIMARY KEY (cabd_assessment_id);
ALTER TABLE {script.nonTidalSites} ADD CONSTRAINT {script.datasetname}_data_source_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.nonTidalSites}
    ADD COLUMN cabd_id uuid,
    ADD COLUMN original_point geometry(Point,4617);

UPDATE {script.nonTidalSites} SET original_point = ST_Transform(geometry, 4617);

UPDATE {script.nonTidalSites} SET cabd_id = r.modelled_crossing_id::uuid
FROM {script.reviewTable} AS r
WHERE
    (r.source_1 = 'ACAP_Stream_Barriers' AND cabd_assessment_id = r.id_1::uuid)
    OR 
    (r.source_2 = 'ACAP_Stream_Barriers' AND cabd_assessment_id = r.id_2::uuid)
    OR
    (r.source_3 = 'ACAP_Stream_Barriers' AND cabd_assessment_id = r.id_3::uuid);

ALTER TABLE {script.nonTidalSites} ADD COLUMN entry_classification varchar;
UPDATE {script.nonTidalSites} SET entry_classification =
    CASE
    WHEN cabd_id IS NULL THEN 'new feature'
    WHEN cabd_id IS NOT NULL THEN 'update feature'
    ELSE NULL END;

------------------------------------------
-- nontidal structures
------------------------------------------

--add information to structures tables

DROP TABLE IF EXISTS {script.nonTidalStructures};
CREATE TABLE {script.nonTidalStructures} AS (
    SELECT
        site.cabd_id AS site_id,
        gen_random_uuid() as structure_id,
        source.data_source_id,
        source.cabd_assessment_id,
        source.stucture_length_m AS structure_length_m,
        source.passability_status_code,
        source.internal_structures_code
    FROM
        {script.nonTidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
);

ALTER TABLE {script.nonTidalStructures} ALTER COLUMN structure_id SET NOT NULL;
ALTER TABLE {script.nonTidalStructures} ADD PRIMARY KEY (structure_id);

------------------------------------------
-- tidal crossings
-------------------------------------------

--add information to sites table
DROP TABLE IF EXISTS {script.tidalSites};
CREATE TABLE {script.tidalSites} AS
    SELECT
        cabd_assessment_id,
        data_source_name,
        data_source_id,
        location as stream_name,
        site_type_code,
        crossing_type_code,
        geometry
    FROM {script.sourceTable}
    WHERE include = 'TRUE' AND site_type_code = 1;

ALTER TABLE {script.tidalSites} ALTER COLUMN cabd_assessment_id SET NOT NULL;
ALTER TABLE {script.tidalSites} ADD PRIMARY KEY (cabd_assessment_id);
ALTER TABLE {script.tidalSites} ADD CONSTRAINT {script.datasetname}_data_source_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.tidalSites}
    ADD COLUMN cabd_id uuid,
    ADD COLUMN original_point geometry(Point,4617);

UPDATE {script.tidalSites} SET original_point = ST_Transform(geometry, 4617);

UPDATE {script.tidalSites} SET cabd_id = r.modelled_crossing_id::uuid
FROM {script.reviewTable} AS r
WHERE
    (r.source_1 = 'ACAP_Stream_Barriers' AND cabd_assessment_id = r.id_1::uuid)
    OR 
    (r.source_2 = 'ACAP_Stream_Barriers' AND cabd_assessment_id = r.id_2::uuid)
    OR
    (r.source_3 = 'ACAP_Stream_Barriers' AND cabd_assessment_id = r.id_3::uuid);

ALTER TABLE {script.tidalSites} ADD COLUMN entry_classification varchar;
UPDATE {script.tidalSites} SET entry_classification =
    CASE
    WHEN cabd_id IS NULL THEN 'new feature'
    WHEN cabd_id IS NOT NULL THEN 'update feature'
    ELSE NULL END;

------------------------------------------
-- tidal structures
------------------------------------------

--add information to structures tables

DROP TABLE IF EXISTS {script.tidalStructures};
CREATE TABLE {script.tidalStructures} AS (
    SELECT
        site.cabd_id AS site_id,
        gen_random_uuid() as structure_id,
        source.data_source_id,
        source.cabd_assessment_id,
        source.tidal_tide_gate_type_code
    FROM
        {script.tidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
);

ALTER TABLE {script.tidalStructures} ALTER COLUMN structure_id SET NOT NULL;
ALTER TABLE {script.tidalStructures} ADD PRIMARY KEY (structure_id);

"""

script.do_work(query)