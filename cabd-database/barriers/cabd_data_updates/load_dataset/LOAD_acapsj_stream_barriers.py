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
--nontidal crossings
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
        crossing_type_code,
        crossing_condition_code,
        flow_condition_code,
        geometry
    FROM {script.sourceTable}
    WHERE include = TRUE; -- e.g., exclude rows that are not stream crossings

ALTER TABLE {script.nonTidalSites} ALTER COLUMN cabd_assessment_id SET NOT NULL;
ALTER TABLE {script.nonTidalSites} ADD PRIMARY KEY (cabd_assessment_id);
ALTER TABLE {script.nonTidalSites} ADD CONSTRAINT {script.datasetname}_data_source_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.nonTidalSites}
    ADD COLUMN cabd_id uuid,
    ADD COLUMN original_point geometry(Point,4617);


UPDATE {script.nonTidalSites} SET original_point = ST_Transform(geometry, 4617);

--add information to structures tables

DROP TABLE IF EXISTS {script.nonTidalStructures};
CREATE TABLE {script.nonTidalStructures} AS (
    SELECT
        site.cabd_id AS site_id,
        gen_random_uuid() as structure_id,
        source.data_source_id,
        source.cabd_assessment_id,
        source.structure_length_m,
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
--tidal crossings
-------------------------------------------

--add information to sites table
DROP TABLE IF EXISTS {script.tidalSites};
CREATE TABLE {script.tidalSites} AS
    SELECT
        cabd_assessment_id,
        data_source_name,
        data_source_id,
        crossing_type_code,
        location as stream_name,
        site_type_code,
        crossing_type_code,
        geometry
    FROM {script.sourceTable}
    WHERE include = TRUE; -- e.g., exclude rows that are not stream crossings

ALTER TABLE {script.tidalSites} ALTER COLUMN cabd_assessment_id SET NOT NULL;
ALTER TABLE {script.tidalSites} ADD PRIMARY KEY (cabd_assessment_id);
ALTER TABLE {script.tidalSites} ADD CONSTRAINT {script.datasetname}_data_source_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.tidalSites}
    ADD COLUMN cabd_id uuid,
    ADD COLUMN original_point geometry(Point,4617);


UPDATE {script.tidalSites} SET original_point = ST_Transform(geometry, 4617);

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