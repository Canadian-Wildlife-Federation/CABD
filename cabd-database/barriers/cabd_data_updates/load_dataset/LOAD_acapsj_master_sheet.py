import LOAD_crossings_main as main # this sets the script. variables referenced below

script = main.LoadingScript("acapsj_master_sheet") # this will be the datasetname variable referenced below

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
        site AS original_assessment_id,
        data_source_name,
        data_source_id,
        date as date_observed,
        "note taker" as lead_observer,
        stream as stream_name,
        crossing_type_code,
        road_type_code,
        road_surface,
        road_class,
        crossing_condition_code,
        flow_condition_code,
        geometry
    FROM {script.sourceTable}
    WHERE include = 'TRUE';

ALTER TABLE {script.nonTidalSites} ALTER COLUMN cabd_assessment_id SET NOT NULL;
ALTER TABLE {script.nonTidalSites} ADD PRIMARY KEY (cabd_assessment_id);
ALTER TABLE {script.nonTidalSites} ADD CONSTRAINT {script.datasetname}_data_source_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.nonTidalSites} ALTER COLUMN date_observed TYPE date USING date_observed::date;

ALTER TABLE {script.nonTidalSites}
    ADD COLUMN cabd_id uuid,
    ADD COLUMN original_point geometry(Point,4617);

UPDATE {script.nonTidalSites} SET original_point = ST_Transform(geometry, 4617);

UPDATE {script.nonTidalSites} SET cabd_id = r.modelled_crossing_id::uuid
FROM {script.reviewTable} AS r
WHERE
    (r.source_1 = 'Master_Sheet' AND cabd_assessment_id = r.id_1::uuid)
    OR 
    (r.source_2 = 'Master_Sheet' AND cabd_assessment_id = r.id_2::uuid)
    OR
    (r.source_3 = 'Master_Sheet' AND cabd_assessment_id = r.id_3::uuid);

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
        site.original_assessment_id,
        source.notes as structure_comments,
        source.outlet_shape_code,
        source.inlet_shape_code,
        source.substrate_type_code,
        source.outlet_width_m,
        source.inlet_width_m,
        source.internal_structures_code,
        source.inlet_grade_code,
        source.material_code
    FROM
        {script.nonTidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
);

ALTER TABLE {script.nonTidalStructures} ALTER COLUMN structure_id SET NOT NULL;
ALTER TABLE {script.nonTidalStructures} ADD PRIMARY KEY (structure_id);

--insert into material mapping table
DELETE FROM {script.nonTidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM {script.nonTidalStructures});
INSERT INTO {script.nonTidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        material_code,
        cabd_assessment_id
    FROM {script.nonTidalStructures} WHERE material_code IS NOT NULL;

"""

script.do_work(query)