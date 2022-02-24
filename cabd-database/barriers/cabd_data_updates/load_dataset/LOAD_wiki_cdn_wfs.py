import LOAD_main as main

script = main.LoadingScript("wiki_cdn_wfs")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = fid;
UPDATE {script.sourceTable} SET data_source = '1ec6c575-04da-4416-b870-0af2cba15206';
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--add new columns and map attributes
DROP TABLE IF EXISTS {script.fallWorkingTable};
CREATE TABLE {script.fallWorkingTable} AS
    SELECT 
        waterfall,
        watercourse,
        "drop_m",
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.fallWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.fallWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.fallWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.fallWorkingTable} ADD COLUMN fall_name_en varchar(512);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN fall_height_m float4;

UPDATE {script.fallWorkingTable} SET fall_name_en = waterfall;
UPDATE {script.fallWorkingTable} SET waterbody_name_en = 
    CASE
    WHEN watercourse ILIKE '%Unnamed%' OR watercourse ILIKE '%Unknown%' THEN NULL
    ELSE watercourse END;
UPDATE {script.fallWorkingTable} SET fall_height_m = "drop_m"::float4 WHERE "drop_m" IS NOT NULL;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.fallWorkingTable}
    DROP COLUMN waterfall,
    DROP COLUMN watercourse,
    DROP COLUMN "drop_m";

"""

script.do_work(query)