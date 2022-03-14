import LOAD_main as main

script = main.LoadingScript("ncc")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = unique_id;
UPDATE {script.sourceTable} SET data_source = 'ce45dfdb-26d1-47ae-9f9c-2b353f3676d1';
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--split into dams, add new columns, and map attributes
DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT 
        barrier_na,
        waterbody,
        comment,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE type = 'DAM';

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN "comments" text;

UPDATE {script.damWorkingTable} SET dam_name_en = barrier_na;
UPDATE {script.damWorkingTable} SET waterbody_name_en = waterbody;
UPDATE {script.damWorkingTable} SET "comments" = comment;
    
--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN barrier_na,
    DROP COLUMN waterbody,
    DROP COLUMN comment;


--split into waterfalls, add new columns, and map attributes
DROP TABLE IF EXISTS {script.fallWorkingTable};
CREATE TABLE {script.fallWorkingTable} AS
    SELECT 
        barrier_na,
        waterbody,
        comment,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE type = 'NATURAL';

ALTER TABLE {script.fallWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.fallWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.fallWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.fallWorkingTable} ADD COLUMN fall_name_en varchar(512);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN "comments" text;

UPDATE {script.fallWorkingTable} SET fall_name_en = barrier_na;
UPDATE {script.fallWorkingTable} SET waterbody_name_en = waterbody;
UPDATE {script.fallWorkingTable} SET "comments" = comment;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.fallWorkingTable}
    DROP COLUMN barrier_na,
    DROP COLUMN waterbody,
    DROP COLUMN comment;

"""

script.do_work(query)