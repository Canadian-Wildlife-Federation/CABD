import LOAD_main as main

script = main.LoadingScript("nhn")

query = f"""
--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = nid;
UPDATE {script.sourceTable} SET data_source = '9417da74-5cc8-4efa-8f43-0524fa47996d';
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
        name1,
        manmadestatus,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE manmadetypetext = 'Dam';

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN operating_status_code int2;

UPDATE {script.damWorkingTable} SET dam_name_en = CASE WHEN name1 IS NOT NULL AND name1 != '' THEN name1 ELSE NULL END;
UPDATE {script.damWorkingTable} SET operating_status_code = 
    CASE
    WHEN manmadestatus = 1 THEN 2
    ELSE NULL END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN name1,
    DROP COLUMN manmadestatus;


--split into waterfalls, add new columns, and map attributes
DROP TABLE IF EXISTS {script.fallWorkingTable};
CREATE TABLE {script.fallWorkingTable} AS
    SELECT
        name1,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE obstacletypetext = 'Falls';

ALTER TABLE {script.fallWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.fallWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.fallWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.fallWorkingTable} ADD COLUMN fall_name_en varchar(512);

UPDATE {script.fallWorkingTable} SET fall_name_en = CASE WHEN name1 IS NOT NULL AND name1 != '' THEN name1 ELSE NULL END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.fallWorkingTable}
    DROP COLUMN name1;


--split into fishways, add new columns, and map attributes
DROP TABLE IF EXISTS {script.fishWorkingTable};
CREATE TABLE {script.fishWorkingTable} AS
    SELECT
        name1,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE manmadetypetext = 'Fish Ladder';

ALTER TABLE {script.fishWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.fishWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.fishWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.fishWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.fishWorkingTable} ADD COLUMN dam_name_en varchar(512);

UPDATE {script.fishWorkingTable} SET dam_name_en = CASE WHEN name1 IS NOT NULL AND name1 != '' THEN name1 ELSE NULL END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.fishWorkingTable}
    DROP COLUMN name1;

"""

script.do_work(query)