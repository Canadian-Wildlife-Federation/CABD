import LOAD_main as main

script = main.LoadingScript("nbhn_hy_obstacles")

query = f"""
--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = nid;
UPDATE {script.sourceTable} SET data_source = 'd224a8ba-7e57-4ef6-80c5-9d883d012226';
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;

--add new columns, and map attributes
DROP TABLE IF EXISTS {script.fallWorkingTable};
CREATE TABLE {script.fallWorkingTable} AS
    SELECT 
        name_1,
        local_name,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.fallWorkingTable} ADD COLUMN fall_name_en varchar(512);

UPDATE {script.fallWorkingTable} SET fall_name_en = 
    CASE 
    WHEN name_1 IS NOT NULL THEN name_1
    WHEN name_1 IS NULL THEN local_name
    ELSE NULL END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.fallWorkingTable}
    DROP COLUMN name_1,
    DROP COLUMN local_name;

"""

script.do_work(query)