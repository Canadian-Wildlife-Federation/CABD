import LOAD_main as main

script = main.LoadingScript("goodd")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = dam_id;
UPDATE {script.sourceTable} SET data_source = '8c7b28eb-164a-4dc6-a121-0f8cb8005215';
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--map to working table (no attributes needed; goodd is geometry source only)
DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT 
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

"""

script.do_work(query)