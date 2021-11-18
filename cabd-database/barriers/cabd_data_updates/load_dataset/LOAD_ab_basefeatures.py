import LOAD_main as main

script = main.LoadingScript("ab_basefeatures")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;
UPDATE {script.sourceTable} SET data_source_id = bf_id;
UPDATE {script.sourceTable} SET data_source = '85e725a2-bb6d-45d5-a6c5-1bf7ceed28db';
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;

--split into dams and map attributes

DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT name, data_source, data_source_id FROM {script.sourceTable} WHERE feature_type IN ('DAM-MAJ','DAM-MIN');

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
UPDATE {script.damWorkingTable} SET dam_name_en = name;

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable}
    DROP COLUMN name;

--split into waterfalls and map attributes

DROP TABLE IF EXISTS {script.fallWorkingTable};
CREATE TABLE {script.fallWorkingTable} AS
    SELECT name, data_source, data_source_id FROM {script.sourceTable} WHERE feature_type = 'FALLS';

ALTER TABLE {script.fallWorkingTable} ADD COLUMN fall_name_en varchar(512);
UPDATE {script.fallWorkingTable} SET fall_name_en = name;

ALTER TABLE {script.fallWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.fallWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.fallWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.fallWorkingTable}
    DROP COLUMN name;
"""

script.do_work(query)