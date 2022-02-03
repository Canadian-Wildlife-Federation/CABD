import LOAD_main as main

script = main.LoadingScript("nbwf")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = nb_id;
UPDATE {script.sourceTable} SET data_source = '7d073b9a-4e1c-41b4-aabf-107996fa98fd';
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;

--add new columns and map attributes
DROP TABLE IF EXISTS {script.fallWorkingTable};
CREATE TABLE {script.fallWorkingTable} AS
    SELECT
        name,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.fallWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.fallWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.fallWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.fallWorkingTable} ADD COLUMN fall_name_en varchar(512);

UPDATE {script.fallWorkingTable} SET fall_name_en = name;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.fallWorkingTable}
    DROP COLUMN name;

"""

script.do_work(query)