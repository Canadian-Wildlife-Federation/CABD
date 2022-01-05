import LOAD_main as main

script = main.LoadingScript("nswf")

query = f"""
--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = shape_fid;
UPDATE {script.sourceTable} SET data_source = '3f5c9d6e-4d4f-48af-b57d-cb2a1e2671a0';
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
        rivname_1,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE _____ = ______;

ALTER TABLE {script.damWorkingTable} ADD COLUMN waterbody_name_en varchar(512);

UPDATE {script.damWorkingTable} SET waterbody_name_en = rivname_1;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN rivname_1;


--split into waterfalls, add new columns, and map attributes
DROP TABLE IF EXISTS {script.fallWorkingTable};
CREATE TABLE {script.fallWorkingTable} AS
    SELECT 
        rivname_1,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE _____ = ______;

ALTER TABLE {script.fallWorkingTable} ADD COLUMN waterbody_name_en varchar(512);

UPDATE {script.fallWorkingTable} SET waterbody_name_en = rivname_1;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.fallWorkingTable}
    DROP COLUMN rivname_1;


--split into waterfalls, add new columns, and map attributes
DROP TABLE IF EXISTS {script.fishWorkingTable};
CREATE TABLE {script.fishWorkingTable} AS
    SELECT 
        rivname_1,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE _____ = ______;

ALTER TABLE {script.fishWorkingTable} ADD COLUMN waterbody_name_en varchar(512);

UPDATE {script.fishWorkingTable} SET waterbody_name_en = rivname_1;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.fishWorkingTable}
    DROP COLUMN rivname_1;


"""

script.do_work(query)