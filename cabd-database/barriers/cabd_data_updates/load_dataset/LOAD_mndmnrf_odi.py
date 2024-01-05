import LOAD_main as main

script = main.LoadingScript("mndmnrf_odi")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = ogf_id;
UPDATE {script.sourceTable} SET data_source = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}');
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN IF EXISTS fid;

--split into dams, add new columns, and map attributes
DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT 
        dam_name,
        dam_ownership,
        general_comments,
        data_source,
        data_source_id,
        geometry
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_invasivespecies_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN "comments" text;

UPDATE {script.damWorkingTable} SET dam_name_en = dam_name;
UPDATE {script.damWorkingTable} SET "owner" = dam_ownership;
UPDATE {script.damWorkingTable} SET ownership_type_code =
    CASE
    WHEN dam_ownership = 'Conservation Authority' THEN 1
    WHEN dam_ownership = 'Federal' THEN 2
    WHEN dam_ownership = 'Municipal' THEN 3
    WHEN dam_ownership = 'Private' THEN 4
    WHEN dam_ownership = 'Ontario Power Generation' OR dam_ownership = 'Provincial' THEN 5
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_code = 
    CASE
    WHEN dam_name_en ILIKE '%lamprey%' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Invasive species control')
    WHEN dam_ownership = 'Ontario Power Generation' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Hydroelectricity')
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_invasivespecies_code = (SELECT code FROM dams.use_codes WHERE name_en = 'Main') WHERE dam_name_en ILIKE '%lamprey%';
UPDATE {script.damWorkingTable} SET use_electricity_code = (SELECT code FROM dams.use_codes WHERE name_en = 'Main') WHERE dam_ownership = 'Ontario Power Generation';
UPDATE {script.damWorkingTable} SET "comments" = general_comments;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN dam_name,
    DROP COLUMN dam_ownership,
    DROP COLUMN general_comments;
"""

script.do_work(query)

# load supplementary dataset of additional matches to CABD features and new features
print("Loading additional ODI matches and new features")

tableName = "source_data.odi_matches"
file = input("Filepath of additional ODI features:")
# file = r"C:\Users\kohearn\Canadian Wildlife Federation\Conservation Science General - Documents\Freshwater\Fish Passage\National Aquatic Barrier Database\GIS work folder\database_updates\nov_2023_updates\odi_missing_dams.gpkg"

orgDb="dbname='" + main.dbName + "' host='"+ main.dbHost+"' port='"+main.dbPort+"' user='"+main.dbUser+"' password='"+ main.dbPassword+"'"
pycmd = '"' + main.ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + tableName + '" -overwrite -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + '"' + file + '"'
print(pycmd)
main.subprocess.run(pycmd)
