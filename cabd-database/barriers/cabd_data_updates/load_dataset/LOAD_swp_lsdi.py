import LOAD_main as main

script = main.LoadingScript("swp_lsdi")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = BID;
UPDATE {script.sourceTable} SET data_source = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}');
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
        Barr_Type,
        Names_Comb,
        OWNER_NAME,
        Struct_Ht,
        MainDmWdth,
        Blk_Lmp,
        HydroPwr,
        Comments,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE Barr_Type <> 'Waterfall';

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_invasivespecies_code int2;
ALTER TABLE {script.damWorkingTable} RENAME COLUMN Comments TO comments_orig;
ALTER TABLE {script.damWorkingTable} ADD COLUMN "comments" text;

UPDATE {script.damWorkingTable} SET dam_name_en = initcap(Names_Comb);
UPDATE {script.damWorkingTable} SET "owner" = 
    CASE
    WHEN OWNER_NAME = 'BP' THEN 'Brookfield Renewable Partners'
    ELSE OWNER_NAME END;
UPDATE {script.damWorkingTable} SET ownership_type_code = 
    CASE
    WHEN "owner" = 'DFO' THEN 2
    WHEN "owner" = 'MNR' THEN 5
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET height_m = Struct_Ht WHERE Struct_Ht > 0;
UPDATE {script.damWorkingTable} SET length_m = MainDmWdth WHERE MainDmWdth > 0;
UPDATE {script.damWorkingTable} SET construction_type_code =
    CASE
    WHEN Barr_Type = 'Embankment only' THEN 3
    WHEN Barr_Type ILIKE '%Steel Sheetpile%' THEN 7
    WHEN Barr_Type IS NULL THEN NULL
    ELSE 10 END;
UPDATE {script.damWorkingTable} SET use_code =
    CASE
    WHEN Barr_Type = 'Hydro' OR HydroPwr = 'Yes' THEN 2
    WHEN Blk_Lmp IN ('1', '2') AND Barr_Type <> 'Hydro' THEN 9
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_electricity_code = 1 WHERE Barr_Type = 'Hydro' OR HydroPwr = 'Yes';
UPDATE {script.damWorkingTable} SET use_invasivespecies_code =
    CASE
    WHEN Blk_Lmp IN ('1', '2') AND use_electricity_code IS NULL THEN 1
    WHEN Blk_Lmp IN ('1', '2') AND use_electricity_code = 1 THEN 2
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET "comments" = comments_orig;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN Barr_Type,
    DROP COLUMN Names_Comb,
    DROP COLUMN OWNER_NAME,
    DROP COLUMN Struct_Ht,
    DROP COLUMN MainDmWdth,
    DROP COLUMN Blk_Lmp,
    DROP COLUMN HydroPwr,
    DROP COLUMN comments_orig;

"""

script.do_work(query)
