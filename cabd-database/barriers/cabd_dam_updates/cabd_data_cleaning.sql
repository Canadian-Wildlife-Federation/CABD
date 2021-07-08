--Run this script before you update any attributes/run the update scripts.
--This script cleans up some columns that were populated incorrectly
--when the first data loading was done on the CABD table.

--Original GRanD script used the USE_IRRI field for all of these codes
BEGIN;

UPDATE
    [cabd_table]
SET 
    use_electricity_code = NULL,
    use_supply_code = NULL,
    use_floodcontrol_code = NULL,
    use_recreation_code = NULL,
    use_navigation_code = NULL,
    use_fish_code = NULL,
    use_pollution_code = NULL,
    use_invasivespecies_code = NULL,
    use_other_code = NULL
WHERE
    data_source = 'GRanD_Database_v1.3';

--Fix two individual points who didn't get a data_source_id or data_source
UPDATE
    [cabd_table]
SET
    data_source_id = '523',
    data_source = 'NPDP'
WHERE
    waterbody_name_en = 'Sixteen Mile'
    AND data_source IS NULL;

UPDATE
    [cabd_table]
SET
    data_source_id = '787',
    data_source = 'NPDP'
WHERE
    waterbody_name_en = 'Junction Brook'
    AND data_source IS NULL;

--Dam_name_en and nearest_municipality fields were populated incorrectly in original NPDP script
UPDATE
    [cabd_table]
SET
    dam_name_en = NULL,
    nearest_municipality = NULL
WHERE
    data_source = 'NPDP';

--Original scripts imported some column values as mcm and some as cubic metres
UPDATE [cabd_table] SET storage_capacity_mcm = NULL;

--Construction_type_code populated incorrectly in original load scripts
UPDATE [cabd_table] SET construction_type_code = NULL WHERE data_source IN ('Public_Dams_KML','WRIS_Public_Dams');

--Merge the data_source_id and data_source fields
ALTER TABLE [cabd_table] ADD COLUMN data_source_comb text;
UPDATE [cabd_table] SET data_source_comb = data_source || '_' || data_source_id;
ALTER TABLE [cabd_table] DROP COLUMN data_source;
ALTER TABLE [cabd_table] DROP COLUMN data_source_id;
ALTER TABLE [cabd_table] RENAME COLUMN data_source_comb TO data_source;
ALTER TABLE [cabd_table] ALTER COLUMN data_source SET NOT NULL;

COMMIT;