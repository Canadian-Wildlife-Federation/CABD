import user_submit as main

#change the datasetName below for each round of updates
script = main.MappingScript("user_submitted_updates")

#probably need to create a pre-processing script that cleans the CSV to run beforehand
#that deals with coded value fields, trims fields, etc.

mappingquery = f"""
--add new data sources and match new uuids for each data source to each record

--TO DO: confirm with Nick that this is an appropriate solution
--possible that we want to keep this constraint on all the time, but for now just add and delete

ALTER TABLE cabd.data_source ADD CONSTRAINT unique_name (name);

INSERT INTO cabd.data_source (name, id, source_type)
    SELECT DISTINCT data_source, gen_random_uuid(), 'non-spatial' FROM {script.sourceTable}
    ON CONFLICT DO NOTHING;

ALTER TABLE cabd.data_source DROP CONSTRAINT unique_name;
dam
--add data source ids to the table
ALTER TABLE {script.sourceTable} RENAME COLUMN data_source to data_source_text;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} AS s SET data_source = d.id FROM cabd.data_source AS d WHERE d.name = s.data_source_text;

--TO DO: create damTableNewModify, damTableDelete with proper format

--split into new/modified and deleted features
--TO DECIDE: these queries will only insert records, we'll have to clear old records at some point?

INSERT INTO {script.damTableNewModify} (
    cabd_id,
    entry_classification,
    data_source,
    latitude,
    longitude,
    use_analysis,
    dam_name_en
)
SELECT
    cabd_id,
    entry_classification,
    data_source,
    latitude,
    longitude,
    use_analysis,
    dam_name_en
FROM {script.sourceTable} WHERE entry_classification IN ('new feature', 'modify feature');

INSERT INTO {script.damTableDelete} (
    cabd_id,
    entry_classification,
    data_source,
    latitude,
    longitude,
    use_analysis,
    dam_name_en
)
SELECT
    cabd_id,
    entry_classification,
    data_source,
    latitude,
    longitude,
    use_analysis,
    dam_name_en
FROM {script.sourceTable} WHERE entry_classification = 'delete feature';

"""
#print(mappingquery)
script.do_work(mappingquery)
