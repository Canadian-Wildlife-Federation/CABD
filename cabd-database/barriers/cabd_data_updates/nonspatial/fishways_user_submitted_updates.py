import nonspatial as main

script = main.MappingScript("user_submitted_updates")

mappingquery = f"""

--TO DO: confirm with Nick that this constraint is an appropriate solution
--possible that we want to keep this constraint on all the time, but for now just add and delete
--prevents multiple data sources being added for the same data source based on names in csv

ALTER TABLE cabd.data_source ADD CONSTRAINT unique_name UNIQUE (name);

INSERT INTO cabd.data_source (name, id, source_type)
    SELECT DISTINCT data_source, uuid_generate_v4(), 'non-spatial' FROM {script.sourceTable}
    ON CONFLICT DO NOTHING;

ALTER TABLE cabd.data_source DROP CONSTRAINT unique_name;

--add data source ids to the table
ALTER TABLE {script.sourceTable} RENAME COLUMN data_source to data_source_text;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} AS s SET data_source = d.id FROM cabd.data_source AS d WHERE d.name = s.data_source_text;

--SELECT DISTINCT data_source, data_source_text FROM {script.sourceTable};

--TO DO: figure out how we want to handle the DISTINCT clause for coded value fields
--users will be giving us the name instead of the id for these in the template
--possible solution: run a pre-processing script on the CSV input to switch these values?

--update existing features 
UPDATE
    {script.liveFishAttributeTable} AS cabdsource
SET    
    assess_schedule_ds = CASE WHEN ({script.datasetName}.assess_schedule IS NOT NULL AND {script.datasetName}.assess_schedule IS DISTINCT FROM cabd.assess_schedule) THEN {script.datasetName}.data_source ELSE cabdsource.assess_schedule_ds END
FROM
    {script.liveFishTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.liveFishTable} AS cabd
SET
    assess_schedule = CASE WHEN ({script.datasetName}.assess_schedule IS NOT NULL AND {script.datasetName}.assess_schedule IS DISTINCT FROM cabd.assess_schedule) THEN {script.datasetName}.assess_schedule ELSE cabd.assess_schedule END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)
