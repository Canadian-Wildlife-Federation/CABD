import nonspatial as main

script = main.MappingScript("nlp_sep_2008")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetname}',
    '2008-01-01',
    'Newfoundland Power, 2009. 2008 Submission - Sustainable Electricity Program. pp. 4. Accessed from https://secure.newfoundlandpower.com/-/media/PDFs/About-Us/Sustainable-Electricity/SEP-2008-Report.pdf',
    'Accessed February 22, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.workingTable} ADD COLUMN data_source uuid;
UPDATE {script.workingTable} SET data_source = '{script.dsUuid}';

--update new features
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
   generating_capacity_mwh_ds = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetname}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   generating_capacity_mwh_dsfid = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN NULL ELSE cabdsource.generating_capacity_mwh_dsfid END
FROM
   {script.damTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   cabdsource.cabd_id = {script.datasetname}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   generating_capacity_mwh = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetname}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)