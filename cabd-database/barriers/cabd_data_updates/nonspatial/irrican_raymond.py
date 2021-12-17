import nonspatial as main

script = main.MappingScript("irrican_raymond")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Raymond Reservoir Power Plant Fact Sheet', now(), null, 'Irrican Power', 'Data update - ' || now());

--add data source to the table
ALTER TABLE {script.workingTable} ADD COLUMN data_source uuid;
UPDATE {script.workingTable} SET data_source = '{script.dsUuid}';

--update existing features 
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
   reservoir_present_ds = CASE WHEN {script.datasetname}.reservoir_present IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_present_ds END,
   reservoir_name_en_ds = CASE WHEN {script.datasetname}.reservoir_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_name_en_ds END,
   avg_rate_of_discharge_ls_ds = CASE WHEN {script.datasetname}.avg_rate_of_discharge_ls IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.avg_rate_of_discharge_ls_ds END,
   turbine_type_code_ds = CASE WHEN {script.datasetname}.turbine_type_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.turbine_type_code_ds END,
   turbine_number_ds = CASE WHEN {script.datasetname}.turbine_number IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.turbine_number_ds END,
   generating_capacity_mwh_ds = CASE WHEN {script.datasetname}.generating_capacity_mwh IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.generating_capacity_mwh_ds END
FROM
   {script.damTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   reservoir_present = CASE WHEN {script.datasetname}.reservoir_present IS NOT NULL THEN {script.datasetname}.reservoir_present ELSE cabd.reservoir_present END,
   reservoir_name_en = CASE WHEN {script.datasetname}.reservoir_name_en IS NOT NULL THEN {script.datasetname}.reservoir_name_en ELSE cabd.reservoir_name_en END,
   avg_rate_of_discharge_ls = CASE WHEN {script.datasetname}.avg_rate_of_discharge_ls IS NOT NULL THEN {script.datasetname}.avg_rate_of_discharge_ls ELSE cabd.avg_rate_of_discharge_ls END,
   turbine_type_code = CASE WHEN {script.datasetname}.turbine_type_code IS NOT NULL THEN {script.datasetname}.turbine_type_code ELSE cabd.turbine_type_code END,
   turbine_number = CASE WHEN {script.datasetname}.turbine_number IS NOT NULL THEN {script.datasetname}.turbine_number ELSE cabd.turbine_number END,
   generating_capacity_mwh = CASE WHEN {script.datasetname}.generating_capacity_mwh IS NOT NULL THEN {script.datasetname}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)