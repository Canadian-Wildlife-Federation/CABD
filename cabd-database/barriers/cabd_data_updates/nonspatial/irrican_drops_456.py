import nonspatial as main

script = main.MappingScript("irrican_drops_456")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Drops 4, 5, and 6 Power Plant Fact Sheet', now(), null, 'Irrican Power', 'Data update - ' || now());

--add data source to the table
UPDATE TABLE {script.workingTable} ADD COLUMN data_source varchar(512);
UPDATE TABLE {script.workingTable} SET data_source = {script.dsUuid};

--update existing features 
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
   height_m_ds = CASE WHEN {script.datasetname}.height_m IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.height_m_ds END,
   turbine_number_ds = CASE WHEN {script.datasetname}.turbine_number IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.turbine_number_ds END,
   turbine_type_code_ds = CASE WHEN {script.datasetname}.turbine_type_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.turbine_type_code_ds END,
   generating_capacity_mwh_ds = CASE WHEN {script.datasetname}.generating_capacity_mwh IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
   use_code_ds = CASE WHEN {script.datasetname}.use_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_code_ds END,
   use_electricity_code_ds = CASE WHEN {script.datasetname}.use_electricity_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END,
   avg_rate_of_discharge_ls_ds = CASE WHEN {script.datasetname}.avg_rate_of_discharge_ls IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.avg_rate_of_discharge_ls_ds END,
   owner_ds = CASE WHEN {script.datasetname}.owner IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,
   ownership_type_code_ds = CASE WHEN {script.datasetname}.ownership_type_code IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END
FROM
   {script.damTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   height_m = CASE WHEN {script.datasetname}.height_m IS NOT NULL THEN {script.datasetname}.height_m ELSE cabd.height_m END,
   turbine_number = CASE WHEN {script.datasetname}.turbine_number IS NOT NULL THEN {script.datasetname}.turbine_number ELSE cabd.turbine_number END,
   turbine_type_code = CASE WHEN {script.datasetname}.turbine_type_code IS NOT NULL THEN {script.datasetname}.turbine_type_code ELSE cabd.turbine_type_code END,
   generating_capacity_mwh = CASE WHEN {script.datasetname}.generating_capacity_mwh IS NOT NULL THEN {script.datasetname}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
   use_code = CASE WHEN {script.datasetname}.use_code IS NOT NULL THEN {script.datasetname}.use_code ELSE cabd.use_code END,
   use_electricity_code = CASE WHEN {script.datasetname}.use_electricity_code IS NOT NULL THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END,
   avg_rate_of_discharge_ls = CASE WHEN {script.datasetname}.avg_rate_of_discharge_ls IS NOT NULL THEN {script.datasetname}.avg_rate_of_discharge_ls ELSE cabd.avg_rate_of_discharge_ls END,
   "owner" = CASE WHEN {script.datasetname}."owner" IS NOT NULL THEN {script.datasetname}."owner" ELSE cabd."owner" END,
   ownership_type_code = CASE WHEN {script.datasetname}.ownership_type_code IS NOT NULL THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)