import nonspatial as main

script = main.MappingScript("nlhydro_ops_hydro")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetname}',
    '2022-02-08',
    'Newfoundland and Labrador Hydro, no date. Operations - Hydro Generation. Accessed February 8, 2022, from https://nlhydro.com/operations/hydro-generation/',
    'Accessed February 8, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.workingTable} ADD COLUMN data_source uuid;
UPDATE {script.workingTable} SET data_source = '{script.dsUuid}';

--update new features
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
   dam_name_en_ds = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
   facility_name_en_ds = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetname}.data_source ELSE cabdsource.facility_name_en_ds END,
   owner_ds = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,
   ownership_type_code_ds = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,
   function_code_ds = CASE WHEN ({script.datasetname}.function_code IS NOT NULL AND {script.datasetname}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetname}.data_source ELSE cabdsource.function_code_ds END,
   generating_capacity_mwh_ds = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetname}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
   construction_year_ds = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
   turbine_type_code_ds = CASE WHEN ({script.datasetname}.turbine_type_code IS NOT NULL AND {script.datasetname}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN {script.datasetname}.data_source ELSE cabdsource.turbine_type_code_ds END,
   
   dam_name_en_dsfid = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
   facility_name_en_dsfid = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
   owner_dsfid = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
   ownership_type_code_dsfid = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN NULL ELSE cabdsource.ownership_type_code_dsfid END,
   function_code_dsfid = CASE WHEN ({script.datasetname}.function_code IS NOT NULL AND {script.datasetname}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END,
   generating_capacity_mwh_dsfid = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN NULL ELSE cabdsource.generating_capacity_mwh_dsfid END,
   construction_year_dsfid = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
   turbine_type_code_dsfid = CASE WHEN ({script.datasetname}.turbine_type_code IS NOT NULL AND {script.datasetname}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN NULL ELSE cabdsource.turbine_type_code_dsfid END
FROM
   {script.damTable} AS cabd,
   {script.workingTable} AS {script.datasetname}
WHERE
   cabdsource.cabd_id = {script.datasetname}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
   {script.damTable} AS cabd
SET
   dam_name_en = CASE WHEN ({script.datasetname}.dam_name_en IS NOT NULL AND {script.datasetname}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
   facility_name_en = CASE WHEN ({script.datasetname}.facility_name_en IS NOT NULL AND {script.datasetname}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetname}.facility_name_en ELSE cabd.facility_name_en END,
   "owner" = CASE WHEN ({script.datasetname}.owner IS NOT NULL AND {script.datasetname}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetname}.owner ELSE cabd.owner END,
   ownership_type_code = CASE WHEN ({script.datasetname}.ownership_type_code IS NOT NULL AND {script.datasetname}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END,
   function_code = CASE WHEN ({script.datasetname}.function_code IS NOT NULL AND {script.datasetname}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetname}.function_code ELSE cabd.function_code END,
   generating_capacity_mwh = CASE WHEN ({script.datasetname}.generating_capacity_mwh IS NOT NULL AND {script.datasetname}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetname}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
   construction_year = CASE WHEN ({script.datasetname}.construction_year IS NOT NULL AND {script.datasetname}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
   turbine_type_code = CASE WHEN ({script.datasetname}.turbine_type_code IS NOT NULL AND {script.datasetname}.turbine_type_code IS DISTINCT FROM cabd.turbine_type_code) THEN {script.datasetname}.turbine_type_code ELSE cabd.turbine_type_code END
FROM
   {script.workingTable} AS {script.datasetname}
WHERE
   cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)