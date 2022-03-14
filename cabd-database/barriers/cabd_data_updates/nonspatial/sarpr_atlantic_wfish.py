import nonspatial as main

script = main.MappingScript("sarpr_atlantic_wfish")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2019-04-17',
    'Government of Canada, 2019. Recovery Strategy for the Atlantic Whitefish; Background: Threats and Critical Habitat. Accessed February 8, 2022, from https://sararegistry.gc.ca/default.asp?lang=En&n=B2C0EB3D-1&offset=5',
    'Accessed February 8, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} SET data_source = '{script.dsUuid}';

--update new features
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    function_code_ds = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
    use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
    use_supply_code_ds = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_supply_code_ds END,
    construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
    construction_year_ds = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.data_source ELSE cabdsource.construction_year_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    function_code_dsfid = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END,
    use_code_dsfid = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN NULL ELSE cabdsource.use_supply_code_dsfid END,
    construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
    construction_year_dsfid = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.fishway_yn IS FALSE
    AND (cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    function_code = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.function_code ELSE cabd.function_code END,
    use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
    use_supply_code = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.use_supply_code ELSE cabd.use_supply_code END,
    construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
    construction_year = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.construction_year ELSE cabd.construction_year END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.fishway_yn IS FALSE
    AND cabd.cabd_id = {script.datasetName}.cabd_id;


--update fishway features
UPDATE
   {script.fishAttributeTable} AS cabdsource
SET    
    structure_name_en_ds = CASE WHEN ({script.datasetName}.structure_name_en IS NOT NULL AND {script.datasetName}.structure_name_en IS DISTINCT FROM cabd.structure_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.structure_name_en_ds END,
    year_constructed_ds = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN {script.datasetName}.data_source ELSE cabdsource.year_constructed_ds END,

    structure_name_en_dsfid = CASE WHEN ({script.datasetName}.structure_name_en IS NOT NULL AND {script.datasetName}.structure_name_en IS DISTINCT FROM cabd.structure_name_en) THEN NULL ELSE cabdsource.structure_name_en_dsfid END,
    year_constructed_dsfid = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN NULL ELSE cabdsource.year_constructed_dsfid END
FROM
    {script.fishTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.fishway_yn IS TRUE
    AND (cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);

UPDATE
    {script.fishTable} AS cabd
SET
    structure_name_en = CASE WHEN ({script.datasetName}.structure_name_en IS NOT NULL AND {script.datasetName}.structure_name_en IS DISTINCT FROM cabd.structure_name_en) THEN {script.datasetName}.structure_name_en ELSE cabd.structure_name_en END,
    year_constructed = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN {script.datasetName}.year_constructed ELSE cabd.year_constructed END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.fishway_yn IS TRUE
    AND cabd.cabd_id = {script.datasetName}.cabd_id;
"""

script.do_work(mappingquery)