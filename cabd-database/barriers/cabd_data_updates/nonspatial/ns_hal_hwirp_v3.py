import nonspatial as main

script = main.MappingScript("ns_hal_hwirp_v3")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2012-10-31',
    'Halifax Water, 2012. Halifax Water Integrated Resource Plan; Volume 3 Appendix A - Water System Review. pp. 41-42. Accessed from https://www.halifax.ca/sites/default/files/documents/home-property/water/HW_IntegratedResourcePlan_AppendixA.pdf',
    'Accessed February 22, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} SET data_source = '{script.dsUuid}';

--update new features
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    owner_ds = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.data_source ELSE cabdsource.owner_ds END,
    use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
    use_supply_code_ds = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_supply_code_ds END,
    use_pollution_code_ds = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_pollution_code_ds END,
    function_code_ds = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
    construction_type_code_ds = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,
    spillway_type_code_ds = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.spillway_type_code_ds END,
    construction_year_ds = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.data_source ELSE cabdsource.construction_year_ds END,
    maintenance_last_ds = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.data_source ELSE cabdsource.maintenance_last_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    owner_dsfid = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
    use_code_dsfid = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN NULL ELSE cabdsource.use_supply_code_dsfid END,
    use_pollution_code_dsfid = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN NULL ELSE cabdsource.use_pollution_code_dsfid END,
    function_code_dsfid = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END,
    construction_type_code_dsfid = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN NULL ELSE cabdsource.construction_type_code_dsfid END,
    spillway_type_code_dsfid = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN NULL ELSE cabdsource.spillway_type_code_dsfid END,
    construction_year_dsfid = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN NULL ELSE cabdsource.construction_year_dsfid END,
    maintenance_last_dsfid = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN NULL ELSE cabdsource.maintenance_last_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    "owner" = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.owner ELSE cabd.owner END,
    use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
    use_supply_code = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.use_supply_code ELSE cabd.use_supply_code END,
    use_pollution_code = CASE WHEN ({script.datasetName}.use_pollution_code IS NOT NULL AND {script.datasetName}.use_pollution_code IS DISTINCT FROM cabd.use_pollution_code) THEN {script.datasetName}.use_pollution_code ELSE cabd.use_pollution_code END,
    function_code = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.function_code ELSE cabd.function_code END,
    construction_type_code = CASE WHEN ({script.datasetName}.construction_type_code IS NOT NULL AND {script.datasetName}.construction_type_code IS DISTINCT FROM cabd.construction_type_code) THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
    spillway_type_code = CASE WHEN ({script.datasetName}.spillway_type_code IS NOT NULL AND {script.datasetName}.spillway_type_code IS DISTINCT FROM cabd.spillway_type_code) THEN {script.datasetName}.spillway_type_code ELSE cabd.spillway_type_code END,
    construction_year = CASE WHEN ({script.datasetName}.construction_year IS NOT NULL AND {script.datasetName}.construction_year IS DISTINCT FROM cabd.construction_year) THEN {script.datasetName}.construction_year ELSE cabd.construction_year END,
    maintenance_last = CASE WHEN ({script.datasetName}.maintenance_last IS NOT NULL AND {script.datasetName}.maintenance_last IS DISTINCT FROM cabd.maintenance_last) THEN {script.datasetName}.maintenance_last ELSE cabd.maintenance_last END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)