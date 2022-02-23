import nonspatial as main

script = main.MappingScript("dfo_atl_sal_rehab_ns")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2022-02-08',
    'Department of Fisheries and Forestry of Canada - Resource Development Branch - Fisheries Service, no date. Atlantic Salmon Rehabilitation Project - East River, Sheet Harbour, Nova Scotia. Halifax, NS: Department of Fisheries and Forestry of Canada. Accessed February 8, 2022 from https://waves-vagues.dfo-mpo.gc.ca/Library/83454.pdf',
    'Accessed February 8, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} SET data_source = '{script.dsUuid}';

--update new features
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
    facility_name_en_ds = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_en_ds END,
    function_code_ds = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
    use_code_ds = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_code_ds END,
    use_supply_code_ds = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_supply_code_ds END,
    use_electricity_code_ds = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetName}.data_source ELSE cabdsource.use_electricity_code_ds END,
    waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    up_passage_type_code_ds = CASE WHEN ({script.datasetName}.up_passage_type_code IS NOT NULL AND {script.datasetName}.up_passage_type_code IS DISTINCT FROM cabd.up_passage_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.up_passage_type_code_ds END,

    facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
    function_code_dsfid = CASE WHEN ({script.datasetName}.function_code IS NOT NULL AND {script.datasetName}.function_code IS DISTINCT FROM cabd.function_code) THEN NULL ELSE cabdsource.function_code_dsfid END,
    use_code_dsfid = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN NULL ELSE cabdsource.use_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN NULL ELSE cabdsource.use_supply_code_dsfid END,
    use_electricity_code_dsfid = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN NULL ELSE cabdsource.use_electricity_code_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN NULL ELSE cabdsource.waterbody_name_en_dsfid END,
    up_passage_type_code_dsfid = CASE WHEN ({script.datasetName}.up_passage_type_code IS NOT NULL AND {script.datasetName}.up_passage_type_code IS DISTINCT FROM cabd.up_passage_type_code) THEN NULL ELSE cabdsource.up_passage_type_code_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.fishway_yn IS FALSE
    AND (cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);;

UPDATE
    {script.damTable} AS cabd
SET
    facility_name_en = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.facility_name_en ELSE cabd.facility_name_en END,
    function_code = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    use_code = CASE WHEN ({script.datasetName}.use_code IS NOT NULL AND {script.datasetName}.use_code IS DISTINCT FROM cabd.use_code) THEN {script.datasetName}.use_code ELSE cabd.use_code END,
    use_supply_code = CASE WHEN ({script.datasetName}.use_supply_code IS NOT NULL AND {script.datasetName}.use_supply_code IS DISTINCT FROM cabd.use_supply_code) THEN {script.datasetName}.use_supply_code ELSE cabd.use_supply_code END,
    use_electricity_code = CASE WHEN ({script.datasetName}.use_electricity_code IS NOT NULL AND {script.datasetName}.use_electricity_code IS DISTINCT FROM cabd.use_electricity_code) THEN {script.datasetName}.use_electricity_code ELSE cabd.use_electricity_code END,
    waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    up_passage_type_code = CASE WHEN ({script.datasetName}.up_passage_type_code IS NOT NULL AND {script.datasetName}.up_passage_type_code IS DISTINCT FROM cabd.up_passage_type_code) THEN {script.datasetName}.up_passage_type_code ELSE cabd.up_passage_type_code END
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
    waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    year_constructed_ds = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN {script.datasetName}.data_source ELSE cabdsource.year_constructed_ds END,
    fishpass_type_code_ds = CASE WHEN ({script.datasetName}.fishpass_type_code IS NOT NULL AND {script.datasetName}.fishpass_type_code IS DISTINCT FROM cabd.fishpass_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.fishpass_type_code_ds END,
    purpose_ds = CASE WHEN ({script.datasetName}.purpose IS NOT NULL AND {script.datasetName}.purpose IS DISTINCT FROM cabd.purpose) THEN {script.datasetName}.data_source ELSE cabdsource.purpose_ds END,
    operation_period_ds = CASE WHEN ({script.datasetName}.operation_period IS NOT NULL AND {script.datasetName}.operation_period IS DISTINCT FROM cabd.operation_period) THEN {script.datasetName}.data_source ELSE cabdsource.operation_period_ds END,
    operating_notes_ds = CASE WHEN ({script.datasetName}.operating_notes IS NOT NULL AND {script.datasetName}.operating_notes IS DISTINCT FROM cabd.operating_notes) THEN {script.datasetName}.data_source ELSE cabdsource.operating_notes_ds END,

    structure_name_en_dsfid = CASE WHEN ({script.datasetName}.structure_name_en IS NOT NULL AND {script.datasetName}.structure_name_en IS DISTINCT FROM cabd.structure_name_en) THEN NULL ELSE cabdsource.structure_name_en_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN NULL ELSE cabdsource.waterbody_name_en_dsfid END,
    year_constructed_dsfid = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN NULL ELSE cabdsource.year_constructed_dsfid END,
    fishpass_type_code_dsfid = CASE WHEN ({script.datasetName}.fishpass_type_code IS NOT NULL AND {script.datasetName}.fishpass_type_code IS DISTINCT FROM cabd.fishpass_type_code) THEN NULL ELSE cabdsource.fishpass_type_code_dsfid END,
    purpose_dsfid = CASE WHEN ({script.datasetName}.purpose IS NOT NULL AND {script.datasetName}.purpose IS DISTINCT FROM cabd.purpose) THEN NULL ELSE cabdsource.purpose_dsfid END,
    operation_period_dsfid = CASE WHEN ({script.datasetName}.operation_period IS NOT NULL AND {script.datasetName}.operation_period IS DISTINCT FROM cabd.operation_period) THEN NULL ELSE cabdsource.operation_period_dsfid END,
    operating_notes_dsfid = CASE WHEN ({script.datasetName}.operating_notes IS NOT NULL AND {script.datasetName}.operating_notes IS DISTINCT FROM cabd.operating_notes) THEN NULL ELSE cabdsource.operating_notes_dsfid END
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
    waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    year_constructed = CASE WHEN ({script.datasetName}.year_constructed IS NOT NULL AND {script.datasetName}.year_constructed IS DISTINCT FROM cabd.year_constructed) THEN {script.datasetName}.year_constructed ELSE cabd.year_constructed END,
    fishpass_type_code = CASE WHEN ({script.datasetName}.fishpass_type_code IS NOT NULL AND {script.datasetName}.fishpass_type_code IS DISTINCT FROM cabd.fishpass_type_code) THEN {script.datasetName}.fishpass_type_code ELSE cabd.fishpass_type_code END,
    purpose = CASE WHEN ({script.datasetName}.purpose IS NOT NULL AND {script.datasetName}.purpose IS DISTINCT FROM cabd.purpose) THEN {script.datasetName}.purpose ELSE cabd.purpose END,
    operation_period = CASE WHEN ({script.datasetName}.operation_period IS NOT NULL AND {script.datasetName}.operation_period IS DISTINCT FROM cabd.operation_period) THEN {script.datasetName}.operation_period ELSE cabd.operation_period END,
    operating_notes = CASE WHEN ({script.datasetName}.operating_notes IS NOT NULL AND {script.datasetName}.operating_notes IS DISTINCT FROM cabd.operating_notes) THEN {script.datasetName}.operating_notes ELSE cabd.operating_notes END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.fishway_yn IS TRUE
    AND cabd.cabd_id = {script.datasetName}.cabd_id;
"""

script.do_work(mappingquery)