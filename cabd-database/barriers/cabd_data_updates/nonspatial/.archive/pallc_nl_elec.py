import nonspatial as main

script = main.MappingScript("pallc_nl_elec")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2015-10-26',
    'Power Advisory LLC, 2015. Review of the Newfoundland and Labrador Electricity System. pp. 40, 45, 47, 52-53, 76. Accessed from https://www.muskratfallsinquiry.ca/files/P-00110.pdf',
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
    facility_name_en_ds = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_en_ds END,
    owner_ds = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.data_source ELSE cabdsource.owner_ds END,
    ownership_type_code_ds = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.ownership_type_code_ds END,
    generating_capacity_mwh_ds = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
    operating_status_code_ds = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.operating_status_code_ds END,
    turbine_number_ds = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.data_source ELSE cabdsource.turbine_number_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
    owner_dsfid = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
    ownership_type_code_dsfid = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN NULL ELSE cabdsource.ownership_type_code_dsfid END,
    generating_capacity_mwh_dsfid = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN NULL ELSE cabdsource.generating_capacity_mwh_dsfid END,
    operating_status_code_dsfid = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN NULL ELSE cabdsource.operating_status_code_dsfid END,
    turbine_number_dsfid = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN NULL ELSE cabdsource.turbine_number_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.existing_pilot_region_pt IS FALSE
    AND (cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    facility_name_en = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.facility_name_en ELSE cabd.facility_name_en END,
    "owner" = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.ownership_type_code ELSE cabd.ownership_type_code END,
    generating_capacity_mwh = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
    operating_status_code = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.operating_status_code ELSE cabd.operating_status_code END,
    turbine_number = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.turbine_number ELSE cabd.turbine_number END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.existing_pilot_region_pt IS FALSE
    AND cabd.cabd_id = {script.datasetName}.cabd_id;


--update pilot region features
UPDATE
   {script.liveDamAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    facility_name_en_ds = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_en_ds END,
    owner_ds = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.data_source ELSE cabdsource.owner_ds END,
    ownership_type_code_ds = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.ownership_type_code_ds END,
    generating_capacity_mwh_ds = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
    operating_status_code_ds = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.operating_status_code_ds END,
    turbine_number_ds = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.data_source ELSE cabdsource.turbine_number_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    facility_name_en_dsfid = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN NULL ELSE cabdsource.facility_name_en_dsfid END,
    owner_dsfid = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN NULL ELSE cabdsource.owner_dsfid END,
    ownership_type_code_dsfid = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN NULL ELSE cabdsource.ownership_type_code_dsfid END,
    generating_capacity_mwh_dsfid = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN NULL ELSE cabdsource.generating_capacity_mwh_dsfid END,
    operating_status_code_dsfid = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN NULL ELSE cabdsource.operating_status_code_dsfid END,
    turbine_number_dsfid = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN NULL ELSE cabdsource.turbine_number_dsfid END
FROM
    {script.liveDamTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.existing_pilot_region_pt IS TRUE
    AND (cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id);

UPDATE
    {script.liveDamTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    facility_name_en = CASE WHEN ({script.datasetName}.facility_name_en IS NOT NULL AND {script.datasetName}.facility_name_en IS DISTINCT FROM cabd.facility_name_en) THEN {script.datasetName}.facility_name_en ELSE cabd.facility_name_en END,
    "owner" = CASE WHEN ({script.datasetName}.owner IS NOT NULL AND {script.datasetName}.owner IS DISTINCT FROM cabd.owner) THEN {script.datasetName}.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN ({script.datasetName}.ownership_type_code IS NOT NULL AND {script.datasetName}.ownership_type_code IS DISTINCT FROM cabd.ownership_type_code) THEN {script.datasetName}.ownership_type_code ELSE cabd.ownership_type_code END,
    generating_capacity_mwh = CASE WHEN ({script.datasetName}.generating_capacity_mwh IS NOT NULL AND {script.datasetName}.generating_capacity_mwh IS DISTINCT FROM cabd.generating_capacity_mwh) THEN {script.datasetName}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
    operating_status_code = CASE WHEN ({script.datasetName}.operating_status_code IS NOT NULL AND {script.datasetName}.operating_status_code IS DISTINCT FROM cabd.operating_status_code) THEN {script.datasetName}.operating_status_code ELSE cabd.operating_status_code END,
    turbine_number = CASE WHEN ({script.datasetName}.turbine_number IS NOT NULL AND {script.datasetName}.turbine_number IS DISTINCT FROM cabd.turbine_number) THEN {script.datasetName}.turbine_number ELSE cabd.turbine_number END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    {script.datasetName}.existing_pilot_region_pt IS TRUE
    AND cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)