import MAP_attributes_main as main

script = main.MappingScript("mndmnrf_odi")

mappingquery = f"""

UPDATE cabd.data_source
SET
    version_date = '2023-11-13',
    source = 'Ontario Ministry of Natural Resources and Forestry, 2014. Ontario Dam Inventory. Ontario GeoHub. Accessed November 13, 2023, from https://geohub.lio.gov.on.ca/datasets/mnrf::ontario-dam-inventory/about',
    comments = 'Accessed November 13, 2023',
    licence = 'https://www.ontario.ca/page/open-government-licence-ontario'
WHERE name = 'mndmnrf_odi';

--special handling for odi
--we have some additional matches that were not recorded in the feature_source table
--comment out once no longer needed
INSERT INTO {script.damSourceTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
)
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}'),
    ogf_id
FROM
    source_data.odi_matches
WHERE cabd_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

--fix an incorrect odi feature
UPDATE {script.damSourceTable} SET datasource_feature_id = '108413024' WHERE cabd_id = '5455f5fd-07d0-4b94-9968-f351048d97be';

--find CABD IDs
UPDATE {script.workingTable} SET cabd_id = NULL;

UPDATE
    {script.workingTable}
SET
    cabd_id = a.cabd_id
FROM
    {script.damSourceTable} AS a
WHERE
    a.datasource_id = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}')
    AND a.datasource_feature_id = data_source_id;

UPDATE {script.workingTable} SET cabd_id = gen_random_uuid() WHERE data_source_id IN (SELECT ogf_id::varchar FROM source_data.odi_matches WHERE comments = 'new feature');

--insert new features
INSERT INTO {script.damTable} (
    cabd_id,
    original_point,
    province_territory_code
    )
SELECT
    cabd_id::uuid,
    ST_GeometryN(ST_Transform(geometry,4617),1),
    'on'
FROM {script.workingTable}
WHERE data_source_id IN (SELECT ogf_id::varchar FROM source_data.odi_matches WHERE comments = 'new feature');

INSERT INTO {script.damSourceTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
)
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}'),
    data_source_id
FROM
    {script.workingTable}
WHERE
    data_source_id IN (SELECT ogf_id::varchar FROM source_data.odi_matches WHERE comments = 'new feature');

INSERT INTO {script.damAttributeTable} (cabd_id) SELECT cabd_id FROM {script.workingTable} WHERE data_source_id IN (SELECT ogf_id::varchar FROM source_data.odi_matches WHERE comments = 'new feature');


--update features
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
    owner_ds = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.owner_ds END,
    ownership_type_code_ds = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.ownership_type_code_ds END,
    comments_ds = CASE WHEN (cabd.comments IS NULL AND {script.datasetname}.comments IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.comments_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND {script.datasetname}.dam_name_en IS NOT NULL) THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    "owner" = CASE WHEN (cabd.owner IS NULL AND {script.datasetname}.owner IS NOT NULL) THEN {script.datasetname}.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN (cabd.ownership_type_code IS NULL AND {script.datasetname}.ownership_type_code IS NOT NULL) THEN {script.datasetname}.ownership_type_code ELSE cabd.ownership_type_code END,
    "comments" = CASE WHEN (cabd.comments IS NULL AND {script.datasetname}.comments IS NOT NULL) THEN {script.datasetname}.comments ELSE cabd.comments END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;
"""

script.do_work(mappingquery)
