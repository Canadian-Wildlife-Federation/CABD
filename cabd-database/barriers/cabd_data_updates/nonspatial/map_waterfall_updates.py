##################

# Expected usage: py map_dam_updates.py <featureType>
# Please ensure you have run insert_data_sources.py for the data sources you are mapping from
# Otherwise any updates missing a data source id will not be made

# This script will process all updates with an update_status of 'ready'

##################

import user_submit as main

script = main.MappingScript("waterfall_updates")

query = f"""
UPDATE {script.fallUpdateTable} SET update_status = NULL WHERE data_source_short_name NOT IN (SELECT name FROM cabd.data_source);

-- add data source ids to the table
ALTER TABLE {script.fallUpdateTable} ADD COLUMN IF NOT EXISTS data_source uuid;
UPDATE {script.fallUpdateTable} AS s SET data_source = d.id FROM cabd.data_source AS d
    WHERE d.name = s.data_source_short_name
    AND s.update_status = 'ready';

ALTER TABLE cabd.waterfall_updates ALTER COLUMN submitted_on TYPE timestamptz USING submitted_on::timestamptz;
"""

initializequery = f"""
--where multiple updates exist for a feature, only update one at a time
WITH cte AS (
  SELECT id, cabd_id,
    row_number() OVER(PARTITION BY cabd_id ORDER BY submitted_on ASC) AS rn
  FROM {script.fallUpdateTable} WHERE update_status = 'ready'
)
UPDATE {script.fallUpdateTable}
SET update_status = 'wait'
    WHERE id IN (SELECT id FROM cte WHERE rn > 1);
"""

mappingquery = f"""
--------------------------------------------------------------------------
-- TO DO: add dsfid records to damAttributeTable for updates coming from BC water rights database
--------------------------------------------------------------------------

-- deal with new and modified records
WITH new_points AS (
    SELECT (ST_SetSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4617)) AS geom, cabd_id AS id, province_territory_code AS loc
    FROM {script.fallUpdateTable}
    WHERE entry_classification = 'new feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND update_status = 'ready'
    )
INSERT INTO {script.fallTable} (original_point, cabd_id, province_territory_code)
    SELECT geom, id, loc FROM new_points;

INSERT INTO {script.fallAttributeTable} (cabd_id)
    SELECT cabd_id FROM {script.fallUpdateTable}
    WHERE entry_classification = 'new feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND update_status = 'ready';

UPDATE {script.fallTable} SET snapped_point = original_point WHERE snapped_point IS NULL;

WITH move_points AS (
    SELECT (ST_SetSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4617)) AS geom, cabd_id AS id
    FROM {script.fallUpdateTable}
    WHERE entry_classification = 'modify feature'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND update_status = 'ready'
    )
UPDATE {script.fallTable} AS foo 
    SET snapped_point = bar.geom,
        original_point = bar.geom
    FROM move_points AS bar
    WHERE cabd_id = bar.id;

-- add records to feature_source table
INSERT INTO {script.fallFeatureTable} (cabd_id, datasource_id)
SELECT cabd_id, data_source FROM {script.fallUpdateTable} WHERE update_status = 'ready'
ON CONFLICT DO NOTHING;

-- update attribute_source table for live data
UPDATE
    {script.fallAttributeTable} AS cabdsource
SET    
    fall_name_en_ds = CASE WHEN ({script.datasetName}.fall_name_en IS NOT NULL AND {script.datasetName}.fall_name_en IS DISTINCT FROM cabd.fall_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.fall_name_en_ds END,
    fall_name_fr_ds = CASE WHEN ({script.datasetName}.fall_name_fr IS NOT NULL AND {script.datasetName}.fall_name_fr IS DISTINCT FROM cabd.fall_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.fall_name_fr_ds END,
    waterbody_name_en_ds = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    waterbody_name_fr_ds = CASE WHEN ({script.datasetName}.waterbody_name_fr IS NOT NULL AND {script.datasetName}.waterbody_name_fr IS DISTINCT FROM cabd.waterbody_name_fr) THEN {script.datasetName}.data_source ELSE cabdsource.waterbody_name_fr_ds END,
    fall_height_m_ds = CASE WHEN ({script.datasetName}.fall_height_m IS NOT NULL AND {script.datasetName}.fall_height_m IS DISTINCT FROM cabd.fall_height_m) THEN {script.datasetName}.data_source ELSE cabdsource.fall_height_m_ds END,
    comments_ds = CASE WHEN ({script.datasetName}.comments IS NOT NULL AND {script.datasetName}.comments IS DISTINCT FROM cabd.comments) THEN {script.datasetName}.data_source ELSE cabdsource.comments_ds END,
    passability_status_code_ds = CASE WHEN ({script.datasetName}.passability_status_code IS NOT NULL AND {script.datasetName}.passability_status_code IS DISTINCT FROM cabd.passability_status_code) THEN {script.datasetName}.data_source ELSE cabdsource.passability_status_code_ds END,
    waterfall_type_code_ds = CASE WHEN ({script.datasetName}.waterfall_type_code IS NOT NULL AND {script.datasetName}.waterfall_type_code IS DISTINCT FROM cabd.waterfall_type_code) THEN {script.datasetName}.data_source ELSE cabdsource.waterfall_type_code_ds END
FROM
    {script.fallTable} AS cabd,
    {script.fallUpdateTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id and cabd.cabd_id = cabdsource.cabd_id
    AND {script.datasetName}.entry_classification IN ('new feature', 'modify feature')
    AND {script.datasetName}.update_status = 'ready'
    AND {script.datasetName}.data_source IS NOT NULL;

--update attributes in live data
UPDATE
    {script.fallTable} AS cabd
SET
    fall_name_en = CASE WHEN ({script.datasetName}.fall_name_en IS NOT NULL AND {script.datasetName}.fall_name_en IS DISTINCT FROM cabd.fall_name_en) THEN {script.datasetName}.fall_name_en ELSE cabd.fall_name_en END,
    fall_name_fr = CASE WHEN ({script.datasetName}.fall_name_fr IS NOT NULL AND {script.datasetName}.fall_name_fr IS DISTINCT FROM cabd.fall_name_fr) THEN {script.datasetName}.fall_name_fr ELSE cabd.fall_name_fr END,
    waterbody_name_en = CASE WHEN ({script.datasetName}.waterbody_name_en IS NOT NULL AND {script.datasetName}.waterbody_name_en IS DISTINCT FROM cabd.waterbody_name_en) THEN {script.datasetName}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    waterbody_name_fr = CASE WHEN ({script.datasetName}.waterbody_name_fr IS NOT NULL AND {script.datasetName}.waterbody_name_fr IS DISTINCT FROM cabd.waterbody_name_fr) THEN {script.datasetName}.waterbody_name_fr ELSE cabd.waterbody_name_fr END,
    fall_height_m = CASE WHEN ({script.datasetName}.fall_height_m IS NOT NULL AND {script.datasetName}.fall_height_m IS DISTINCT FROM cabd.fall_height_m) THEN {script.datasetName}.fall_height_m ELSE cabd.fall_height_m END,
    "comments" = CASE WHEN ({script.datasetName}.comments IS NOT NULL AND {script.datasetName}.comments IS DISTINCT FROM cabd.comments) THEN {script.datasetName}.comments ELSE cabd.comments END,
    passability_status_code = CASE WHEN ({script.datasetName}.passability_status_code IS NOT NULL AND {script.datasetName}.passability_status_code IS DISTINCT FROM cabd.passability_status_code) THEN {script.datasetName}.passability_status_code ELSE cabd.passability_status_code END,
    use_analysis = CASE WHEN ({script.datasetName}.use_analysis IS NOT NULL AND {script.datasetName}.use_analysis IS DISTINCT FROM cabd.use_analysis) THEN {script.datasetName}.use_analysis ELSE cabd.use_analysis END,
    waterfall_type_code = CASE WHEN ({script.datasetName}.waterfall_type_code IS NOT NULL AND {script.datasetName}.waterfall_type_code IS DISTINCT FROM cabd.waterfall_type_code) THEN {script.datasetName}.waterfall_type_code ELSE cabd.waterfall_type_code END   
FROM
    {script.fallUpdateTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id
    AND {script.datasetName}.entry_classification IN ('new feature', 'modify feature')
    AND {script.datasetName}.update_status = 'ready'
    AND {script.datasetName}.data_source IS NOT NULL;

-- deal with records to be deleted
DELETE FROM {script.fallAttributeTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.fallUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');
DELETE FROM {script.damFeatureTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.fallUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');
DELETE FROM {script.fallTable} WHERE cabd_id IN (SELECT cabd_id FROM {script.fallUpdateTable} WHERE entry_classification = 'delete feature' AND update_status = 'ready');

-- set records to 'done' and delete from update table
UPDATE {script.fallUpdateTable} SET update_status = 'done' WHERE update_status = 'ready';
UPDATE {script.fallUpdateTable} SET update_status = 'ready' WHERE update_status = 'wait';
-- DELETE FROM {script.fallUpdateTable} WHERE update_status = 'done';

"""

script.do_work(query, initializequery, mappingquery)
