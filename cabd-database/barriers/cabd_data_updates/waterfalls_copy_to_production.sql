-- https://www.postgresql.org/docs/13/sql-begin.html
-- if you are using a GUI you need to configure the script editor to 
-- not run in autocommit mode 

BEGIN TRANSACTION;

INSERT INTO waterfalls.waterfalls(
    cabd_id,
    fall_name_en,
    fall_name_fr,
    waterbody_name_en,
    waterbody_name_fr,
    province_territory_code,
    nhn_watershed_id,
    municipality,
    fall_height_m,
    last_modified,
    "comments",
    complete_level_code,
    snapped_point,
    original_point,
    passability_status_code,
    use_analysis
)
SELECT
    cabd_id,
    fall_name_en,
    fall_name_fr,
    waterbody_name_en,
    waterbody_name_fr,
    province_territory_code,
    nhn_watershed_id,
    municipality,
    fall_height_m,
    last_modified,
    "comments",
    complete_level_code,
    snapped_point,
    original_point,
    passability_status_code,
    use_analysis
FROM featurecopy.waterfalls;

INSERT INTO waterfalls.waterfalls_attribute_source(
    cabd_id,
    fall_name_en_ds,
    fall_name_fr_ds,
    waterbody_name_en_ds,
    waterbody_name_fr_ds,
    fall_height_m_ds,
    comments_ds,
    complete_level_code_ds,
    passability_status_code_ds,
    original_point_ds
)
SELECT
    cabd_id,
    fall_name_en_ds,
    fall_name_fr_ds,
    waterbody_name_en_ds,
    waterbody_name_fr_ds,
    fall_height_m_ds,
    comments_ds,
    complete_level_code_ds,
    passability_status_code_ds,
    original_point_ds
FROM featurecopy.waterfalls_attribute_source;

INSERT INTO waterfalls.waterfalls_feature_source (cabd_id, datasource_id, datasource_feature_id)
    SELECT cabd_id, datasource_id, datasource_feature_id
FROM featurecopy.waterfalls_feature_source
ON CONFLICT DO NOTHING;

--do whatever qa checks you want to do here?

SELECT COUNT(*) FROM waterfalls.waterfalls;
SELECT COUNT(*) FROM waterfalls.waterfalls_attribute_source;
SELECT COUNT(*) FROM waterfalls.waterfalls_feature_source;

--COMMIT;