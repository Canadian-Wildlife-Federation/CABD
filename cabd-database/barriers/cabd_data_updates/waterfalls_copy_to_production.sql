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
    nhn_workunit_id,
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
    nhn_workunit_id,
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
    fall_name_en_dsfid,
    fall_name_fr_ds,
    fall_name_fr_dsfid,
    waterbody_name_en_ds,
    waterbody_name_en_dsfid,
    waterbody_name_fr_ds,
    waterbody_name_fr_dsfid,
    fall_height_m_ds,
    fall_height_m_dsfid,
    comments_ds,
    comments_dsfid,
    complete_level_code_ds,
    complete_level_code_dsfid,
    passability_status_code_ds,
    passability_status_code_dsfid,
    original_point_ds,
    original_point_dsfid
)
SELECT
    cabd_id,
    fall_name_en_ds,
    fall_name_en_dsfid,
    fall_name_fr_ds,
    fall_name_fr_dsfid,
    waterbody_name_en_ds,
    waterbody_name_en_dsfid,
    waterbody_name_fr_ds,
    waterbody_name_fr_dsfid,
    fall_height_m_ds,
    fall_height_m_dsfid,
    comments_ds,
    comments_dsfid,
    complete_level_code_ds,
    complete_level_code_dsfid,
    passability_status_code_ds,
    passability_status_code_dsfid,
    original_point_ds,
    original_point_dsfid
FROM featurecopy.waterfalls_attribute_source;

--do whatever qa checks you want to do here?

SELECT COUNT(*) FROM waterfalls.waterfalls;
SELECT COUNT(*) FROM waterfalls.waterfalls_attribute_source;

--COMMIT;