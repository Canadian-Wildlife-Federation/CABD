---------------------------------------------------
-- nontidal
---------------------------------------------------

---------------------------------------------------
-- get all new features from assessment data
---------------------------------------------------

TRUNCATE TABLE featurecopy.nontidal_sites CASCADE;

INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    crossing_type_code,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    crossing_type_code,
    original_point
FROM featurecopy.nontidal_sites_kwrc_bridges
WHERE entry_classification = 'new feature';


INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    road_type_code,
    road_class,
    road_surface,
    site_type,
    alignment_code,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_bankfull_width_m,
    downstream_bankfull_width_m,
    constriction_code,
    tailwater_scour_pool_code,
    crossing_comments,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    road_type_code,
    road_class,
    road_surface,
    site_type,
    alignment_code,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_bankfull_width_m,
    downstream_bankfull_width_m,
    constriction_code,
    tailwater_scour_pool_code,
    crossing_comments,
    original_point
FROM featurecopy.nontidal_sites_kwrc_current_aug_6
WHERE entry_classification = 'new feature';


INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    flow_condition_code,
    crossing_comments,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    flow_condition_code,
    crossing_comments,
    original_point
FROM featurecopy.nontidal_sites_kwrc_intermitted_dry_drains
WHERE entry_classification = 'new feature';


INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    road_type_code,
    road_class,
    road_surface,
    site_type,
    alignment_code,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_bankfull_width_m,
    downstream_bankfull_width_m,
    constriction_code,
    tailwater_scour_pool_code,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    road_type_code,
    road_class,
    road_surface,
    site_type,
    alignment_code,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_bankfull_width_m,
    downstream_bankfull_width_m,
    constriction_code,
    tailwater_scour_pool_code,
    original_point
FROM featurecopy.nontidal_sites_kwrc_master_2
WHERE entry_classification = 'new feature';


INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    stream_name,
    road_name,
    road_type_code,
    crossing_type_code,
    structure_count,
    flow_condition_code,
    crossing_condition_code,
    site_type,
    alignment_code,
    road_fill_height_m,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_bankfull_width_m,
    downstream_bankfull_width_m,
    upstream_bankfull_width_confidence_code,
    downstream_bankfull_width_confidence_code,
    constriction_code,
    tailwater_scour_pool_code,
    crossing_comments,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    stream_name,
    road_name,
    road_type_code,
    crossing_type_code,
    structure_count,
    flow_condition_code,
    crossing_condition_code,
    site_type,
    alignment_code,
    road_fill_height_m,
    upstream_channel_depth_m,
    downstream_channel_depth_m,
    upstream_bankfull_width_m,
    downstream_bankfull_width_m,
    upstream_bankfull_width_confidence_code,
    downstream_bankfull_width_confidence_code,
    constriction_code,
    tailwater_scour_pool_code,
    crossing_comments,
    original_point
FROM featurecopy.nontidal_sites_peskotomuhkati_nation_01192023
WHERE entry_classification = 'new feature';

INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    date_observed,
    crossing_type_code,
    road_name,
    crossing_condition_code,
    crossing_comments,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    date_observed,
    crossing_type_code,
    road_name,
    crossing_condition_code,
    crossing_comments,
    original_point
FROM featurecopy.nontidal_sites_acapsj_ibof_barriers
WHERE entry_classification = 'new feature';

INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    lead_observer,
    stream_name,
    crossing_type_code,
    road_type_code,
    road_surface,
    road_class,
    crossing_condition_code,
    flow_condition_code,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    lead_observer,
    stream_name,
    crossing_type_code,
    road_type_code,
    road_surface,
    road_class,
    crossing_condition_code,
    flow_condition_code,
    original_point
FROM featurecopy.nontidal_sites_acapsj_master_sheet
WHERE entry_classification = 'new feature';

INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    crossing_type_code,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    crossing_type_code,
    original_point
FROM featurecopy.nontidal_sites_acapsj_salmon_creek_rothesay
WHERE entry_classification = 'new feature';

INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    crossing_type_code,
    stream_name,
    crossing_comments,
    site_type,
    crossing_condition_code,
    flow_condition_code,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    crossing_type_code,
    stream_name,
    crossing_comments,
    site_type_code,
    crossing_condition_code,
    flow_condition_code,
    original_point
FROM featurecopy.nontidal_sites_acapsj_stream_barriers
WHERE entry_classification = 'new feature';

INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    crossing_type_code,
    crossing_comments,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    crossing_type_code,
    crossing_comments,
    original_point
FROM featurecopy.nontidal_sites_acapsj_stream_crossing_layers
WHERE entry_classification = 'new feature';

---------------------------------------------------
-- get all modelled crossings
---------------------------------------------------

INSERT INTO featurecopy.nontidal_sites (
    cabd_id,
    stream_name,
    strahler_order,
    road_name,
    road_type_code,
    road_class,
    road_surface,
    transport_feature_owner,
    railway_operator,
    num_railway_tracks,
    railway_status,
    crossing_type_code,
    original_point
)
SELECT
    id,
    stream_name_1,
    strahler_order::integer,
    transport_feature_name,
    CASE
        WHEN transport_feature_type = 'road' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'road')
        WHEN transport_feature_type = 'resource road' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'resource road')
        WHEN transport_feature_type = 'rail' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'railroad')
        END AS transport_feature_type,
    roadway_type,
    roadway_surface,
    transport_feature_owner,
    railway_operator,
    num_railway_tracks,
    transport_feature_condition,
    CASE
        WHEN crossing_type = 'Bridge' THEN (SELECT code FROM stream_crossings.crossing_type_codes WHERE name_en = 'bridge')
        ELSE NULL
        END AS crossing_type,
    geometry
FROM nb_data.modelled_crossings
WHERE 
    id NOT IN (SELECT cabd_id FROM featurecopy.tidal_sites_peskotomuhkati_nation_01192023 WHERE cabd_id IS NOT NULL)
    AND id NOT IN (SELECT cabd_id FROM featurecopy.tidal_sites_acapsj_stream_barriers WHERE cabd_id IS NOT NULL);

---------------------------------------------------
-- tidal
---------------------------------------------------

---------------------------------------------------
-- get all new features from assessment data
---------------------------------------------------

TRUNCATE TABLE featurecopy.tidal_sites CASCADE;

INSERT INTO featurecopy.tidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    stream_name,
    road_name,
    road_type_code,
    crossing_type_code,
    structure_count,
    flow_condition_code,
    crossing_condition_code,
    site_type,
    alignment_code,
    road_fill_height_m,
    downstream_channel_width_m,
    crossing_comments,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    original_assessment_id,
    date_observed,
    stream_name,
    road_name,
    road_type_code,
    crossing_type_code,
    structure_count,
    flow_condition_code,
    crossing_condition_code,
    site_type,
    alignment_code,
    road_fill_height_m,
    downstream_channel_width_m,
    crossing_comments,
    original_point
FROM featurecopy.tidal_sites_peskotomuhkati_nation_01192023
WHERE entry_classification = 'new feature';

INSERT INTO featurecopy.tidal_sites (
    cabd_id,
    data_source_id,
    cabd_assessment_id,
    crossing_type_code,
    stream_name,
    site_type,
    original_point
)
SELECT
    gen_random_uuid(),
    data_source_id,
    cabd_assessment_id,
    crossing_type_code,
    stream_name,
    site_type_code,
    original_point
FROM featurecopy.tidal_sites_acapsj_stream_barriers
WHERE entry_classification = 'new feature';

---------------------------------------------------
-- get all modelled crossings
---------------------------------------------------

INSERT INTO featurecopy.tidal_sites (
    cabd_id,
    stream_name,
    strahler_order,
    road_name,
    road_type_code,
    road_class,
    road_surface,
    transport_feature_owner,
    railway_operator,
    num_railway_tracks,
    railway_status,
    crossing_type_code,
    original_point
)
SELECT
    id,
    stream_name_1,
    strahler_order::integer,
    transport_feature_name,
    CASE
        WHEN transport_feature_type = 'road' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'road')
        WHEN transport_feature_type = 'resource road' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'resource road')
        WHEN transport_feature_type = 'rail' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'railroad')
        END AS transport_feature_type,
    roadway_type,
    roadway_surface,
    transport_feature_owner,
    railway_operator,
    num_railway_tracks,
    transport_feature_condition,
    CASE
        WHEN crossing_type = 'Bridge' THEN (SELECT code FROM stream_crossings.crossing_type_codes WHERE name_en = 'bridge')
        ELSE NULL
        END AS crossing_type,
    geometry
FROM nb_data.modelled_crossings
WHERE 
    id IN (SELECT cabd_id FROM featurecopy.tidal_sites_peskotomuhkati_nation_01192023 WHERE cabd_id IS NOT NULL)
    OR id IN (SELECT cabd_id FROM featurecopy.tidal_sites_acapsj_stream_barriers WHERE cabd_id IS NOT NULL);