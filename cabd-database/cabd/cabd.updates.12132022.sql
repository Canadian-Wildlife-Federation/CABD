-- FEATURE TYPE METADATA --

INSERT INTO cabd.feature_type_metadata (
    view_name,
    field_name,
    name_en,
    description_en,
    is_link,
    data_type,
    vw_simple_order,
    vw_all_order,
    include_vector_tile,
    value_options_reference,
    name_fr,
    description_fr,
    is_name_search
)
SELECT
    view_name,
    field_name,
    name_en,
    description_en,
    is_link,
    data_type,
    vw_simple_order,
    vw_all_order,
    include_vector_tile,
    value_options_reference,
    name_fr,
    description_fr,
    is_name_search
FROM
    cabd.temp_feature_type_metadata;

-- FEATURE TYPE VERSION HISTORY --

INSERT INTO cabd.feature_type_version_history (
    type,
    version,
    start_date,
    end_date
)
VALUES (
    'stream_crossings',
    '1.0'
    NULL,
    NULL
);

-- STREAM CROSSINGS VIEW - ENGLISH --

CREATE OR REPLACE VIEW cabd.stream_crossings_view_en
AS
SELECT
    s.cabd_id,
    'stream_crossings'::text AS feature_type,
    s.original_assessment_id,
    ds.full_name AS assessment_source,
    st_y(s.snapped_point) AS latitude,
    st_x(s.snapped_point) AS longitude,
    CASE
        WHEN nts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
        ELSE nts.passability_status_code
        END AS passability_status_code,
    CASE
        WHEN ps.name_en IS NULL THEN 'Unknown'::text
        ELSE ps.name_en
        END AS passability_status,
    s.date_observed,
    s.lead_observer,
    s.town_county,
    s.stream_name,
    s.strahler_order,
    s.road_name,
    s.road_type_code,
    rt.name_en AS road_type,
    s.road_class,
    s.road_surface,
    s.transport_feature_owner,
    s.railway_operator,
    s.num_railway_tracks,
    s.railway_status,
    s.location_description,
    s.crossing_type_code,
    ct.name_en AS crossing_type,
    s.structure_count,
    s.photo_id_inlet,
    s.photo_id_outlet,
    s.photo_id_upstream,
    s.photo_id_downstream,
    s.photo_id_other,
    s.flow_condition_code,
    fc.name_en AS flow_condition,
    s.crossing_condition_code,
    cc.name_en AS crossing_condition,
    s.site_type AS site_type_code,
    st.name_en AS site_type,
    s.alignment_code,
    ac.name_en AS alignment,
    s.road_fill_height_m,
    s.upstream_channel_depth_m,
    s.downstream_channel_depth_m,
    NULL AS upstream_channel_width_m,
    s.upstream_scour_pool_width_m,
    NULL AS downstream_channel_width_m,
    s.downstream_scour_pool_width_m,
    s.upstream_bankfull_width_m,
    s.downstream_bankfull_width_m,
    s.upstream_bankfull_width_confidence_code,
    c1.name_en AS upstream_bankfull_width_confidence,
    s.downstream_bankfull_width_confidence_code,
    c2.name_en AS downstream_bankfull_width_confidence,
    s.constriction_code,
    c3.name_en AS constriction,
    s.tailwater_scour_pool_code,
    sp.name_en AS tailwater_scour_pool,
    s.crossing_comments,
    NULL AS tidal_tide_stage_code,
    NULL AS tidal_tide_stage,
    NULL AS tidal_low_tide_prediction,
    NULL AS tidal_tide_station,
    NULL AS tidal_stream_type_code,
    NULL AS tidal_stream_type,
    NULL AS tidal_salinity_ppt,
    NULL AS tidal_visible_utilities_code,
    NULL AS tidal_visible_utilities,
    NULL AS tidal_road_flooded_high_tide,
    NULL AS tidal_upstream_tidal_range_m,
    NULL AS tidal_downstream_tidal_range_m,
    NULL AS tidal_veg_change_code,
    NULL AS tidal_veg_change,
    s.snapped_point AS geometry
FROM
	stream_crossings.nontidal_sites AS s
	LEFT JOIN cabd.data_source ds ON ds.id = s.data_source_id
    LEFT JOIN stream_crossings.road_type_codes rt ON rt.code = s.road_type_code
    LEFT JOIN stream_crossings.crossing_type_codes ct ON ct.code = s.crossing_type_code
    LEFT JOIN stream_crossings.flow_condition_codes fc ON fc.code = s.flow_condition_code
    LEFT JOIN stream_crossings.crossing_condition_codes cc ON cc.code = s.crossing_condition_code
    LEFT JOIN stream_crossings.site_type_codes st ON st.code = s.site_type
    LEFT JOIN stream_crossings.alignment_codes ac ON ac.code = s.alignment_code
    LEFT JOIN stream_crossings.confidence_codes c1 ON c1.code = s.upstream_bankfull_width_confidence_code
    LEFT JOIN stream_crossings.confidence_codes c2 ON c2.code = s.downstream_bankfull_width_confidence_code
    LEFT JOIN stream_crossings.constriction_codes c3 ON c3.code = s.constriction_code
    LEFT JOIN stream_crossings.scour_pool_codes sp ON sp.code = s.tailwater_scour_pool_code
    LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = s.cabd_id AND nts.primary_structure is true
    LEFT JOIN cabd.passability_status_codes ps ON ps.code = nts.passability_status_code

UNION

SELECT
    s.cabd_id,
    'stream_crossings'::text AS feature_type,
    s.original_assessment_id,
    ds.full_name AS assessment_source,
    st_y(s.snapped_point) AS latitude,
    st_x(s.snapped_point) AS longitude,
    CASE
        WHEN nts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
        ELSE nts.passability_status_code
        END AS passability_status_code,
    CASE
        WHEN ps.name_en IS NULL THEN 'Unknown'::text
        ELSE ps.name_en
        END AS passability_status,
    s.date_observed,
    s.lead_observer,
    s.town_county,
    s.stream_name,
    s.strahler_order,
    s.road_name,
    s.road_type_code,
    rt.name_en AS road_type,
    s.road_class,
    s.road_surface,
    s.transport_feature_owner,
    s.railway_operator,
    s.num_railway_tracks,
    s.railway_status,
    s.location_description,
    s.crossing_type_code,
    ct.name_en AS crossing_type,
    s.structure_count,
    s.photo_id_inlet,
    s.photo_id_outlet,
    s.photo_id_upstream,
    s.photo_id_downstream,
    s.photo_id_other,
    s.flow_condition_code,
    fc.name_en AS flow_condition,
    s.crossing_condition_code,
    cc.name_en AS crossing_condition,
    s.site_type AS site_type_code,
    st.name_en AS site_type,
    s.alignment_code,
    ac.name_en AS alignment,
    s.road_fill_height_m,
    s.upstream_channel_depth_m,
    s.downstream_channel_depth_m,
    s.upstream_channel_width_m,
    s.upstream_scour_pool_width_m,
    s.downstream_channel_width_m,
    s.downstream_scour_pool_width_m,
    NULL AS upstream_bankfull_width_m,
    NULL AS downstream_bankfull_width_m,
    NULL AS upstream_bankfull_width_confidence_code,
    NULL AS upstream_bankfull_width_confidence,
    NULL AS downstream_bankfull_width_confidence_code,
    NULL AS downstream_bankfull_width_confidence,
    NULL AS constriction_code,
    NULL AS constriction,
    NULL AS tailwater_scour_pool_code,
    NULL AS tailwater_scour_pool,
    s.crossing_comments,
    s.tidal_tide_stage_code,
    ts.name_en AS tidal_tide_stage,
    s.tidal_low_tide_prediction,
    s.tidal_tide_station,
    s.tidal_stream_type_code,
    sty.name_en AS tidal_stream_type,
    s.tidal_salinity_ppt,
    s.tidal_visible_utilities_code,
    vu.name_en AS tidal_visible_utilities,
    s.tidal_road_flooded_high_tide,
    s.tidal_upstream_tidal_range_m,
    s.tidal_downstream_tidal_range_m,
    s.tidal_veg_change_code,
    vc.name_en AS tidal_veg_change,
    s.snapped_point AS geometry
FROM
	stream_crossings.tidal_sites AS s
	LEFT JOIN cabd.data_source ds ON ds.id = s.data_source_id
    LEFT JOIN stream_crossings.road_type_codes rt ON rt.code = s.road_type_code
    LEFT JOIN stream_crossings.crossing_type_codes ct ON ct.code = s.crossing_type_code
    LEFT JOIN stream_crossings.flow_condition_codes fc ON fc.code = s.flow_condition_code
    LEFT JOIN stream_crossings.crossing_condition_codes cc ON cc.code = s.crossing_condition_code
    LEFT JOIN stream_crossings.site_type_codes st ON st.code = s.site_type
    LEFT JOIN stream_crossings.alignment_codes ac ON ac.code = s.alignment_code
    LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = s.cabd_id AND nts.primary_structure is true
    LEFT JOIN cabd.passability_status_codes ps ON ps.code = nts.passability_status_code
    LEFT JOIN stream_crossings.tide_stage_codes ts ON ts.code = s.tidal_tide_stage_code
    LEFT JOIN stream_crossings.stream_type_codes sty ON sty.code = s.tidal_stream_type_code
    LEFT JOIN stream_crossings.visible_utilities_codes vu ON vu.code = s.tidal_visible_utilities_code
    LEFT JOIN stream_crossings.vegetation_change_codes vc ON vc.code = s.tidal_veg_change_code
;

ALTER TABLE cabd.stream_crossings_view_en
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.stream_crossings_view_en TO PUBLIC;
GRANT ALL ON TABLE cabd.stream_crossings_view_en TO cabd;
GRANT SELECT ON TABLE cabd.stream_crossings_view_en TO cwf_user;
GRANT SELECT ON TABLE cabd.stream_crossings_view_en TO egouge;

-- STREAM CROSSINGS VIEW - FRENCH --

CREATE OR REPLACE VIEW cabd.stream_crossings_view_fr
AS
SELECT
    s.cabd_id,
    'stream_crossings'::text AS feature_type,
    s.original_assessment_id,
    ds.full_name AS assessment_source,
    st_y(s.snapped_point) AS latitude,
    st_x(s.snapped_point) AS longitude,
    CASE
        WHEN nts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_fr = 'Inconnu')
        ELSE nts.passability_status_code
        END AS passability_status_code,
    CASE
        WHEN ps.name_fr IS NULL THEN 'Inconnu'::text
        ELSE ps.name_fr
        END AS passability_status,
    s.date_observed,
    s.lead_observer,
    s.town_county,
    s.stream_name,
    s.strahler_order,
    s.road_name,
    s.road_type_code,
    rt.name_fr AS road_type,
    s.road_class,
    s.road_surface,
    s.transport_feature_owner,
    s.railway_operator,
    s.num_railway_tracks,
    s.railway_status,
    s.location_description,
    s.crossing_type_code,
    ct.name_fr AS crossing_type,
    s.structure_count,
    s.photo_id_inlet,
    s.photo_id_outlet,
    s.photo_id_upstream,
    s.photo_id_downstream,
    s.photo_id_other,
    s.flow_condition_code,
    fc.name_fr AS flow_condition,
    s.crossing_condition_code,
    cc.name_fr AS crossing_condition,
    s.site_type AS site_type_code,
    st.name_fr AS site_type,
    s.alignment_code,
    ac.name_fr AS alignment,
    s.road_fill_height_m,
    s.upstream_channel_depth_m,
    s.downstream_channel_depth_m,
    NULL AS upstream_channel_width_m,
    s.upstream_scour_pool_width_m,
    NULL AS downstream_channel_width_m,
    s.downstream_scour_pool_width_m,
    s.upstream_bankfull_width_m,
    s.downstream_bankfull_width_m,
    s.upstream_bankfull_width_confidence_code,
    c1.name_fr AS upstream_bankfull_width_confidence,
    s.downstream_bankfull_width_confidence_code,
    c2.name_fr AS downstream_bankfull_width_confidence,
    s.constriction_code,
    c3.name_fr AS constriction,
    s.tailwater_scour_pool_code,
    sp.name_fr AS tailwater_scour_pool,
    s.crossing_comments,
    NULL AS tidal_tide_stage_code,
    NULL AS tidal_tide_stage,
    NULL AS tidal_low_tide_prediction,
    NULL AS tidal_tide_station,
    NULL AS tidal_stream_type_code,
    NULL AS tidal_stream_type,
    NULL AS tidal_salinity_ppt,
    NULL AS tidal_visible_utilities_code,
    NULL AS tidal_visible_utilities,
    NULL AS tidal_road_flooded_high_tide,
    NULL AS tidal_upstream_tidal_range_m,
    NULL AS tidal_downstream_tidal_range_m,
    NULL AS tidal_veg_change_code,
    NULL AS tidal_veg_change,
    s.snapped_point AS geometry
FROM
	stream_crossings.nontidal_sites AS s
	LEFT JOIN cabd.data_source ds ON ds.id = s.data_source_id
    LEFT JOIN stream_crossings.road_type_codes rt ON rt.code = s.road_type_code
    LEFT JOIN stream_crossings.crossing_type_codes ct ON ct.code = s.crossing_type_code
    LEFT JOIN stream_crossings.flow_condition_codes fc ON fc.code = s.flow_condition_code
    LEFT JOIN stream_crossings.crossing_condition_codes cc ON cc.code = s.crossing_condition_code
    LEFT JOIN stream_crossings.site_type_codes st ON st.code = s.site_type
    LEFT JOIN stream_crossings.alignment_codes ac ON ac.code = s.alignment_code
    LEFT JOIN stream_crossings.confidence_codes c1 ON c1.code = s.upstream_bankfull_width_confidence_code
    LEFT JOIN stream_crossings.confidence_codes c2 ON c2.code = s.downstream_bankfull_width_confidence_code
    LEFT JOIN stream_crossings.constriction_codes c3 ON c3.code = s.constriction_code
    LEFT JOIN stream_crossings.scour_pool_codes sp ON sp.code = s.tailwater_scour_pool_code
    LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = s.cabd_id AND nts.primary_structure is true
    LEFT JOIN cabd.passability_status_codes ps ON ps.code = nts.passability_status_code

UNION

SELECT
    s.cabd_id,
    'stream_crossings'::text AS feature_type,
    s.original_assessment_id,
    ds.full_name AS assessment_source,
    st_y(s.snapped_point) AS latitude,
    st_x(s.snapped_point) AS longitude,
    CASE
        WHEN nts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_fr = 'Inconnu')
        ELSE nts.passability_status_code
        END AS passability_status_code,
    CASE
        WHEN ps.name_fr IS NULL THEN 'Inconnu'::text
        ELSE ps.name_fr
        END AS passability_status,
    s.date_observed,
    s.lead_observer,
    s.town_county,
    s.stream_name,
    s.strahler_order,
    s.road_name,
    s.road_type_code,
    rt.name_fr AS road_type,
    s.road_class,
    s.road_surface,
    s.transport_feature_owner,
    s.railway_operator,
    s.num_railway_tracks,
    s.railway_status,
    s.location_description,
    s.crossing_type_code,
    ct.name_fr AS crossing_type,
    s.structure_count,
    s.photo_id_inlet,
    s.photo_id_outlet,
    s.photo_id_upstream,
    s.photo_id_downstream,
    s.photo_id_other,
    s.flow_condition_code,
    fc.name_fr AS flow_condition,
    s.crossing_condition_code,
    cc.name_fr AS crossing_condition,
    s.site_type AS site_type_code,
    st.name_fr AS site_type,
    s.alignment_code,
    ac.name_fr AS alignment,
    s.road_fill_height_m,
    s.upstream_channel_depth_m,
    s.downstream_channel_depth_m,
    s.upstream_channel_width_m,
    s.upstream_scour_pool_width_m,
    s.downstream_channel_width_m,
    s.downstream_scour_pool_width_m,
    NULL AS upstream_bankfull_width_m,
    NULL AS downstream_bankfull_width_m,
    NULL AS upstream_bankfull_width_confidence_code,
    NULL AS upstream_bankfull_width_confidence,
    NULL AS downstream_bankfull_width_confidence_code,
    NULL AS downstream_bankfull_width_confidence,
    NULL AS constriction_code,
    NULL AS constriction,
    NULL AS tailwater_scour_pool_code,
    NULL AS tailwater_scour_pool,
    s.crossing_comments,
    s.tidal_tide_stage_code,
    ts.name_fr AS tidal_tide_stage,
    s.tidal_low_tide_prediction,
    s.tidal_tide_station,
    s.tidal_stream_type_code,
    sty.name_fr AS tidal_stream_type,
    s.tidal_salinity_ppt,
    s.tidal_visible_utilities_code,
    vu.name_fr AS tidal_visible_utilities,
    s.tidal_road_flooded_high_tide,
    s.tidal_upstream_tidal_range_m,
    s.tidal_downstream_tidal_range_m,
    s.tidal_veg_change_code,
    vc.name_fr AS tidal_veg_change,
    s.snapped_point AS geometry
FROM
	stream_crossings.tidal_sites AS s
	LEFT JOIN cabd.data_source ds ON ds.id = s.data_source_id
    LEFT JOIN stream_crossings.road_type_codes rt ON rt.code = s.road_type_code
    LEFT JOIN stream_crossings.crossing_type_codes ct ON ct.code = s.crossing_type_code
    LEFT JOIN stream_crossings.flow_condition_codes fc ON fc.code = s.flow_condition_code
    LEFT JOIN stream_crossings.crossing_condition_codes cc ON cc.code = s.crossing_condition_code
    LEFT JOIN stream_crossings.site_type_codes st ON st.code = s.site_type
    LEFT JOIN stream_crossings.alignment_codes ac ON ac.code = s.alignment_code
    LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = s.cabd_id AND nts.primary_structure is true
    LEFT JOIN cabd.passability_status_codes ps ON ps.code = nts.passability_status_code
    LEFT JOIN stream_crossings.tide_stage_codes ts ON ts.code = s.tidal_tide_stage_code
    LEFT JOIN stream_crossings.stream_type_codes sty ON sty.code = s.tidal_stream_type_code
    LEFT JOIN stream_crossings.visible_utilities_codes vu ON vu.code = s.tidal_visible_utilities_code
    LEFT JOIN stream_crossings.vegetation_change_codes vc ON vc.code = s.tidal_veg_change_code
;

ALTER TABLE cabd.stream_crossings_view_fr
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.stream_crossings_view_fr TO PUBLIC;
GRANT ALL ON TABLE cabd.stream_crossings_view_fr TO cabd;
GRANT SELECT ON TABLE cabd.stream_crossings_view_fr TO cwf_user;
GRANT SELECT ON TABLE cabd.stream_crossings_view_fr TO egouge;

-- ALL FEATURES VIEW - ENGLISH --

CREATE OR REPLACE VIEW cabd.all_features_view_en
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.barrier_type AS feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name_en AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name_en AS passability_status,
    barriers.use_analysis,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS barrier_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_watershed_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.use_analysis,
            dams.snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS barrier_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_watershed_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.use_analysis,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
         SELECT fishways.cabd_id,
            'fishways'::text AS barrier_type,
            fishways.structure_name_en,
            fishways.structure_name_fr,
            fishways.province_territory_code,
            fishways.nhn_watershed_id,
            fishways.municipality,
            fishways.river_name_en,
            fishways.river_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::smallint AS int2,
            NULL::boolean AS "boolean",
            fishways.original_point
           FROM fishways.fishways
        UNION
            SELECT nontidal.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            nontidal.town_county,
            nontidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            CASE
                WHEN nts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
                ELSE nts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            nontidal.snapped_point
           FROM stream_crossings.nontidal_sites nontidal
            LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = nontidal.cabd_id AND nts.primary_structure is true
        UNION
            SELECT tidal.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            tidal.town_county,
            tidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            CASE
                WHEN ts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
                ELSE ts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            tidal.snapped_point
           FROM stream_crossings.tidal_sites tidal
            LEFT JOIN stream_crossings.tidal_structures ts ON ts.site_id = tidal.cabd_id AND ts.primary_structure is true
           ) barriers


     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;

ALTER TABLE cabd.all_features_view_en
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.all_features_view_en TO PUBLIC;
GRANT ALL ON TABLE cabd.all_features_view_en TO cabd;
GRANT SELECT ON TABLE cabd.all_features_view_en TO cwf_user;
GRANT SELECT ON TABLE cabd.all_features_view_en TO egouge;

-- ALL FEATURES VIEW - FRENCH --

CREATE OR REPLACE VIEW cabd.all_features_view_fr
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.barrier_type AS feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name_fr AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name_fr AS passability_status,
    barriers.use_analysis,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS barrier_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_watershed_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.use_analysis,
            dams.snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS barrier_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_watershed_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.use_analysis,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
         SELECT fishways.cabd_id,
            'fishways'::text AS barrier_type,
            fishways.structure_name_en,
            fishways.structure_name_fr,
            fishways.province_territory_code,
            fishways.nhn_watershed_id,
            fishways.municipality,
            fishways.river_name_en,
            fishways.river_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::smallint AS int2,
            NULL::boolean AS "boolean",
            fishways.original_point
           FROM fishways.fishways
        UNION
            SELECT nontidal.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            nontidal.town_county,
            nontidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            CASE
                WHEN nts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_fr = 'Inconnu')
                ELSE nts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            nontidal.snapped_point
           FROM stream_crossings.nontidal_sites nontidal
            LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = nontidal.cabd_id AND nts.primary_structure is true
        UNION
            SELECT tidal.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            tidal.town_county,
            tidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            CASE
                WHEN ts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_fr = 'Inconnu')
                ELSE ts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            tidal.snapped_point
           FROM stream_crossings.tidal_sites tidal
            LEFT JOIN stream_crossings.tidal_structures ts ON ts.site_id = tidal.cabd_id AND ts.primary_structure is true
           ) barriers


     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;

ALTER TABLE cabd.all_features_view_fr
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.all_features_view_fr TO PUBLIC;
GRANT ALL ON TABLE cabd.all_features_view_fr TO cabd;
GRANT SELECT ON TABLE cabd.all_features_view_fr TO cwf_user;
GRANT SELECT ON TABLE cabd.all_features_view_fr TO egouge;

-- BARRIERS VIEW - ENGLISH --

CREATE OR REPLACE VIEW cabd.barriers_view_en
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name_en AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name_en AS passability_status,
    barriers.use_analysis,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS feature_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_watershed_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.use_analysis,
            dams.snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS feature_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_watershed_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.use_analysis,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
            SELECT nontidal.cabd_id,
            'stream_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            nontidal.town_county,
            nontidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            CASE
                WHEN nts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
                ELSE nts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            nontidal.snapped_point
           FROM stream_crossings.nontidal_sites nontidal
            LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = nontidal.cabd_id AND nts.primary_structure is true
        UNION
            SELECT tidal.cabd_id,
            'stream_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            tidal.town_county,
            tidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            CASE
                WHEN ts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
                ELSE ts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            tidal.snapped_point
           FROM stream_crossings.tidal_sites tidal
            LEFT JOIN stream_crossings.tidal_structures ts ON ts.site_id = tidal.cabd_id AND ts.primary_structure is true
           
        ) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;

ALTER TABLE cabd.barriers_view_en
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.barriers_view_en TO PUBLIC;
GRANT ALL ON TABLE cabd.barriers_view_en TO cabd;
GRANT SELECT ON TABLE cabd.barriers_view_en TO cwf_user;
GRANT SELECT ON TABLE cabd.barriers_view_en TO egouge;

-- BARRIERS VIEW - FRENCH --

CREATE OR REPLACE VIEW cabd.barriers_view_fr
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name_fr AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name_fr AS passability_status,
    barriers.use_analysis,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS feature_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_watershed_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.use_analysis,
            dams.snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS feature_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_watershed_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.use_analysis,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
            SELECT nontidal.cabd_id,
            'stream_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            nontidal.town_county,
            nontidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            CASE
                WHEN nts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_fr = 'Inconnu')
                ELSE nts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            nontidal.snapped_point
           FROM stream_crossings.nontidal_sites nontidal
            LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = nontidal.cabd_id AND nts.primary_structure is true
        UNION
            SELECT tidal.cabd_id,
            'stream_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            tidal.town_county,
            tidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            CASE
                WHEN ts.passability_status_code IS NULL THEN (SELECT code FROM cabd.passability_status_codes WHERE name_fr = 'Inconnu')
                ELSE ts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            tidal.snapped_point
           FROM stream_crossings.tidal_sites tidal
            LEFT JOIN stream_crossings.tidal_structures ts ON ts.site_id = tidal.cabd_id AND ts.primary_structure is true
           
        ) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;

ALTER TABLE cabd.barriers_view_fr
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.barriers_view_fr TO PUBLIC;
GRANT ALL ON TABLE cabd.barriers_view_fr TO cabd;
GRANT SELECT ON TABLE cabd.barriers_view_fr TO cwf_user;
GRANT SELECT ON TABLE cabd.barriers_view_fr TO egouge;