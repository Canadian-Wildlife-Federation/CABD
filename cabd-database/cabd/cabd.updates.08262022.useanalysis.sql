--these queries add the 'use_analysis' field to the relevant views and
--the feature_type_metadata table
--they also correct some missing descriptions for fields in the
--feature type metadata table and add any fields missing from the relevant views

-----------------------------------------------------------------
--
--   DAMS
--
-----------------------------------------------------------------

DROP VIEW cabd.dams_view;

--recreate view
CREATE OR REPLACE VIEW cabd.dams_view
 AS
 SELECT d.cabd_id,
    'dams'::text AS feature_type,
    'features/datasources/'::text || d.cabd_id AS datasource_url,
    st_y(d.snapped_point) AS latitude,
    st_x(d.snapped_point) AS longitude,
    d.dam_name_en,
    d.dam_name_fr,
    d.facility_name_en,
    d.facility_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.reservoir_name_en,
    d.reservoir_name_fr,
    d.nhn_watershed_id,
    nhn.sub_sub_drainage_area AS nhn_watershed_name,
    d.province_territory_code,
    pt.name AS province_territory,
    d.owner,
    d.ownership_type_code,
    ow.name AS ownership_type,
    d.municipality,
    d.provincial_compliance_status,
    d.federal_compliance_status,
    d.operating_notes,
    d.operating_status_code,
    os.name AS operating_status,
    d.removed_year,
    d.use_code,
    duc.name AS dam_use,
    d.use_irrigation_code,
    c1.name AS use_irrigation,
    d.use_electricity_code,
    c2.name AS use_electricity,
    d.use_supply_code,
    c3.name AS use_supply,
    d.use_floodcontrol_code,
    c4.name AS use_floodcontrol,
    d.use_recreation_code,
    c5.name AS use_recreation,
    d.use_navigation_code,
    c6.name AS use_navigation,
    d.use_fish_code,
    c7.name AS use_fish,
    d.use_pollution_code,
    c8.name AS use_pollution,
    d.use_invasivespecies_code,
    c9.name AS use_invasivespecies,
    d.use_other_code,
    c10.name AS use_other,
    d.lake_control_code,
    lk.name AS lake_control,
    d.construction_year,
    d.assess_schedule,
    d.expected_life,
    d.maintenance_last,
    d.maintenance_next,
    d.function_code,
    f.name AS function_name,
    d.condition_code,
    dc.name AS dam_condition,
    d.construction_type_code,
    dct.name AS construction_type,
    d.height_m,
    d.length_m,
    d.size_class_code,
    ds.name AS size_class,
    d.spillway_capacity,
    d.spillway_type_code,
    dsp.name AS spillway_type,
    d.reservoir_present,
    d.reservoir_area_skm,
    d.reservoir_depth_m,
    d.storage_capacity_mcm,
    d.avg_rate_of_discharge_ls,
    d.degree_of_regulation_pc,
    d.provincial_flow_req,
    d.federal_flow_req,
    d.catchment_area_skm,
    d.hydro_peaking_system,
    d.upstream_linear_km,
    d.generating_capacity_mwh,
    d.turbine_number,
    d.turbine_type_code,
    dt.name AS turbine_type,
    d.up_passage_type_code,
    up.name AS up_passage_type,
    d.down_passage_route_code,
    down.name AS down_passage_route,
    d.last_modified,
    d.use_analysis,
    d.comments,
    d.passability_status_code,
    ps.name AS passability_status,
    d.passability_status_note,
    d.complete_level_code,
    cl.name AS complete_level,
    d.snapped_point AS geometry
   FROM dams.dams d
     JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.barrier_ownership_type_codes ow ON ow.code = d.ownership_type_code
     LEFT JOIN dams.operating_status_codes os ON os.code = d.operating_status_code
     LEFT JOIN dams.dam_use_codes duc ON duc.code = d.use_code
     LEFT JOIN dams.use_codes c1 ON c1.code = d.use_irrigation_code
     LEFT JOIN dams.use_codes c2 ON c2.code = d.use_electricity_code
     LEFT JOIN dams.use_codes c3 ON c3.code = d.use_supply_code
     LEFT JOIN dams.use_codes c4 ON c4.code = d.use_floodcontrol_code
     LEFT JOIN dams.use_codes c5 ON c5.code = d.use_recreation_code
     LEFT JOIN dams.use_codes c6 ON c6.code = d.use_navigation_code
     LEFT JOIN dams.use_codes c7 ON c7.code = d.use_fish_code
     LEFT JOIN dams.use_codes c8 ON c8.code = d.use_pollution_code
     LEFT JOIN dams.use_codes c9 ON c9.code = d.use_invasivespecies_code
     LEFT JOIN dams.use_codes c10 ON c10.code = d.use_other_code
     LEFT JOIN dams.function_codes f ON f.code = d.function_code
     LEFT JOIN dams.condition_codes dc ON dc.code = d.condition_code
     LEFT JOIN dams.construction_type_codes dct ON dct.code = d.construction_type_code
     LEFT JOIN dams.size_codes ds ON ds.code = d.size_class_code
     LEFT JOIN dams.spillway_type_codes dsp ON dsp.code = d.spillway_type_code
     LEFT JOIN dams.turbine_type_codes dt ON dt.code = d.turbine_type_code
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN dams.downstream_passage_route_codes down ON down.code = d.down_passage_route_code
     LEFT JOIN dams.dam_complete_level_codes cl ON cl.code = d.complete_level_code
     LEFT JOIN dams.lake_control_codes lk ON lk.code = d.lake_control_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code;

GRANT ALL PRIVILEGES ON cabd.dams_view to cabd;
GRANT ALL PRIVILEGES ON cabd.dams_view to egouge;

--reorder fields
UPDATE cabd.feature_type_metadata SET vw_all_order = (vw_all_order + 1)
WHERE view_name = 'cabd.dams_view' AND vw_all_order >=24 AND vw_all_order < 92;

UPDATE cabd.feature_type_metadata SET vw_all_order = 93
WHERE view_name = 'cabd.dams_view' AND field_name = 'last_modified';

UPDATE cabd.feature_type_metadata SET vw_all_order = 95
WHERE view_name = 'cabd.dams_view' AND field_name = 'feature_type';

UPDATE cabd.feature_type_metadata SET vw_all_order = 96
WHERE view_name = 'cabd.dams_view' AND field_name = 'datasource_url';

--add records to feature type metadata table
INSERT INTO cabd.feature_type_metadata(
    view_name,
    field_name,
    name,
    description,
    is_link,
    data_type,
    vw_simple_order,
    vw_all_order,
    include_vector_tile)
values(
    'cabd.dams_view',
    'removed_year',
    'Year Removed',
    'The year the dam was decommissioned, removed, replaced, subsumed, or destroyed.',
    false,
    'integer',
    null,
    24,
    false);

INSERT INTO cabd.feature_type_metadata(
    view_name,
    field_name,
    name,
    description,
    is_link,
    data_type,
    vw_simple_order,
    vw_all_order,
    include_vector_tile)
values(
    'cabd.dams_view',
    'use_analysis',
    'Use for Network Analysis',
    'If true, the data point representing this feature is/should be snapped to hydrographic networks for analysis purposes. Examples of structures that should not be snapped to hydrographic networks include powerhouses, some agricultural pond dams, canal walls, and other lateral barriers.',
    false,
    'boolean',
    null,
    94,
    false);

--add/fix descriptions for various fields
UPDATE cabd.feature_type_metadata 
SET description = 'The level of information available for the dam in the CABD.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'complete_level';

UPDATE cabd.feature_type_metadata 
SET "name" = 'Dam Condition'
WHERE view_name = 'cabd.dams_view' AND field_name = 'condition_code';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of dam structure, categorized by construction material/design.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'construction_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The dam''s physical condition.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'dam_condition';

UPDATE cabd.feature_type_metadata 
SET description = 'The primary use of the dam.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'dam_use';

UPDATE cabd.feature_type_metadata 
SET description = 'Link that identifies the data source for the feature''s attributes.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'datasource_url';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of downstream fish passage route associated with the dam.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'down_passage_route';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of feature the data point represents.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'feature_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The intended function of the dam.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'function_name';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates if a reservoir has been built at the location of an existing natural lake using a lake control structure.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'lake_control';

UPDATE cabd.feature_type_metadata 
SET description = 'The municipality the dam is located in.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'municipality';

UPDATE cabd.feature_type_metadata 
SET description = 'The operating status of the dam.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'operating_status';

UPDATE cabd.feature_type_metadata 
SET description = 'The ownership category associated with the dam.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'ownership_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The province or territory the dam is located in.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'province_territory';

UPDATE cabd.feature_type_metadata 
SET description = 'The size category of the dam based on the height of the dam in meters.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'size_class';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of spillway associated with the dam structure.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'spillway_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of turbine in the dam structure.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'turbine_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of upstream fish passage measure associated with the dam.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'up_passage_type';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used for hydroelectric energy production, and the extent to which hydroelectric production is a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_electricity';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used for fisheries purposes, and the extent to which fisheries are a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_fish';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used for flood control purposes, and the extent to which flood control is a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_floodcontrol';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used to control invasive species and the extent to which invasive species control is a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_invasivespecies';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used for irrigation purposes, and the extent to which irrigation is a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_irrigation';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used for navigation, and the extent to which navigation is a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_navigation';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used for "other" purposes, and the extent to which it is a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_other';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used for pollution control purposes, and the extent to which pollution control is a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_pollution';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used for recreation purposes, and the extent to which recreation is a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_recreation';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the dam is used for water supply purposes, and the extent to which water supply is a planned use.'
WHERE view_name = 'cabd.dams_view' AND field_name = 'use_supply';

-----------------------------------------------------------------
--
--   WATERFALLS
--
-----------------------------------------------------------------

DROP VIEW cabd.waterfalls_view;

--recreate view
CREATE OR REPLACE VIEW cabd.waterfalls_view
 AS
 SELECT w.cabd_id,
    'features/datasources/'::text || w.cabd_id AS datasource_url,
    'waterfalls'::text AS feature_type,
    st_y(w.snapped_point) AS latitude,
    st_x(w.snapped_point) AS longitude,
    w.fall_name_en,
    w.fall_name_fr,
    w.waterbody_name_en,
    w.waterbody_name_fr,
    w.nhn_watershed_id,
    nhn.sub_sub_drainage_area AS nhn_watershed_name,
    w.province_territory_code,
    pt.name AS province_territory,
    w.municipality,
    w.fall_height_m,
    w.last_modified,
    w.use_analysis,
    w.comments,
    w.complete_level_code,
    cl.name AS complete_level,
    w.passability_status_code,
    ps.name AS passability_status,
    w.snapped_point AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code;

GRANT ALL PRIVILEGES ON cabd.waterfalls_view to cabd;
GRANT ALL PRIVILEGES ON cabd.waterfalls_view to egouge;

--reorder fields
UPDATE cabd.feature_type_metadata SET vw_all_order = 10
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'passability_status_code';

UPDATE cabd.feature_type_metadata SET vw_all_order = 11
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'passability_status';

UPDATE cabd.feature_type_metadata SET vw_all_order = 12
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'fall_height_m';

UPDATE cabd.feature_type_metadata SET vw_all_order = 13
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'complete_level_code';

UPDATE cabd.feature_type_metadata SET vw_all_order = 14
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'complete_level';

UPDATE cabd.feature_type_metadata SET vw_all_order = 15
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'municipality';

UPDATE cabd.feature_type_metadata SET vw_all_order = 16
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'comments';

UPDATE cabd.feature_type_metadata SET vw_all_order = 17
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'nhn_watershed_id';

UPDATE cabd.feature_type_metadata SET vw_all_order = 18
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'nhn_watershed_name';

UPDATE cabd.feature_type_metadata SET vw_all_order = 19
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'last_modified';

UPDATE cabd.feature_type_metadata SET vw_all_order = 21
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'feature_type';

UPDATE cabd.feature_type_metadata SET vw_all_order = 22
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'datasource_url';

--add record to feature type metadata table
INSERT INTO cabd.feature_type_metadata(
    view_name,
    field_name,
    name,
    description,
    is_link,
    data_type,
    vw_simple_order,
    vw_all_order,
    include_vector_tile)
values(
    'cabd.waterfalls_view',
    'use_analysis',
    'Use for Network Analysis',
    'If true, the data point representing this feature is/should be snapped to hydrographic networks for analysis purposes.',
    false,
    'boolean',
    null,
    20,
    false);

--add/fix descriptions for various fields
UPDATE cabd.feature_type_metadata 
SET description = 'The level of information available for the waterfall in the CABD.'
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'complete_level';

UPDATE cabd.feature_type_metadata 
SET description = 'Link that identifies the data source for the feature''s attributes.'
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'datasource_url';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of feature the data point represents.'
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'feature_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The municipality the waterfall is located in.'
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'municipality';

UPDATE cabd.feature_type_metadata 
SET description = 'The degree to which the waterfall acts as a barrier to fish in the upstream direction.'
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'passability_status';

UPDATE cabd.feature_type_metadata 
SET description = 'The province or territory the waterfall is located in.'
WHERE view_name = 'cabd.waterfalls_view' AND field_name = 'province_territory';

-----------------------------------------------------------------
--
--   FISHWAYS
--
-----------------------------------------------------------------
--reorder fields
UPDATE cabd.feature_type_metadata SET vw_all_order = (vw_all_order + 1)
WHERE view_name = 'cabd.fishways_view' AND vw_all_order >=43 AND vw_all_order < 51;

--add record to feature type metadata table
INSERT INTO cabd.feature_type_metadata(
    view_name,
    field_name,
    name,
    description,
    is_link,
    data_type,
    vw_simple_order,
    vw_all_order,
    include_vector_tile)
values(
    'cabd.fishways_view',
    'operated_by',
    'Operated By',
    'Agency responsible for operating the fishway.',
    false,
    'text',
    null,
    43,
    false);

--add/fix descriptions for various fields
UPDATE cabd.feature_type_metadata 
SET description = 'Company/organization that designed the fishway structure.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'architect';

UPDATE cabd.feature_type_metadata 
SET description = 'Unique identifier for each fishway point.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'cabd_id';

UPDATE cabd.feature_type_metadata 
SET description = 'The level of information available for the fishway in the CABD.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'complete_level';

UPDATE cabd.feature_type_metadata 
SET description = 'Name of the company that constructed the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'constructed_by';

UPDATE cabd.feature_type_metadata 
SET description = 'Name of the agency that contracted the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'contracted_by';

UPDATE cabd.feature_type_metadata 
SET description = 'The unique barrier identifier corresponding to the dam that the fishway structure is associated with.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'dam_id';

UPDATE cabd.feature_type_metadata 
SET description = 'Link that identifies the data source for the feature''s attributes.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'datasource_url';

UPDATE cabd.feature_type_metadata 
SET description = 'Depth of fishway channel, in meters, during operation.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'depth_m';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates whether the fishway was designed based on the biology of the species.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'designed_on_biology';

UPDATE cabd.feature_type_metadata 
SET description = 'Change in height between fishway exit and entrance in meters.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'elevation_m';

UPDATE cabd.feature_type_metadata 
SET description = 'Notes regarding design and construction of the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'engineering_notes';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates if the entrance of the fishway is located mid-stream or on the bank.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'entrance_location';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates the entrance position of the fishway in the water column.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'entrance_position';

UPDATE cabd.feature_type_metadata 
SET description = 'Portion of individuals attracted to the fishway in percent.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'estimate_of_attraction_pct';

UPDATE cabd.feature_type_metadata 
SET description = 'Estimated percentage of individuals that successfully pass through the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'estimate_of_passage_success_pct';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of feature the data point represents.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'feature_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of fishway structure (values are consistent with ''Upstream Passage Type'' values for dams).'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'fishpass_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The reference for the literature (peer-reviewed and "grey") used to gather additional information about the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'fishway_reference_id';

UPDATE cabd.feature_type_metadata 
SET description = 'The fishway''s angle of inclination in percent.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'gradient';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates whether an evaluation study has been performed at the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'has_evaluating_studies';

UPDATE cabd.feature_type_metadata 
SET description = 'Species where it is known that the fishway presents a significant barrier to migration.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'known_notuse';

UPDATE cabd.feature_type_metadata 
SET description = 'Species that are known to use the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'known_use';

UPDATE cabd.feature_type_metadata 
SET description = 'The geographic x-coordinate representing the location of the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'latitude';

UPDATE cabd.feature_type_metadata 
SET description = 'The length of the fishway in meters.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'length_m';

UPDATE cabd.feature_type_metadata 
SET description = 'The geographic y-coordinate representing the location of the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'longitude';

UPDATE cabd.feature_type_metadata 
SET description = 'Maximum velocity of water flow recorded in the fishway in m/s.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'max_fishway_velocity_ms';

UPDATE cabd.feature_type_metadata 
SET description = 'Average velocity of water flow through the fishway in m/s.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'mean_fishway_velocity_ms';

UPDATE cabd.feature_type_metadata 
SET description = 'Purpose of post-construction modifications.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'modification_purpose';

UPDATE cabd.feature_type_metadata 
SET description = 'The year that post-construction modifications were completed.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'modification_year';

UPDATE cabd.feature_type_metadata 
SET description = 'Indicates if the fishway has had any post-construction modifications.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'modified';

UPDATE cabd.feature_type_metadata 
SET description = 'Monitoring equipment used at the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'monitoring_equipment';

UPDATE cabd.feature_type_metadata 
SET description = 'The municipality the fishway is located in.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'municipality';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of evaluation study performed.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'nature_of_evaluation_studies';

UPDATE cabd.feature_type_metadata 
SET description = 'Unstructured comments on important operation considerations for the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'operating_notes';

UPDATE cabd.feature_type_metadata 
SET description = 'The dates the fishway is in operation.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'operation_period';

UPDATE cabd.feature_type_metadata 
SET description = 'Name of the agency that possesses the plans for the fishway.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'plans_held_by';

UPDATE cabd.feature_type_metadata 
SET description = 'The province or territory the fishway is located in.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'province_territory';

UPDATE cabd.feature_type_metadata 
SET description = 'The reason the fishway was designed and implemented.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'purpose';

UPDATE cabd.feature_type_metadata 
SET description = 'Name of river/stream in which the fishway is recorded (English).'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'river_name_en';

UPDATE cabd.feature_type_metadata 
SET description = 'Name of river/stream in which the fishway is recorded (French).'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'river_name_fr';

UPDATE cabd.feature_type_metadata 
SET description = 'The given or known name of the fishway structure or the dam with which it is associated (English).'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'structure_name_en';

UPDATE cabd.feature_type_metadata 
SET description = 'The given or known name of the fishway structure or the dam with which it is associated (French).'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'structure_name_fr';

UPDATE cabd.feature_type_metadata 
SET description = 'Name of waterbody in which the fishway is recorded (English).'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'waterbody_name_en';

UPDATE cabd.feature_type_metadata 
SET description = 'Name of waterbody in which the fishway is recorded (French).'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'waterbody_name_fr';

UPDATE cabd.feature_type_metadata 
SET description = 'Year in which the fishway structure was built.'
WHERE view_name = 'cabd.fishways_view' AND field_name = 'year_constructed';

-----------------------------------------------------------------
--
--   BARRIERS
--
-----------------------------------------------------------------

DROP VIEW cabd.barriers_view;

--recreate view
CREATE OR REPLACE VIEW cabd.barriers_view
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name AS province_territory,
    barriers.nhn_watershed_id,
    nhn.sub_sub_drainage_area AS nhn_watershed_name,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name AS passability_status,
    barriers.use_analysis,
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
           FROM waterfalls.waterfalls) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;
     
GRANT ALL PRIVILEGES ON cabd.barriers_view to cabd;
GRANT ALL PRIVILEGES ON cabd.barriers_view to egouge;

--reorder fields
UPDATE cabd.feature_type_metadata SET vw_all_order = 16
WHERE view_name = 'cabd.barriers_view' AND field_name = 'feature_type';

UPDATE cabd.feature_type_metadata SET vw_all_order = 17
WHERE view_name = 'cabd.barriers_view' AND field_name = 'datasource_url';

--add record to feature type metadata table
INSERT INTO cabd.feature_type_metadata(
    view_name,
    field_name,
    name,
    description,
    is_link,
    data_type,
    vw_simple_order,
    vw_all_order,
    include_vector_tile)
values(
    'cabd.barriers_view',
    'use_analysis',
    'Use for Network Analysis',
    'If true, the data point representing this feature is/should be snapped to hydrographic networks for analysis purposes. Examples of structures that should not be snapped to hydrographic networks include powerhouses, some agricultural pond dams, canal walls, and other lateral barriers.',
    false,
    'boolean',
    null,
    15,
    false);

--add/fix descriptions for various fields
UPDATE cabd.feature_type_metadata 
SET description = 'Link that identifies the data source for the feature''s attributes.'
WHERE view_name = 'cabd.barriers_view' AND field_name = 'datasource_url';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of feature the data point represents.'
WHERE view_name = 'cabd.barriers_view' AND field_name = 'feature_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The province or territory the feature is located in.'
WHERE view_name = 'cabd.barriers_view' AND field_name = 'province_territory';

-----------------------------------------------------------------
--
--   ALL FEATURES
--
-----------------------------------------------------------------

DROP VIEW cabd.all_features_view;

--recreate view
CREATE OR REPLACE VIEW cabd.all_features_view
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.barrier_type AS feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name AS province_territory,
    barriers.nhn_watershed_id,
    nhn.sub_sub_drainage_area AS nhn_watershed_name,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name AS passability_status,
    barriers.use_analysis,
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
            NULL::character varying(512) AS name_en,
            NULL::character varying(512) AS name_fr,
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
           FROM fishways.fishways) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;

GRANT ALL PRIVILEGES ON cabd.all_features_view to cabd;
GRANT ALL PRIVILEGES ON cabd.all_features_view to egouge;

--reorder fields

UPDATE cabd.feature_type_metadata SET vw_all_order = 17
WHERE view_name = 'cabd.all_features_view' AND field_name = 'datasource_url';

--add record to feature type metadata table
INSERT INTO cabd.feature_type_metadata(
    view_name,
    field_name,
    name,
    description,
    is_link,
    data_type,
    vw_simple_order,
    vw_all_order,
    include_vector_tile)
values(
    'cabd.all_features_view',
    'use_analysis',
    'Use for Network Analysis',
    'If true, the data point representing this feature is/should be snapped to hydrographic networks for analysis purposes. Examples of structures that should not be snapped to hydrographic networks include powerhouses, some agricultural pond dams, canal walls, and other lateral barriers.',
    false,
    'boolean',
    null,
    16,
    false);

--add/fix descriptions for various fields
UPDATE cabd.feature_type_metadata 
SET description = 'Link that identifies the data source for the feature''s attributes.'
WHERE view_name = 'cabd.all_features_view' AND field_name = 'datasource_url';

UPDATE cabd.feature_type_metadata 
SET description = 'The type of feature the data point represents.'
WHERE view_name = 'cabd.all_features_view' AND field_name = 'feature_type';

UPDATE cabd.feature_type_metadata 
SET description = 'The province or territory the feature is located in.'
WHERE view_name = 'cabd.all_features_view' AND field_name = 'province_territory';