-- ensure latitude/longitude attributes are included in the mobile attribute
-- set for all features types

with max_order as (
	select view_name, max(vw_mobile_order) as nextorder from cabd.feature_type_metadata ftm group by view_name
)
update cabd.feature_type_metadata 
set vw_mobile_order = case when b.nextorder is null then 0 else b.nextorder end +1 from 
max_order b where cabd.feature_type_metadata.view_name = b.view_name and field_name = 'latitude' and vw_mobile_order is null

with max_order as (
	select view_name, max(vw_mobile_order) as nextorder from cabd.feature_type_metadata ftm group by view_name
)
update cabd.feature_type_metadata 
set vw_mobile_order = case when b.nextorder is null then 0 else b.nextorder end +1 from 
max_order b where cabd.feature_type_metadata.view_name = b.view_name and field_name = 'longitude' and vw_mobile_order is null

-- ensure latitude/longitude attributes are included in the vector tiles for 
-- dams and stream crossings
update cabd.feature_type_metadata set include_vector_tile  = true where field_name = 'latitude' or field_name = 'longitude'
and view_name in ('cabd.dams_view', 'cabd.stream_crossings_sites_structures_view');

delete from cabd.vector_tile_cache where key ilike 'dams%' or key ilike 'stream_crossings%';


set role cabd;

-- update these views to ensure the latitude/longitude values are always populated
-- even if the feature isn't snapped
CREATE OR REPLACE VIEW cabd.stream_crossings_sites_structures_view_fr
AS SELECT t.cabd_id,
    'stream_crossings'::text AS feature_type,
    'features/datasources/'::text || t.cabd_id AS datasource_url,
    'features/structures?filter=site_id:eq:'::text || t.cabd_id AS structures_url,
    st_y( CASE
            WHEN t.snapped_point IS NOT NULL THEN t.snapped_point
            ELSE t.original_point
        END) AS latitude,
        
     st_x(    CASE
            WHEN t.snapped_point IS NOT NULL THEN t.snapped_point
            ELSE t.original_point
        END) AS longitude,  
    t.last_modified,
    t.other_id,
    t.cabd_assessment_id,
    t.original_assessment_id,
    t.date_assessed,
    t.lead_assessor,
    t.municipality,
    t.stream_name,
    t.road_name,
    t.road_type_code,
    rt.name_fr AS road_type_name,
    t.location_description,
    t.land_ownership_context,
    t.incomplete_assess_code,
    ia.name_fr AS incomplete_assess_name,
    t.crossing_type_code,
    ct.name_fr AS crossing_type_name,
    t.num_structures,
    t.photo_id_inlet,
    t.photo_id_outlet,
    t.photo_id_upstream,
    t.photo_id_downstream,
    t.photo_id_road_surface,
    t.photo_id_other_a,
    t.photo_id_other_b,
    t.photo_id_other_c,
    t.flow_condition_code,
    fc.name_fr AS flow_condition_name,
    t.crossing_condition_code,
    con.name_fr AS crossing_condition_name,
    t.site_type_code,
    st.name_fr AS site_type_name,
    t.alignment_code,
    ac.name_fr AS alignment_name,
    t.road_fill_height_m,
    t.bankfull_width_upstr_a_m,
    t.bankfull_width_upstr_b_m,
    t.bankfull_width_upstr_c_m,
    t.bankfull_width_upstr_avg_m,
    t.bankfull_width_dnstr_a_m,
    t.bankfull_width_dnstr_b_m,
    t.bankfull_width_dnstr_c_m,
    t.bankfull_width_dnstr_avg_m,
    t.bankfull_confidence_code,
    cc.name_fr AS bankfull_confidence_name,
    t.scour_pool_tailwater_code,
    sc.name_fr AS scour_pool_tailwater_name,
    t.crossing_comments,
    t.province_territory_code,
    pt.name_fr AS province_territory,
    t.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    t.strahler_order,
    t.assessment_type_code,
    atc.name_en AS assessment_type_name,
    t.addressed_status_code,
    adrc.name_en AS addressed_status_name,
    t.chu_12_id,
    t.chu_10_id,
    t.chu_8_id,
    t.chu_6_id,
    t.chu_4_id,
    t.chu_2_id,
    t.include_in_act,
    s.structure_id,
    s.last_modified AS structure_last_modified,
    s.primary_structure,
    s.structure_number,
    s.outlet_shape_code,
    os.name_fr AS outlet_shape,
    s.internal_structures_code,
    istruct.name_fr AS internal_structures,
    s.liner_material_code,
    lm.name_fr AS liner_material,
    s.outlet_armouring_code,
    oa.name_fr AS outlet_armouring,
    s.outlet_grade_code,
    og.name_fr AS outlet_grade,
    s.outlet_width_m,
    s.outlet_height_m,
    s.outlet_substrate_water_width_m,
    s.outlet_water_depth_m,
    s.abutment_height_m,
    s.outlet_drop_to_water_surface_m,
    s.outlet_drop_to_stream_bottom_m,
    s.outlet_water_surface_to_residual_pool_top_m,
    s.residual_pool_confidence_code,
    rpc.name_fr AS residual_pool_confidence,
    s.structure_length_m,
    s.inlet_shape_code,
    ishp.name_fr AS inlet_shape,
    s.inlet_type_code,
    it.name_fr AS inlet_type,
    s.inlet_grade_code,
    ig.name_fr AS inlet_grade,
    s.inlet_width_m,
    s.inlet_height_m,
    s.inlet_substrate_water_width_m,
    s.inlet_water_depth_m,
    s.structure_slope_pct,
    s.structure_slope_method_code,
    sm.name_fr AS slope_method,
    s.structure_slope_to_channel_code,
    rsc.name_fr AS structure_slope_to_channel,
    s.substrate_type_code,
    rst.name_fr AS substrate_type,
    s.substrate_matches_stream_code,
    sms.name_fr AS substrate_matches,
    s.substrate_coverage_code,
    scov.name_fr AS substrate_coverage,
    s.substrate_depth_consistent_code,
    sdc.name_fr AS substrate_depth_consistent,
    s.backwatered_pct_code,
    bwp.name_fr AS backwatered_pct,
    s.physical_blockage_severity_code,
    pbs.name_fr AS blockage_severity,
    s.water_depth_matches_stream_code,
    wdms.name_fr AS water_depth_matches,
    s.water_velocity_matches_stream_code,
    wvms.name_fr AS water_velocity_matches,
    s.dry_passage_code,
    dp.name_fr AS dry_passage,
    s.height_above_dry_passage_m,
    s.structure_comments,
    s.passability_status_code,
    psc.name_fr AS passability_status,
        CASE
            WHEN t.snapped_point IS NOT NULL THEN t.snapped_point
            ELSE t.original_point
        END AS geometry
   FROM stream_crossings.sites t
     JOIN stream_crossings.structures s ON t.cabd_id = s.site_id
     LEFT JOIN cabd.province_territory_codes pt ON pt.code::text = t.province_territory_code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = t.nhn_watershed_id::text
     LEFT JOIN stream_crossings.alignment_codes ac ON t.alignment_code = ac.code
     LEFT JOIN stream_crossings.confidence_codes cc ON t.bankfull_confidence_code = cc.code
     LEFT JOIN stream_crossings.crossing_condition_codes con ON t.crossing_condition_code = con.code
     LEFT JOIN stream_crossings.crossing_type_codes ct ON t.crossing_type_code = ct.code
     LEFT JOIN stream_crossings.flow_condition_codes fc ON t.flow_condition_code = fc.code
     LEFT JOIN stream_crossings.incomplete_assessment_codes ia ON t.incomplete_assess_code = ia.code
     LEFT JOIN cabd.road_type_codes rt ON t.road_type_code = rt.code
     LEFT JOIN stream_crossings.scour_pool_codes sc ON t.scour_pool_tailwater_code = sc.code
     LEFT JOIN stream_crossings.site_type_codes st ON t.site_type_code = st.code
     LEFT JOIN stream_crossings.shape_codes os ON s.outlet_shape_code = os.code
     LEFT JOIN stream_crossings.internal_structure_codes istruct ON s.internal_structures_code = istruct.code
     LEFT JOIN stream_crossings.material_codes lm ON s.liner_material_code = lm.code
     LEFT JOIN stream_crossings.armouring_codes oa ON s.outlet_armouring_code = oa.code
     LEFT JOIN stream_crossings.grade_codes og ON s.outlet_grade_code = og.code
     LEFT JOIN stream_crossings.confidence_codes rpc ON s.residual_pool_confidence_code = rpc.code
     LEFT JOIN stream_crossings.shape_codes ishp ON s.inlet_shape_code = ishp.code
     LEFT JOIN stream_crossings.inlet_type_codes it ON s.inlet_type_code = it.code
     LEFT JOIN stream_crossings.grade_codes ig ON s.inlet_grade_code = ig.code
     LEFT JOIN stream_crossings.slope_method_codes sm ON s.structure_slope_method_code = sm.code
     LEFT JOIN stream_crossings.relative_slope_codes rsc ON s.structure_slope_to_channel_code = rsc.code
     LEFT JOIN stream_crossings.substrate_type_codes rst ON s.substrate_type_code = rst.code
     LEFT JOIN stream_crossings.substrate_matches_stream_codes sms ON s.substrate_matches_stream_code = sms.code
     LEFT JOIN stream_crossings.structure_coverage_codes scov ON s.substrate_coverage_code = scov.code
     LEFT JOIN cabd.response_codes sdc ON s.substrate_depth_consistent_code = sdc.code
     LEFT JOIN stream_crossings.structure_coverage_codes bwp ON s.backwatered_pct_code = bwp.code
     LEFT JOIN stream_crossings.blockage_severity_codes pbs ON s.physical_blockage_severity_code = pbs.code
     LEFT JOIN stream_crossings.water_depth_matches_stream_codes wdms ON s.water_depth_matches_stream_code = wdms.code
     LEFT JOIN stream_crossings.water_velocity_matches_stream_codes wvms ON s.water_velocity_matches_stream_code = wvms.code
     LEFT JOIN cabd.response_codes dp ON s.dry_passage_code = dp.code
     LEFT JOIN cabd.passability_status_codes psc ON s.passability_status_code = psc.code
     LEFT JOIN cabd.assessment_type_codes atc ON t.assessment_type_code = atc.code
     LEFT JOIN cabd.addressed_status_codes adrc ON t.addressed_status_code = adrc.code
  WHERE s.primary_structure;
  
  
  
  
  -- cabd.stream_crossings_sites_structures_view_en source

CREATE OR REPLACE VIEW cabd.stream_crossings_sites_structures_view_en
AS SELECT t.cabd_id,
    'stream_crossings'::text AS feature_type,
    'features/datasources/'::text || t.cabd_id AS datasource_url,
    'features/structures?filter=site_id:eq:'::text || t.cabd_id AS structures_url,
    
    st_y( CASE
            WHEN t.snapped_point IS NOT NULL THEN t.snapped_point
            ELSE t.original_point
        END) AS latitude,
        
     st_x(    CASE
            WHEN t.snapped_point IS NOT NULL THEN t.snapped_point
            ELSE t.original_point
        END) AS longitude,        
    t.last_modified,
    t.other_id,
    t.cabd_assessment_id,
    t.original_assessment_id,
    t.date_assessed,
    t.lead_assessor,
    t.municipality,
    t.stream_name,
    t.road_name,
    t.road_type_code,
    rt.name_en AS road_type_name,
    t.location_description,
    t.land_ownership_context,
    t.incomplete_assess_code,
    ia.name_en AS incomplete_assess_name,
    t.crossing_type_code,
    ct.name_en AS crossing_type_name,
    t.num_structures,
    t.photo_id_inlet,
    t.photo_id_outlet,
    t.photo_id_upstream,
    t.photo_id_downstream,
    t.photo_id_road_surface,
    t.photo_id_other_a,
    t.photo_id_other_b,
    t.photo_id_other_c,
    t.flow_condition_code,
    fc.name_en AS flow_condition_name,
    t.crossing_condition_code,
    con.name_en AS crossing_condition_name,
    t.site_type_code,
    st.name_en AS site_type_name,
    t.alignment_code,
    ac.name_en AS alignment_name,
    t.road_fill_height_m,
    t.bankfull_width_upstr_a_m,
    t.bankfull_width_upstr_b_m,
    t.bankfull_width_upstr_c_m,
    t.bankfull_width_upstr_avg_m,
    t.bankfull_width_dnstr_a_m,
    t.bankfull_width_dnstr_b_m,
    t.bankfull_width_dnstr_c_m,
    t.bankfull_width_dnstr_avg_m,
    t.bankfull_confidence_code,
    cc.name_en AS bankfull_confidence_name,
    t.scour_pool_tailwater_code,
    sc.name_en AS scour_pool_tailwater_name,
    t.crossing_comments,
    t.province_territory_code,
    pt.name_en AS province_territory,
    t.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    t.strahler_order,
    t.assessment_type_code,
    atc.name_en AS assessment_type_name,
    t.addressed_status_code,
    adrc.name_en AS addressed_status_name,
    t.chu_12_id,
    t.chu_10_id,
    t.chu_8_id,
    t.chu_6_id,
    t.chu_4_id,
    t.chu_2_id,
    t.include_in_act,
    s.structure_id,
    s.last_modified AS structure_last_modified,
    s.primary_structure,
    s.structure_number,
    s.outlet_shape_code,
    os.name_en AS outlet_shape,
    s.internal_structures_code,
    istruct.name_en AS internal_structures,
    s.liner_material_code,
    lm.name_en AS liner_material,
    s.outlet_armouring_code,
    oa.name_en AS outlet_armouring,
    s.outlet_grade_code,
    og.name_en AS outlet_grade,
    s.outlet_width_m,
    s.outlet_height_m,
    s.outlet_substrate_water_width_m,
    s.outlet_water_depth_m,
    s.abutment_height_m,
    s.outlet_drop_to_water_surface_m,
    s.outlet_drop_to_stream_bottom_m,
    s.outlet_water_surface_to_residual_pool_top_m,
    s.residual_pool_confidence_code,
    rpc.name_en AS residual_pool_confidence,
    s.structure_length_m,
    s.inlet_shape_code,
    ishp.name_en AS inlet_shape,
    s.inlet_type_code,
    it.name_en AS inlet_type,
    s.inlet_grade_code,
    ig.name_en AS inlet_grade,
    s.inlet_width_m,
    s.inlet_height_m,
    s.inlet_substrate_water_width_m,
    s.inlet_water_depth_m,
    s.structure_slope_pct,
    s.structure_slope_method_code,
    sm.name_en AS slope_method,
    s.structure_slope_to_channel_code,
    rsc.name_en AS structure_slope_to_channel,
    s.substrate_type_code,
    rst.name_en AS substrate_type,
    s.substrate_matches_stream_code,
    sms.name_en AS substrate_matches,
    s.substrate_coverage_code,
    scov.name_en AS substrate_coverage,
    s.substrate_depth_consistent_code,
    sdc.name_en AS substrate_depth_consistent,
    s.backwatered_pct_code,
    bwp.name_en AS backwatered_pct,
    s.physical_blockage_severity_code,
    pbs.name_en AS blockage_severity,
    s.water_depth_matches_stream_code,
    wdms.name_en AS water_depth_matches,
    s.water_velocity_matches_stream_code,
    wvms.name_en AS water_velocity_matches,
    s.dry_passage_code,
    dp.name_en AS dry_passage,
    s.height_above_dry_passage_m,
    s.structure_comments,
    s.passability_status_code,
    psc.name_en AS passability_status,
        CASE
            WHEN t.snapped_point IS NOT NULL THEN t.snapped_point
            ELSE t.original_point
        END AS geometry
   FROM stream_crossings.sites t
     JOIN stream_crossings.structures s ON t.cabd_id = s.site_id
     LEFT JOIN cabd.province_territory_codes pt ON pt.code::text = t.province_territory_code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = t.nhn_watershed_id::text
     LEFT JOIN stream_crossings.alignment_codes ac ON t.alignment_code = ac.code
     LEFT JOIN stream_crossings.confidence_codes cc ON t.bankfull_confidence_code = cc.code
     LEFT JOIN stream_crossings.crossing_condition_codes con ON t.crossing_condition_code = con.code
     LEFT JOIN stream_crossings.crossing_type_codes ct ON t.crossing_type_code = ct.code
     LEFT JOIN stream_crossings.flow_condition_codes fc ON t.flow_condition_code = fc.code
     LEFT JOIN stream_crossings.incomplete_assessment_codes ia ON t.incomplete_assess_code = ia.code
     LEFT JOIN cabd.road_type_codes rt ON t.road_type_code = rt.code
     LEFT JOIN stream_crossings.scour_pool_codes sc ON t.scour_pool_tailwater_code = sc.code
     LEFT JOIN stream_crossings.site_type_codes st ON t.site_type_code = st.code
     LEFT JOIN stream_crossings.shape_codes os ON s.outlet_shape_code = os.code
     LEFT JOIN stream_crossings.internal_structure_codes istruct ON s.internal_structures_code = istruct.code
     LEFT JOIN stream_crossings.material_codes lm ON s.liner_material_code = lm.code
     LEFT JOIN stream_crossings.armouring_codes oa ON s.outlet_armouring_code = oa.code
     LEFT JOIN stream_crossings.grade_codes og ON s.outlet_grade_code = og.code
     LEFT JOIN stream_crossings.confidence_codes rpc ON s.residual_pool_confidence_code = rpc.code
     LEFT JOIN stream_crossings.shape_codes ishp ON s.inlet_shape_code = ishp.code
     LEFT JOIN stream_crossings.inlet_type_codes it ON s.inlet_type_code = it.code
     LEFT JOIN stream_crossings.grade_codes ig ON s.inlet_grade_code = ig.code
     LEFT JOIN stream_crossings.slope_method_codes sm ON s.structure_slope_method_code = sm.code
     LEFT JOIN stream_crossings.relative_slope_codes rsc ON s.structure_slope_to_channel_code = rsc.code
     LEFT JOIN stream_crossings.substrate_type_codes rst ON s.substrate_type_code = rst.code
     LEFT JOIN stream_crossings.substrate_matches_stream_codes sms ON s.substrate_matches_stream_code = sms.code
     LEFT JOIN stream_crossings.structure_coverage_codes scov ON s.substrate_coverage_code = scov.code
     LEFT JOIN cabd.response_codes sdc ON s.substrate_depth_consistent_code = sdc.code
     LEFT JOIN stream_crossings.structure_coverage_codes bwp ON s.backwatered_pct_code = bwp.code
     LEFT JOIN stream_crossings.blockage_severity_codes pbs ON s.physical_blockage_severity_code = pbs.code
     LEFT JOIN stream_crossings.water_depth_matches_stream_codes wdms ON s.water_depth_matches_stream_code = wdms.code
     LEFT JOIN stream_crossings.water_velocity_matches_stream_codes wvms ON s.water_velocity_matches_stream_code = wvms.code
     LEFT JOIN cabd.response_codes dp ON s.dry_passage_code = dp.code
     LEFT JOIN cabd.passability_status_codes psc ON s.passability_status_code = psc.code
     LEFT JOIN cabd.assessment_type_codes atc ON t.assessment_type_code = atc.code
     LEFT JOIN cabd.addressed_status_codes adrc ON t.addressed_status_code = adrc.code
  WHERE s.primary_structure;













  -- add lat/long of the cabd feature to the community data api:

CREATE OR REPLACE VIEW cabd.community_data_staging_view
AS SELECT a.id,
    a.cabd_id,
    a.user_id,
    a.uploaded_datetime,
    a.data,
    'stream_crossings'::text AS feature_type,
        CASE
            WHEN b.status IS NULL THEN 'NEW'::character varying
            ELSE b.status::character varying
        END AS status,
        CASE
            WHEN b.passability_status_code IS NULL THEN 4
            ELSE b.passability_status_code::integer
        END AS passability_status_code,
        st_y(case when c.snapped_point is null then c.original_point else c.snapped_point end) as cabd_latitude,
        st_x(case when c.snapped_point is null then c.original_point else c.snapped_point end) as cabd_longitude
   FROM stream_crossings.stream_crossings_community_staging a
     LEFT JOIN cabd.community_holding b ON a.id = b.id
     left join stream_crossings.sites c on c.cabd_id = a.cabd_id
UNION
 SELECT a.id,
    a.cabd_id,
    a.user_id,
    a.uploaded_datetime,
    a.data,
    'dams'::text AS feature_type,
    'NEW'::character varying AS status,
        CASE
            WHEN b.passability_status_code IS NULL THEN 4
            ELSE b.passability_status_code::integer
        END AS passability_status_code,
        st_y(c.snapped_point) as cabd_latitude,
        st_x(c.snapped_point) as cabd_longitude
   FROM dams.dams_community_staging a
     LEFT JOIN cabd.community_holding b ON a.id = b.id
     left join dams.dams c on c.cabd_id = a.cabd_id;

