set role cabd;

alter table stream_crossings.structures add constraint structures_passability_status_code_fk foreign key (passability_status_code) references cabd.passability_status_codes(code);

alter table stream_crossings.sites add constraint site_province_territory_code_fk foreign key (province_territory_code) REFERENCES cabd.province_territory_codes(code);
alter table stream_crossings.sites add constraint site_nhn_watershed_id_fk foreign key (nhn_watershed_id) REFERENCES cabd.nhn_workunit(id);

delete from cabd.feature_type_metadata ftm where view_name = 'cabd.structures_view' and field_name = 'structure_id';	

drop VIEW if exists cabd.stream_crossings_sites_structures_view_en;
drop VIEW if exists cabd.stream_crossings_sites_structures_view_fr;

CREATE OR REPLACE VIEW cabd.stream_crossings_sites_structures_view_en AS
 SELECT 
    t.cabd_id,
    'stream_crossings'::text AS feature_type,
    'features/datasources/'::text || t.cabd_id AS datasource_url,
    'features/structures?filter=site_id:eq:'::text || t.cabd_id AS structures_url,  
    st_y(t.snapped_point) AS latitude,
    st_x(t.snapped_point) AS longitude,
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
    pt.name_en as province_territory,
    t.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,

    t.strahler_order,
    t.assessment_type_code,
    t.addressed_status_code,
    t.chu_12_id,
    t.chu_10_id,
    t.chu_8_id,
    t.chu_6_id,
    t.chu_4_id,
    t.chu_2_id,
    t.include_in_act,
    
    s.structure_id AS structure_id,  
    s.last_modified as structure_last_modified,
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
    psc.name_en as passability_status,
    
    case when t.snapped_point is not null then t.snapped_point else t.original_point end as geometry
  
   FROM stream_crossings.sites t
     JOIN stream_crossings.structures s ON t.cabd_id = s.site_id
     
     LEFT JOIN cabd.province_territory_codes pt ON pt.code = t.province_territory_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id = t.nhn_watershed_id
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
 where s.primary_structure;






CREATE OR REPLACE VIEW cabd.stream_crossings_sites_structures_view_fr AS
 SELECT 
    t.cabd_id,
    'stream_crossings'::text AS feature_type,
    'features/datasources/'::text || t.cabd_id AS datasource_url,    
    'features/structures?filter=site_id:eq:'::text || t.cabd_id AS structures_url,
    st_y(t.snapped_point) AS latitude,
    st_x(t.snapped_point) AS longitude,
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
    pt.name_fr as province_territory,
    t.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,

    t.strahler_order,
    t.assessment_type_code,
    t.addressed_status_code,
    t.chu_12_id,
    t.chu_10_id,
    t.chu_8_id,
    t.chu_6_id,
    t.chu_4_id,
    t.chu_2_id,
    t.include_in_act,
    
    s.structure_id AS structure_id,  
    s.last_modified as structure_last_modified,
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
    psc.name_fr as passability_status,
    case when t.snapped_point is not null then t.snapped_point else t.original_point end as geometry
  
   FROM stream_crossings.sites t
     JOIN stream_crossings.structures s ON t.cabd_id = s.site_id
     
     LEFT JOIN cabd.province_territory_codes pt ON pt.code = t.province_territory_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id = t.nhn_watershed_id
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
 where s.primary_structure;


delete from cabd.feature_type_metadata where view_name = 'cabd.stream_crossings_sites_structures_view';

insert into cabd.feature_type_metadata(view_name,field_name,name_en,description_en,is_link,data_type,vw_simple_order,vw_all_order,include_vector_tile,value_options_reference,name_fr,description_fr,is_name_search,shape_field_name,vw_mobile_order) values
('cabd.stream_crossings_sites_structures_view','cabd_id','cabd id',null,false,'uuid',1,1,true,null,'cabd id',null,false,'cabd_id',1),
('cabd.stream_crossings_sites_structures_view','feature_type','feature type',null,false,'varchar',2,2,true,null,'feature type',null,false,'feat_typ',2),
('cabd.stream_crossings_sites_structures_view','datasource_url','datasource url',null,true,'url',null,3,false,null,'datasource url',null,false,'data_src',null),
('cabd.stream_crossings_sites_structures_view','latitude','latitude',null,false,'double',null,4,false,null,'latitude',null,false,'lat_dd',null),
('cabd.stream_crossings_sites_structures_view','longitude','longitude',null,false,'double',null,5,false,null,'longitude',null,false,'long_dd',null),
('cabd.stream_crossings_sites_structures_view','last_modified','last modified',null,false,'timestamp without time zone',null,6,false,null,'last modified',null,false,'last_mod',null),
('cabd.stream_crossings_sites_structures_view','other_id','other id',null,false,'varchar',null,'7',false,null,'other id',null,false,'other_id',null),
('cabd.stream_crossings_sites_structures_view','cabd_assessment_id','cabd assessment id',null,false,'uuid',null,'8',false,null,'cabd assessment id',null,false,'assess_id',null),
('cabd.stream_crossings_sites_structures_view','original_assessment_id','original assessment id',null,false,'varchar',null,'9',false,null,'original assessment id',null,false,'o_asses_id',null),
('cabd.stream_crossings_sites_structures_view','date_assessed','date assessed',null,false,'date',null,'10',false,null,'date assessed',null,false,'d_assess',null),
('cabd.stream_crossings_sites_structures_view','lead_assessor','lead assessor',null,false,'varchar',null,'11',false,null,'lead assessor',null,false,'l_assessor',null),
('cabd.stream_crossings_sites_structures_view','municipality','municipality',null,false,'varchar',null,'12',false,null,'municipality',null,false,'muni_name',null),
('cabd.stream_crossings_sites_structures_view','stream_name','stream name',null,false,'varchar',null,'13',false,null,'stream name',null,true,'strm_name',null),
('cabd.stream_crossings_sites_structures_view','road_name','road name',null,false,'varchar',null,'14',false,null,'road name',null,true,'road_name',null),
('cabd.stream_crossings_sites_structures_view','road_type_code','road type code',null,false,'integer',null,'15',false,'cabd.road_type_codes;code;name;description','road type code',null,false,'roadtype_c',null),
('cabd.stream_crossings_sites_structures_view','road_type_name','road type name',null,false,'varchar',null,'16',false,'cabd.road_type_codes;;name;description','road type name',null,false,'roadtype',null),
('cabd.stream_crossings_sites_structures_view','location_description','location description',null,false,'varchar',null,'17',false,null,'location description',null,false,'loc_desc',null),
('cabd.stream_crossings_sites_structures_view','land_ownership_context','land ownership context',null,false,'varchar',null,'18',false,null,'land ownership context',null,false,'land_owner',null),
('cabd.stream_crossings_sites_structures_view','incomplete_assess_code','incomplete assess code',null,false,'integer',null,'19',false,'stream_crossings.incomplete_assessment_codes;code;name;description','incomplete assess code',null,false,'inasses_c',null),
('cabd.stream_crossings_sites_structures_view','incomplete_assess_name','incomplete assess name',null,false,'varchar',null,'20',false,'stream_crossings.incomplete_assessment_codes;;name;description','incomplete assess name',null,false,'inasses',null),
('cabd.stream_crossings_sites_structures_view','crossing_type_code','crossing type code',null,false,'integer',null,'21',false,'stream_crossings.crossing_type_codes;code;name;description','crossing type code',null,false,'ctype_c',null),
('cabd.stream_crossings_sites_structures_view','crossing_type_name','crossing type name',null,false,'varchar',null,'22',false,'stream_crossings.crossing_type_codes;;name;description','crossing type name',null,false,'ctype',null),
('cabd.stream_crossings_sites_structures_view','num_structures','num structures',null,false,'integer',null,'23',false,null,'num structures',null,false,'num_struct',null),
('cabd.stream_crossings_sites_structures_view','photo_id_inlet','photo id inlet',null,false,'varchar',null,'24',false,null,'photo id inlet',null,false,'ph_in',null),
('cabd.stream_crossings_sites_structures_view','photo_id_outlet','photo id outlet',null,false,'varchar',null,'25',false,null,'photo id outlet',null,false,'ph_out',null),
('cabd.stream_crossings_sites_structures_view','photo_id_upstream','photo id upstream',null,false,'varchar',null,'26',false,null,'photo id upstream',null,false,'ph_up',null),
('cabd.stream_crossings_sites_structures_view','photo_id_downstream','photo id downstream',null,false,'varchar',null,'27',false,null,'photo id downstream',null,false,'ph_down',null),
('cabd.stream_crossings_sites_structures_view','photo_id_road_surface','photo id road surface',null,false,'varchar',null,'28',false,null,'photo id road surface',null,false,'ph_road',null),
('cabd.stream_crossings_sites_structures_view','photo_id_other_a','photo id other a',null,false,'varchar',null,'29',false,null,'photo id other a',null,false,'ph_othera',null),
('cabd.stream_crossings_sites_structures_view','photo_id_other_b','photo id other b',null,false,'varchar',null,'30',false,null,'photo id other b',null,false,'ph_otherb',null),
('cabd.stream_crossings_sites_structures_view','photo_id_other_c','photo id other c',null,false,'varchar',null,'31',false,null,'photo id other c',null,false,'ph_otherc',null),
('cabd.stream_crossings_sites_structures_view','flow_condition_code','flow condition code',null,false,'integer',null,'32',false,'stream_crossings.flow_condition_codes;code;name;description','flow condition code',null,false,'flowcon_c',null),
('cabd.stream_crossings_sites_structures_view','flow_condition_name','flow condition name',null,false,'varchar',null,'33',false,'stream_crossings.flow_condition_codes;;name;description','flow condition name',null,false,'flowcon',null),
('cabd.stream_crossings_sites_structures_view','crossing_condition_code','crossing condition code',null,false,'integer',null,'34',false,'stream_crossings.crossing_condition_codes;code;name;description','crossing condition code',null,false,'crosscon_c',null),
('cabd.stream_crossings_sites_structures_view','crossing_condition_name','crossing condition name',null,false,'varchar',null,'35',false,'stream_crossings.crossing_condition_codes;;name;description','crossing condition name',null,false,'crosson',null),
('cabd.stream_crossings_sites_structures_view','site_type_code','site type code',null,false,'integer',null,'36',false,'stream_crossings.site_type_codes;code;name;description','site type code',null,false,'sitetype_c',null),
('cabd.stream_crossings_sites_structures_view','site_type_name','site type name',null,false,'varchar',null,'37',false,'stream_crossings.site_type_codes;;name;description','site type name',null,false,'sitetype',null),
('cabd.stream_crossings_sites_structures_view','alignment_code','alignment code',null,false,'integer',null,'38',false,'stream_crossings.alignment_codes;code;name;description','alignment code',null,false,'align_c',null),
('cabd.stream_crossings_sites_structures_view','alignment_name','alignment name',null,false,'varchar',null,'39',false,'stream_crossings.alignment_codes;;name;description','alignment name',null,false,'align',null),
('cabd.stream_crossings_sites_structures_view','road_fill_height_m','road fill height m',null,false,'numeric',null,'40',false,null,'road fill height m',null,false,'roadfillht',null),
('cabd.stream_crossings_sites_structures_view','bankfull_width_upstr_a_m','bankfull width upstr a m',null,false,'numeric',null,'41',false,null,'bankfull width upstr a m',null,false,'bf_w_up_a',null),
('cabd.stream_crossings_sites_structures_view','bankfull_width_upstr_b_m','bankfull width upstr b m',null,false,'numeric',null,'42',false,null,'bankfull width upstr b m',null,false,'bfl_w_up_b',null),
('cabd.stream_crossings_sites_structures_view','bankfull_width_upstr_c_m','bankfull width upstr c m',null,false,'numeric',null,'43',false,null,'bankfull width upstr c m',null,false,'bf_w_up_c',null),
('cabd.stream_crossings_sites_structures_view','bankfull_width_upstr_avg_m','bankfull width upstr avg m',null,false,'numeric',null,'44',false,null,'bankfull width upstr avg m',null,false,'bf_w_up_av',null),
('cabd.stream_crossings_sites_structures_view','bankfull_width_dnstr_a_m','bankfull width dnstr a m',null,false,'numeric',null,'45',false,null,'bankfull width dnstr a m',null,false,'bf_w_dn_a',null),
('cabd.stream_crossings_sites_structures_view','bankfull_width_dnstr_b_m','bankfull width dnstr b m',null,false,'numeric',null,'46',false,null,'bankfull width dnstr b m',null,false,'bf_w_dn_b',null),
('cabd.stream_crossings_sites_structures_view','bankfull_width_dnstr_c_m','bankfull width dnstr c m',null,false,'numeric',null,'47',false,null,'bankfull width dnstr c m',null,false,'bf_w_dn_c',null),
('cabd.stream_crossings_sites_structures_view','bankfull_width_dnstr_avg_m','bankfull width dnstr avg m',null,false,'numeric',null,'48',false,null,'bankfull width dnstr avg m',null,false,'bf_w_dn_av',null),
('cabd.stream_crossings_sites_structures_view','bankfull_confidence_code','bankfull confidence code',null,false,'integer',null,'49',false,'stream_crossings.confidence_codes;code;name;description','bankfull confidence code',null,false,'bf_cc_c',null),
('cabd.stream_crossings_sites_structures_view','bankfull_confidence_name','bankfull confidence name',null,false,'varchar',null,'50',false,'stream_crossings.confidence_codes;;name;description','bankfull confidence name',null,false,'bf_cc',null),
('cabd.stream_crossings_sites_structures_view','scour_pool_tailwater_code','scour pool tailwater code',null,false,'integer',null,'51',false,'stream_crossings.scour_pool_codes;code;name;description','scour pool tailwater code',null,false,'scour_co',null),
('cabd.stream_crossings_sites_structures_view','scour_pool_tailwater_name','scour pool tailwater name',null,false,'varchar',null,'52',false,'stream_crossings.scour_pool_codes;;name;description','scour pool tailwater name',null,false,'scour',null),
('cabd.stream_crossings_sites_structures_view','crossing_comments','crossing comments',null,false,'varchar',null,'53',false,null,'crossing comments',null,false,'cross_com',null),
('cabd.stream_crossings_sites_structures_view','province_territory_code','province territory code',null,false,'varchar',null,'54',false,'cabd.province_territory_codes;code;name;','province territory code',null,false,'pr_terr_c',null),
('cabd.stream_crossings_sites_structures_view','province_territory','province territory',null,false,'varchar',null,'55',false,'cabd.province_territory_codes;;name;',null,null,false,'pr_terr',null),
('cabd.stream_crossings_sites_structures_view','nhn_watershed_id','nhn watershed id',null,false,'varchar',null,'56',false,'cabd.nhn_workunit;id;name;;polygon','nhn watershed id',null,false,'nhnws_id',null),
('cabd.stream_crossings_sites_structures_view','nhn_watershed_name','nhn watershed name',null,false,'varchar',null,'57',false,null,null,null,false,'nhnws',null),
('cabd.stream_crossings_sites_structures_view','strahler_order','strahler order',null,false,'integer',null,'58',false,null,'strahler order',null,false,'strahler_o',null),
('cabd.stream_crossings_sites_structures_view','assessment_type_code','assessment type code',null,false,'integer',null,'59',false,null,'assessment type code',null,false,'asses_c',null),
('cabd.stream_crossings_sites_structures_view','addressed_status_code','addressed status code',null,false,'integer',null,'60',false,null,'addressed status code',null,false,'asses',null),
('cabd.stream_crossings_sites_structures_view','chu_12_id','chu 12 id',null,false,'varchar',null,'61',false,null,'chu 12 id',null,false,'chu_12_id',null),
('cabd.stream_crossings_sites_structures_view','chu_10_id','chu 10 id',null,false,'varchar',null,'62',false,null,'chu 10 id',null,false,'chu_10_id',null),
('cabd.stream_crossings_sites_structures_view','chu_8_id','chu 8 id',null,false,'varchar',null,'63',false,null,'chu 8 id',null,false,'chu_8_id',null),
('cabd.stream_crossings_sites_structures_view','chu_6_id','chu 6 id',null,false,'varchar',null,'64',false,null,'chu 6 id',null,false,'chu_6_id',null),
('cabd.stream_crossings_sites_structures_view','chu_4_id','chu 4 id',null,false,'varchar',null,'65',false,null,'chu 4 id',null,false,'chu_4_id',null),
('cabd.stream_crossings_sites_structures_view','chu_2_id','chu 2 id',null,false,'varchar',null,'66',false,null,'chu 2 id',null,false,'chu_2_id',null),
('cabd.stream_crossings_sites_structures_view','include_in_act','include in act',null,false,'boolean',null,'67',false,null,'include in act',null,false,'in_cat',null),
('cabd.stream_crossings_sites_structures_view','structure_id','structure id',null,false,'uuid',null,'68',false,null,'structure id',null,false,'struct_id',null),
('cabd.stream_crossings_sites_structures_view','structure_last_modified','structure last modified',null,false,'timestamp without time zone',null,'69',false,null,'structure last modified',null,false,'st_last_md',null),
('cabd.stream_crossings_sites_structures_view','primary_structure','primary structure',null,false,'boolean',null,'70',false,null,'primary structure',null,false,'st_primary',null),
('cabd.stream_crossings_sites_structures_view','structure_number','structure number',null,false,'integer',null,'71',false,null,'structure number',null,false,'st_num',null),
('cabd.stream_crossings_sites_structures_view','outlet_shape_code','outlet shape code',null,false,'integer',null,'72',false,'stream_crossings.shape_codes;code;name;description','outlet shape code',null,false,'st_outsp_c',null),
('cabd.stream_crossings_sites_structures_view','outlet_shape','outlet shape',null,false,'varchar',null,'73',false,'stream_crossings.shape_codes;;name;description','outlet shape',null,false,'st_outsp',null),
('cabd.stream_crossings_sites_structures_view','internal_structures_code','internal structures code',null,false,'integer',null,'74',false,'stream_crossings.internal_structure_codes;code;name;description','internal structures code',null,false,'st_instr_c',null),
('cabd.stream_crossings_sites_structures_view','internal_structures','internal structures',null,false,'varchar',null,'75',false,'stream_crossings.internal_structure_codes;;name;description','internal structures',null,false,'st_instr',null),
('cabd.stream_crossings_sites_structures_view','liner_material_code','liner material code',null,false,'integer',null,'76',false,'stream_crossings.material_codes;code;name;description','liner material code',null,false,'st_lmat_c',null),
('cabd.stream_crossings_sites_structures_view','liner_material','liner material',null,false,'varchar',null,'77',false,'stream_crossings.material_codes;;name;description','liner material',null,false,'st_lmat',null),
('cabd.stream_crossings_sites_structures_view','outlet_armouring_code','outlet armouring code',null,false,'integer',null,'78',false,'stream_crossings.armouring_codes;code;name;description','outlet armouring code',null,false,'st_oarm_c',null),
('cabd.stream_crossings_sites_structures_view','outlet_armouring','outlet armouring',null,false,'varchar',null,'79',false,'stream_crossings.armouring_codes;;name;description','outlet armouring',null,false,'st_oarm',null),
('cabd.stream_crossings_sites_structures_view','outlet_grade_code','outlet grade code',null,false,'integer',null,'80',false,'stream_crossings.grade_codes;code;name;description','outlet grade code',null,false,'st_outgd_c',null),
('cabd.stream_crossings_sites_structures_view','outlet_grade','outlet grade',null,false,'varchar',null,'81',false,'stream_crossings.grade_codes;;name;description','outlet grade',null,false,'st_outgd',null),
('cabd.stream_crossings_sites_structures_view','outlet_width_m','outlet width m',null,false,'numeric',null,'82',false,null,'outlet width m',null,false,'st_outw',null),
('cabd.stream_crossings_sites_structures_view','outlet_height_m','outlet height m',null,false,'numeric',null,'83',false,null,'outlet height m',null,false,'st_outh',null),
('cabd.stream_crossings_sites_structures_view','outlet_substrate_water_width_m','outlet substrate water width m',null,false,'numeric',null,'84',false,null,'outlet substrate water width m',null,false,'st_oustsbw',null),
('cabd.stream_crossings_sites_structures_view','outlet_water_depth_m','outlet water depth m',null,false,'numeric',null,'85',false,null,'outlet water depth m',null,false,'st_outwd',null),
('cabd.stream_crossings_sites_structures_view','abutment_height_m','abutment height m',null,false,'numeric',null,'86',false,null,'abutment height m',null,false,'st_abuth',null),
('cabd.stream_crossings_sites_structures_view','outlet_drop_to_water_surface_m','outlet drop to water surface m',null,false,'numeric',null,'87',false,null,'outlet drop to water surface m',null,false,'st_outdps',null),
('cabd.stream_crossings_sites_structures_view','outlet_drop_to_stream_bottom_m','outlet drop to stream bottom m',null,false,'numeric',null,'88',false,null,'outlet drop to stream bottom m',null,false,'st_outdpb',null),
('cabd.stream_crossings_sites_structures_view','outlet_water_surface_to_residual_pool_top_m','outlet water surface to residual pool top m',null,false,'numeric',null,'89',false,null,'outlet water surface to residual pool top m',null,false,'st_outwsr',null),
('cabd.stream_crossings_sites_structures_view','residual_pool_confidence_code','residual pool confidence code',null,false,'integer',null,'90',false,'stream_crossings.confidence_codes;code;name;description','residual pool confidence code',null,false,'st_rpcc_c',null),
('cabd.stream_crossings_sites_structures_view','residual_pool_confidence','residual pool confidence',null,false,'varchar',null,'91',false,'stream_crossings.confidence_codes;;name;description','residual pool confidence',null,false,'st_rpcc',null),
('cabd.stream_crossings_sites_structures_view','structure_length_m','structure length m',null,false,'numeric',null,'92',false,null,'structure length m',null,false,'st_len',null),
('cabd.stream_crossings_sites_structures_view','inlet_shape_code','inlet shape code',null,false,'integer',null,'93',false,'stream_crossings.shape_codes;code;name;description','inlet shape code',null,false,'st_inshp_c',null),
('cabd.stream_crossings_sites_structures_view','inlet_shape','inlet shape',null,false,'varchar',null,'94',false,'stream_crossings.shape_codes;;name;description','inlet shape',null,false,'st_inshp',null),
('cabd.stream_crossings_sites_structures_view','inlet_type_code','inlet type code',null,false,'integer',null,'95',false,'stream_crossings.inlet_type_codes;code;name;description','inlet type code',null,false,'st_intyp_c',null),
('cabd.stream_crossings_sites_structures_view','inlet_type','inlet type',null,false,'varchar',null,'96',false,'stream_crossings.inlet_type_codes;;name;description','inlet type',null,false,'st_intyp',null),
('cabd.stream_crossings_sites_structures_view','inlet_grade_code','inlet grade code',null,false,'integer',null,'97',false,'stream_crossings.grade_codes;code;name;description','inlet grade code',null,false,'st_ingrd_c',null),
('cabd.stream_crossings_sites_structures_view','inlet_grade','inlet grade',null,false,'varchar',null,'98',false,'stream_crossings.grade_codes;;name;description','inlet grade',null,false,'st_ingrd',null),
('cabd.stream_crossings_sites_structures_view','inlet_width_m','inlet width m',null,false,'numeric',null,'99',false,null,'inlet width m',null,false,'st_inw',null),
('cabd.stream_crossings_sites_structures_view','inlet_height_m','inlet height m',null,false,'numeric',null,'100',false,null,'inlet height m',null,false,'st_inh',null),
('cabd.stream_crossings_sites_structures_view','inlet_substrate_water_width_m','inlet substrate water width m',null,false,'numeric',null,'101',false,null,'inlet substrate water width m',null,false,'st_in_sww',null),
('cabd.stream_crossings_sites_structures_view','inlet_water_depth_m','inlet water depth m',null,false,'numeric',null,'102',false,null,'inlet water depth m',null,false,'st_in_Wd',null),
('cabd.stream_crossings_sites_structures_view','structure_slope_pct','structure slope pct',null,false,'numeric',null,'103',false,null,'structure slope pct',null,false,'st_slp',null),
('cabd.stream_crossings_sites_structures_view','structure_slope_method_code','structure slope method code',null,false,'integer',null,'104',false,'stream_crossings.slope_method_codes;code;name;description','structure slope method code',null,false,'st_ssm_c',null),
('cabd.stream_crossings_sites_structures_view','slope_method','slope method',null,false,'varchar',null,'105',false,'stream_crossings.slope_method_codes;;name;description','slope method',null,false,'st_ssm',null),
('cabd.stream_crossings_sites_structures_view','structure_slope_to_channel_code','structure slope to channel code',null,false,'integer',null,'106',false,'stream_crossings.relative_slope_codes;code;name;description','structure slope to channel code',null,false,'st_ssc_c',null),
('cabd.stream_crossings_sites_structures_view','structure_slope_to_channel','structure slope to channel',null,false,'varchar',null,'107',false,'stream_crossings.relative_slope_codes;;name;description','structure slope to channel',null,false,'st_ssc',null),
('cabd.stream_crossings_sites_structures_view','substrate_type_code','substrate type code',null,false,'integer',null,'108',false,'stream_crossings.substrate_type_codes;code;name;description','substrate type code',null,false,'st_ss_tp_c',null),
('cabd.stream_crossings_sites_structures_view','substrate_type','substrate type',null,false,'varchar',null,'109',false,'stream_crossings.substrate_type_codes;;name;description','substrate type',null,false,'st_ss_tp',null),
('cabd.stream_crossings_sites_structures_view','substrate_matches_stream_code','substrate matches stream code',null,false,'integer',null,'110',false,'stream_crossings.substrate_matches_stream_codes;code;name;description','substrate matches stream code',null,false,'st_ss_ms_c',null),
('cabd.stream_crossings_sites_structures_view','substrate_matches','substrate matches',null,false,'varchar',null,'111',false,'stream_crossings.substrate_matches_stream_codes;;name;description','substrate matches',null,false,'st_ss_ms',null),
('cabd.stream_crossings_sites_structures_view','substrate_coverage_code','substrate coverage code',null,false,'integer',null,'112',false,'stream_crossings.structure_coverage_codes;code;name;description','substrate coverage code',null,false,'st_ss_c_c',null),
('cabd.stream_crossings_sites_structures_view','substrate_coverage','substrate coverage',null,false,'varchar',null,'113',false,'stream_crossings.structure_coverage_codes;;name;description','substrate coverage',null,false,'st_ss_c_c',null),
('cabd.stream_crossings_sites_structures_view','substrate_depth_consistent_code','substrate depth consistent code',null,false,'integer',null,'114',false,'cabd.response_codes;code;name;description','substrate depth consistent code',null,false,'st_dptc_c',null),
('cabd.stream_crossings_sites_structures_view','substrate_depth_consistent','substrate depth consistent',null,false,'varchar',null,'115',false,'cabd.response_codes;;name;description','substrate depth consistent',null,false,'st_dptc',null),
('cabd.stream_crossings_sites_structures_view','backwatered_pct_code','backwatered pct code',null,false,'integer',null,'116',false,'stream_crossings.structure_coverage_codes;code;name;description','backwatered pct code',null,false,'st_bkw_c',null),
('cabd.stream_crossings_sites_structures_view','backwatered_pct','backwatered pct',null,false,'varchar',null,'117',false,'stream_crossings.structure_coverage_codes;;name;description','backwatered pct',null,false,'st_bkw',null),
('cabd.stream_crossings_sites_structures_view','physical_blockage_severity_code','physical blockage severity code',null,false,'integer',null,'118',false,'stream_crossings.blockage_severity_codes;code;name;description','physical blockage severity code',null,false,'st_blksv_c',null),
('cabd.stream_crossings_sites_structures_view','blockage_severity','blockage severity',null,false,'varchar',null,'119',false,'stream_crossings.blockage_severity_codes;;name;description','blockage severity',null,false,'st_blksv',null),
('cabd.stream_crossings_sites_structures_view','water_depth_matches_stream_code','water depth matches stream code',null,false,'integer',null,'120',false,'stream_crossings.water_depth_matches_stream_codes;code;name;description','water depth matches stream code',null,false,'st_wd_c',null),
('cabd.stream_crossings_sites_structures_view','water_depth_matches','water depth matches',null,false,'varchar',null,'121',false,'stream_crossings.water_depth_matches_stream_codes;;name;description','water depth matches',null,false,'st_wd',null),
('cabd.stream_crossings_sites_structures_view','water_velocity_matches_stream_code','water velocity matches stream code',null,false,'integer',null,'122',false,'stream_crossings.water_velocity_matches_stream_codes;code;name;description','water velocity matches stream code',null,false,'st_wv_c',null),
('cabd.stream_crossings_sites_structures_view','water_velocity_matches','water velocity matches',null,false,'varchar',null,'123',false,'stream_crossings.water_velocity_matches_stream_codes;;name;description','water velocity matches',null,false,'st_wv',null),
('cabd.stream_crossings_sites_structures_view','dry_passage_code','dry passage code',null,false,'integer',null,'124',false,'cabd.response_codes;code;name;description','dry passage code',null,false,'st_dryps_c',null),
('cabd.stream_crossings_sites_structures_view','dry_passage','dry passage',null,false,'varchar',null,'125',false,'cabd.response_codes;;name;description','dry passage',null,false,'st_dryps',null),
('cabd.stream_crossings_sites_structures_view','height_above_dry_passage_m','height above dry passage m',null,false,'integer',null,'126',false,null,'height above dry passage m',null,false,'st_habove',null),
('cabd.stream_crossings_sites_structures_view','structure_comments','structure comments',null,false,'varchar',null,'127',false,null,'structure comments',null,false,'st_comment',null),
('cabd.stream_crossings_sites_structures_view','passability_status_code','passability status code',null,false,'integer',3,'128',true,'cabd.passability_status_codes;code;name;description','passability status code',null,false,'passstat_c',3),
('cabd.stream_crossings_sites_structures_view','passability_status','passability status',null,false,'varchar',2,'129',true,'cabd.passability_status_codes;;name;description','passability status',null,false,'pass_stat',2),
('cabd.stream_crossings_sites_structures_view','structures_url','structures url',null,true,'url',null,'130',true,null,'structures url',null,false,'st_url',null),
('cabd.stream_crossings_sites_structures_view','geometry','geometry',null,false,'geometry',4,null,true,null,'geometry',null,false,'geom',4);


update cabd.feature_types set 
	data_view = 'cabd.stream_crossings_sites_structures_view',
	feature_source_table = null, --we might consider setting this to sites, but this might cause issues as I think it is used for data_sources and sites doesn't have a datasource_id field
	data_version = '1.0',
	data_table = array['stream_crossings.sites'], --data table is used for feature version reporting; structures don't have cabd_id so we don't want to include them here
	community_data_table = 'stream_crossings.stream_crossings_community_staging',
	community_data_photo_fields = array['transportation_route_image','structure_outlet_image','structure_inlet_image','upstream_blockage_image','downstream_blockage_image','site_image','downstream_direction_image','downstream_side_image','upstream_direction_image','upstream_side_image','fishway_image']
where type = 'stream_crossings'


alter table cabd.feature_types add column is_assessment boolean default false;
update cabd.feature_types set is_assessment = true where type in ('sites', 'stream_crossings');



-- NOTE: I have decided the main feature type for sites is stream_crossings. I have left the sites
-- feature type in place for now but unless the user specifically asks for that feature type
-- it won't be used.


-- UPDATE all features view to use 'stream_crossings' and feature type
-- for sites

CREATE OR REPLACE VIEW cabd.all_features_view_en
AS SELECT barriers.cabd_id,
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
         SELECT c.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.province_territory_code::character varying(2) AS province_territory_code,
            c.nhn_watershed_id::character varying(7) AS nhn_watershed_id,
            c.municipality::character varying(512) AS municipality,
            c.stream_name::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
                CASE
                    WHEN ts.passability_status_code IS NULL THEN ( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text)
                    ELSE ts.passability_status_code::smallint
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            case when c.snapped_point is not null then c.snapped_point else c.original_point end
           FROM stream_crossings.sites c
             LEFT JOIN stream_crossings.structures ts ON ts.site_id = c.cabd_id AND ts.primary_structure IS TRUE) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;


     
CREATE OR REPLACE VIEW cabd.all_features_view_fr
AS SELECT barriers.cabd_id,
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
         SELECT c.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.province_territory_code::character varying(2) AS province_territory_code,
            c.nhn_watershed_id::character varying(7) AS nhn_watershed_id,
            c.municipality::character varying(512) AS municipality,
            c.stream_name::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
                CASE
                    WHEN ts.passability_status_code IS NULL THEN ( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text)
                    ELSE ts.passability_status_code::smallint
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            case when c.snapped_point is not null then c.snapped_point else c.original_point end
           FROM stream_crossings.sites c
             LEFT JOIN stream_crossings.structures ts ON ts.site_id = c.cabd_id AND ts.primary_structure IS TRUE) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;



-- barriers view also needs updating
-- NOTE: I removed modelled crossings from this - not sure if we want it or not, but
-- if we do we want it in the all_features_views as well.
CREATE OR REPLACE VIEW cabd.barriers_view_en
AS SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.feature_type,
    barriers.name_en::character varying(512) AS name_en,
    barriers.name_fr::character varying(512) AS name_fr,
    barriers.province_territory_code,
    pt.name_en AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    barriers.municipality::character varying(512) AS municipality,
    barriers.waterbody_name_en::character varying(512) AS waterbody_name_en,
    barriers.waterbody_name_fr::character varying(512) AS waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code::smallint AS passability_status_code,
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
         SELECT c.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.province_territory_code::character varying(2) AS province_territory_code,
            c.nhn_watershed_id::character varying(7) AS nhn_watershed_id,
            c.municipality::character varying(512) AS municipality,
            c.stream_name::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
                CASE
                    WHEN ts.passability_status_code IS NULL THEN ( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text)
                    ELSE ts.passability_status_code::smallint
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            case when c.snapped_point is not null then c.snapped_point else c.original_point end
           FROM stream_crossings.sites c
             LEFT JOIN stream_crossings.structures ts ON ts.site_id = c.cabd_id AND ts.primary_structure IS TRUE) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;


CREATE OR REPLACE VIEW cabd.barriers_view_fr
AS SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.feature_type,
    barriers.name_en::character varying(512) AS name_en,
    barriers.name_fr::character varying(512) AS name_fr,
    barriers.province_territory_code,
    pt.name_fr AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    barriers.municipality::character varying(512) AS municipality,
    barriers.waterbody_name_en::character varying(512) AS waterbody_name_en,
    barriers.waterbody_name_fr::character varying(512) AS waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code::smallint AS passability_status_code,
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
         SELECT c.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.province_territory_code::character varying(2) AS province_territory_code,
            c.nhn_watershed_id::character varying(7) AS nhn_watershed_id,
            c.municipality::character varying(512) AS municipality,
            c.stream_name::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
                CASE
                    WHEN ts.passability_status_code IS NULL THEN ( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text)
                    ELSE ts.passability_status_code::smallint
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            case when c.snapped_point is not null then c.snapped_point else c.original_point end
           FROM stream_crossings.sites c
             LEFT JOIN stream_crossings.structures ts ON ts.site_id = c.cabd_id AND ts.primary_structure IS TRUE) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;











     -- cabd.sites_view_en source

CREATE OR REPLACE VIEW cabd.sites_view_en
AS SELECT s.cabd_id,
    'sites'::text AS feature_type,
    'features/datasources/'::text || s.cabd_id AS datasource_url,
    'features/structures?filter=site_id:eq:'::text || s.cabd_id AS structures_url,
    st_y(s.snapped_point) AS latitude,
    st_x(s.snapped_point) AS longitude,
    s.last_modified,
    s.other_id,
    s.cabd_assessment_id,
    s.original_assessment_id,
    s.date_assessed,
    s.lead_assessor,
    s.municipality,
    s.stream_name,
    s.road_name,
    s.road_type_code,
    rt.name_en AS road_type_name,
    s.location_description,
    s.land_ownership_context,
    s.incomplete_assess_code,
    ia.name_en AS incomplete_assess_name,
    s.crossing_type_code,
    ct.name_en AS crossing_type_name,
    s.num_structures,
    s.photo_id_inlet,
    s.photo_id_outlet,
    s.photo_id_upstream,
    s.photo_id_downstream,
    s.photo_id_road_surface,
    s.photo_id_other_a,
    s.photo_id_other_b,
    s.photo_id_other_c,
    s.flow_condition_code,
    fc.name_en AS flow_condition_name,
    s.crossing_condition_code,
    con.name_en AS crossing_condition_name,
    s.site_type_code,
    st.name_en AS site_type_name,
    s.alignment_code,
    ac.name_en AS alignment_name,
    s.road_fill_height_m,
    s.bankfull_width_upstr_a_m,
    s.bankfull_width_upstr_b_m,
    s.bankfull_width_upstr_c_m,
    s.bankfull_width_upstr_avg_m,
    s.bankfull_width_dnstr_a_m,
    s.bankfull_width_dnstr_b_m,
    s.bankfull_width_dnstr_c_m,
    s.bankfull_width_dnstr_avg_m,
    s.bankfull_confidence_code,
    cc.name_en AS bankfull_confidence_name,
    s.scour_pool_tailwater_code,
    sc.name_en AS scour_pool_name,
    s.crossing_comments,
    s.province_territory_code,
    s.nhn_watershed_id,
    s.strahler_order,
    s.assessment_type_code,
    s.addressed_status_code,
    s.chu_12_id,
    s.chu_10_id,
    s.chu_8_id,
    s.chu_6_id,
    s.chu_4_id,
    s.chu_2_id,
    s.include_in_act,
    case when s.snapped_point is not null then s.snapped_point else s.original_point end AS geometry
   FROM stream_crossings.sites s
     LEFT JOIN stream_crossings.alignment_codes ac ON s.alignment_code = ac.code
     LEFT JOIN stream_crossings.confidence_codes cc ON s.bankfull_confidence_code = cc.code
     LEFT JOIN stream_crossings.crossing_condition_codes con ON s.crossing_condition_code = con.code
     LEFT JOIN stream_crossings.crossing_type_codes ct ON s.crossing_type_code = ct.code
     LEFT JOIN stream_crossings.flow_condition_codes fc ON s.flow_condition_code = fc.code
     LEFT JOIN stream_crossings.incomplete_assessment_codes ia ON s.incomplete_assess_code = ia.code
     LEFT JOIN cabd.road_type_codes rt ON s.road_type_code = rt.code
     LEFT JOIN stream_crossings.scour_pool_codes sc ON s.scour_pool_tailwater_code = sc.code
     LEFT JOIN stream_crossings.site_type_codes st ON s.site_type_code = st.code;



     -- cabd.sites_view_fr source

CREATE OR REPLACE VIEW cabd.sites_view_fr
AS SELECT s.cabd_id,
    'sites'::text AS feature_type,
    'features/datasources/'::text || s.cabd_id AS datasource_url,
    'features/structures?filter=site_id:eq:'::text || s.cabd_id AS structures_url,
    st_y(s.snapped_point) AS latitude,
    st_x(s.snapped_point) AS longitude,
    s.last_modified,
    s.other_id,
    s.cabd_assessment_id,
    s.original_assessment_id,
    s.date_assessed,
    s.lead_assessor,
    s.municipality,
    s.stream_name,
    s.road_name,
    s.road_type_code,
    rt.name_fr AS road_type_name,
    s.location_description,
    s.land_ownership_context,
    s.incomplete_assess_code,
    ia.name_fr AS incomplete_assess_name,
    s.crossing_type_code,
    ct.name_fr AS crossing_type_name,
    s.num_structures,
    s.photo_id_inlet,
    s.photo_id_outlet,
    s.photo_id_upstream,
    s.photo_id_downstream,
    s.photo_id_road_surface,
    s.photo_id_other_a,
    s.photo_id_other_b,
    s.photo_id_other_c,
    s.flow_condition_code,
    fc.name_fr AS flow_condition_name,
    s.crossing_condition_code,
    con.name_fr AS crossing_condition_name,
    s.site_type_code,
    st.name_fr AS site_type_name,
    s.alignment_code,
    ac.name_fr AS alignment_name,
    s.road_fill_height_m,
    s.bankfull_width_upstr_a_m,
    s.bankfull_width_upstr_b_m,
    s.bankfull_width_upstr_c_m,
    s.bankfull_width_upstr_avg_m,
    s.bankfull_width_dnstr_a_m,
    s.bankfull_width_dnstr_b_m,
    s.bankfull_width_dnstr_c_m,
    s.bankfull_width_dnstr_avg_m,
    s.bankfull_confidence_code,
    cc.name_fr AS bankfull_confidence_name,
    s.scour_pool_tailwater_code,
    sc.name_fr AS scour_pool_name,
    s.crossing_comments,
    s.province_territory_code,
    s.nhn_watershed_id,
    s.strahler_order,
    s.assessment_type_code,
    s.addressed_status_code,
    s.chu_12_id,
    s.chu_10_id,
    s.chu_8_id,
    s.chu_6_id,
    s.chu_4_id,
    s.chu_2_id,
    s.include_in_act,
    case when s.snapped_point is not null then s.snapped_point else s.original_point end AS geometry
   FROM stream_crossings.sites s
     LEFT JOIN stream_crossings.alignment_codes ac ON s.alignment_code = ac.code
     LEFT JOIN stream_crossings.confidence_codes cc ON s.bankfull_confidence_code = cc.code
     LEFT JOIN stream_crossings.crossing_condition_codes con ON s.crossing_condition_code = con.code
     LEFT JOIN stream_crossings.crossing_type_codes ct ON s.crossing_type_code = ct.code
     LEFT JOIN stream_crossings.flow_condition_codes fc ON s.flow_condition_code = fc.code
     LEFT JOIN stream_crossings.incomplete_assessment_codes ia ON s.incomplete_assess_code = ia.code
     LEFT JOIN cabd.road_type_codes rt ON s.road_type_code = rt.code
     LEFT JOIN stream_crossings.scour_pool_codes sc ON s.scour_pool_tailwater_code = sc.code
     LEFT JOIN stream_crossings.site_type_codes st ON s.site_type_code = st.code;


     -- cabd.structures_view_en source

CREATE OR REPLACE VIEW cabd.structures_view_en
AS SELECT s.structure_id AS cabd_id,
    'structures'::text AS feature_type,
    s.site_id,
    s.last_modified,
    s.cabd_assessment_id,
    s.original_assessment_id,
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
    sc.name_en AS slope_to_channel,
    s.substrate_type_code,
    st.name_en AS substrate_type,
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
    case when ss.snapped_point is null then ss.original_point else ss.snapped_point end AS geometry
   FROM stream_crossings.structures s
     JOIN stream_crossings.sites ss ON ss.cabd_id = s.site_id
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
     LEFT JOIN stream_crossings.relative_slope_codes sc ON s.structure_slope_to_channel_code = sc.code
     LEFT JOIN stream_crossings.substrate_type_codes st ON s.substrate_type_code = st.code
     LEFT JOIN stream_crossings.substrate_matches_stream_codes sms ON s.substrate_matches_stream_code = sms.code
     LEFT JOIN stream_crossings.structure_coverage_codes scov ON s.substrate_coverage_code = scov.code
     LEFT JOIN cabd.response_codes sdc ON s.substrate_depth_consistent_code = sdc.code
     LEFT JOIN stream_crossings.structure_coverage_codes bwp ON s.backwatered_pct_code = bwp.code
     LEFT JOIN stream_crossings.blockage_severity_codes pbs ON s.physical_blockage_severity_code = pbs.code
     LEFT JOIN stream_crossings.water_depth_matches_stream_codes wdms ON s.water_depth_matches_stream_code = wdms.code
     LEFT JOIN stream_crossings.water_velocity_matches_stream_codes wvms ON s.water_velocity_matches_stream_code = wvms.code
     LEFT JOIN cabd.response_codes dp ON s.dry_passage_code = dp.code;



-- cabd.structures_view_fr source

CREATE OR REPLACE VIEW cabd.structures_view_fr
AS SELECT s.structure_id AS cabd_id,
    'structures'::text AS feature_type,
    s.site_id,
    s.last_modified,
    s.cabd_assessment_id,
    s.original_assessment_id,
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
    sc.name_fr AS slope_to_channel,
    s.substrate_type_code,
    st.name_fr AS substrate_type,
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
    case when ss.snapped_point is null then ss.original_point else ss.snapped_point end AS geometry
   FROM stream_crossings.structures s
     JOIN stream_crossings.sites ss ON ss.cabd_id = s.site_id
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
     LEFT JOIN stream_crossings.relative_slope_codes sc ON s.structure_slope_to_channel_code = sc.code
     LEFT JOIN stream_crossings.substrate_type_codes st ON s.substrate_type_code = st.code
     LEFT JOIN stream_crossings.substrate_matches_stream_codes sms ON s.substrate_matches_stream_code = sms.code
     LEFT JOIN stream_crossings.structure_coverage_codes scov ON s.substrate_coverage_code = scov.code
     LEFT JOIN cabd.response_codes sdc ON s.substrate_depth_consistent_code = sdc.code
     LEFT JOIN stream_crossings.structure_coverage_codes bwp ON s.backwatered_pct_code = bwp.code
     LEFT JOIN stream_crossings.blockage_severity_codes pbs ON s.physical_blockage_severity_code = pbs.code
     LEFT JOIN stream_crossings.water_depth_matches_stream_codes wdms ON s.water_depth_matches_stream_code = wdms.code
     LEFT JOIN stream_crossings.water_velocity_matches_stream_codes wvms ON s.water_velocity_matches_stream_code = wvms.code
     LEFT JOIN cabd.response_codes dp ON s.dry_passage_code = dp.code;     