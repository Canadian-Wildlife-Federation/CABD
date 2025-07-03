drop table if exists cabd.assessment_types;
drop table if exists cabd.assessment_type_metadata;
create table cabd.assessment_types(
    type varchar(32),
    data_view varchar,
    name_en varchar,
    name_fr varchar,
    constraint assessment_types_pkey primary key (type)
);

create table cabd.assessment_type_metadata(
    type varchar,
    field_name varchar,
    
    name_en varchar,
    name_fr varchar,

    description_en varchar,
    description_fr varchar,

    data_type varchar,
    constraint assessment_type_metdata_pkey primary key (type, field_name)
);

insert into cabd.assessment_types (type, data_view, name_en, name_fr) values
('rapid', 'stream_crossings.assessment_rapid', 'Rapid Assessment', 'Rapid Assessment'),
('long-form', 'stream_crossings.assessment_longform', 'Long Form Assessment', 'Long Form Assessment');


insert into cabd.assessment_type_metadata (type, field_name, data_type, name_en, name_fr) values
('rapid', 'assessment_id','uuid', 'Assessment ID', 'Assessment ID'),
('rapid', 'cabd_id','uuid', 'CABD Feature Id', 'CABD Feature Id'),
('rapid', 'assessment_date','timestamp', 'DateTime', 'DateTime'),
('rapid', 'feature_type_code','integer', 'Feature Type Code', 'Feature Type Code'),
('rapid', 'feature_type','varchar', 'Feature Type', 'Feature Type'),
('rapid', 'to_feature_type_code','integer', 'To Feature Type Code', 'To Feature Type Code'),
('rapid', 'to_feature_type','varchar', 'To Feature Type', 'To Feature Type'),
('rapid', 'latitude','numeric', 'Latitude', 'Latitude'),
('rapid', 'longitude','numeric', 'Longitude', 'Longitude'),
('rapid', 'site_accessible_code','integer', 'Site Accessible Code', 'Site Accessible Code'),
('rapid', 'site_accessible','varchar',  'Site Accessible',  'Site Accessible'),
('rapid', 'no_access_reason_code','integer', 'No Access Reason Code', 'No Access Reason Code'),
('rapid', 'no_access_reason','varchar', 'No Access Reason', 'No Access Reason'),
('rapid', 'crossing_type_code','integer', 'Crossing Type Code', 'Crossing Type Code'),
('rapid', 'crossing_type','integer', 'Crossing Type', 'Crossing Type'),
('rapid', 'road_type_code','integer', 'Road Type Code', 'Road Type Code'),
('rapid', 'road_type','varchar', 'Road Type', 'Road Type'),
('rapid', 'transportation_route_image','varchar', 'Transportation Route Image', 'Transportation Route Image'),
('rapid', 'obs_constriction_code','integer', 'Obs Constriction Code', 'Obs Constriction Code'),
('rapid', 'obs_constriction','varchar', 'Obs Constriction', 'Obs Constriction'),
('rapid', 'water_flowing_upstream_code','integer', 'Water Flowing Upstream Code', 'Water Flowing Upstream Code'),
('rapid', 'water_flowing_upstream','varchar', 'Water Flowing Upstream', 'Water Flowing Upstream'),
('rapid', 'structure_outlet_image','varchar', 'Structure Outlet Image', 'Structure Outlet Image'),
('rapid', 'structure_inlet_image','varchar', 'Structure Inlet Image', 'Structure Inlet Image'),
('rapid', 'upstream_physical_blockages_code','integer[]', 'Upstream Physical Blockage Codes', 'Upstream Physical Blockage Codes'),
('rapid', 'upstream_physical_blockages','text', 'Upstream Physical Blockages', 'Upstream Physical Blockages'),
('rapid', 'upstream_blockage_image','varchar', 'Upstream Blockage Image', 'Upstream Blockage Image'),
('rapid', 'upstream_blockage_height_code','integer', 'Upstream Blockage Height Code', 'Upstream Blockage Height Code'),
('rapid', 'upstream_blockage_height','varchar', 'Upstream Blockage Height', 'Upstream Blockage Height'),
('rapid', 'downstream_physical_blockages_code','integer[]', 'Downstream Physical Blockages Codes', 'Downstream Physical Blockages Codes'),
('rapid', 'downstream_physical_blockages','text', 'Downstream Physical Bloackages', 'Downstream Physical Bloackages'),
('rapid', 'downstream_blockage_height_code','integer', 'Dlownstream Blockage Height Code', 'Dlownstream Blockage Height Code'),
('rapid', 'downstream_blockage_height','varchar', 'Dlownstream Blockage Height', 'Dlownstream Blockage Height'),
('rapid', 'downstream_blockage_image','varchar', 'Downstream Blockage Image', 'Downstream Blockage Image'),
('rapid', 'water_flowing_under_code','integer', 'Water Flowing Under Code', 'Water Flowing Under Code'),
('rapid', 'water_flowing_under','varchar', 'Water Flowing Under', 'Water Flowing Under'),
('rapid', 'outlet_drop_code','integer', 'Outlet Drop Code', 'Outlet Drop Code'),
('rapid', 'outlet_drop','varchar', 'Outlet Drop', 'Outlet Drop'),
('rapid', 'multiple_closed_bottom_code','integer', 'Multiple Closed Bottom Code', 'Multiple Closed Bottom Code'),
('rapid', 'multiple_closed_bottom','varchar', 'Multiple Closed Bottom', 'Multiple Closed Bottom'),
('rapid', 'cbs_constriction_code','integer', 'Cbc Constriction Code', 'Cbc Constriction Code'),
('rapid', 'cbs_constriction','varchar', 'Cbc Constriction', 'Cbc Constriction'),
('rapid', 'structure_count','integer', 'Structure Count', 'Structure Count'),
('rapid', 'water_flowing_through_code','integer', 'Water Flowing Through Code', 'Water Flowing Through Code'),
('rapid', 'water_flowing_through','varchar', 'Water Flowing Through', 'Water Flowing Through'),
('rapid', 'ford_type_code','integer', 'Ford Type Code', 'Ford Type Code'),
('rapid', 'ford_type','varchar', 'Ford Type', 'Ford Type'),
('rapid', 'water_flowing_over_code','integer', 'Water Flowing Over Code', 'Water Flowing Over Code'),
('rapid', 'water_flowing_over','varchar', 'Water Flowing Over', 'Water Flowing Over'),
('rapid', 'site_image','varchar', 'Site Image', 'Site Image'),
('rapid', 'structure_signs_code','integer', 'Structure Signs Codes', 'Structure Signs Codes'),
('rapid', 'structure_signs','varchar', 'Structure Signs', 'Structure Signs'),
('rapid', 'stream_at_site_code','boolean', 'Stream At Site', 'Stream At Site'),
('rapid', 'water_existed_code','integer', 'Water Existed Code', 'Water Existed Code'),
('rapid', 'water_existed','varchar', 'Water Existed', 'Water Existed'),
('rapid', 'trail_end_code','integer', 'Trail End Code', 'Trail End Code'),
('rapid', 'trail_end','varchar', 'Trail End', 'Trail End'),
('rapid', 'access_method_code','integer', 'Access Method Code', 'Access Method Code'),
('rapid', 'access_method','varchar', 'Access Method', 'Access Method'),
('rapid', 'close_by_code','integer', 'Close By Code', 'Close By Code'),
('rapid', 'close_by','varchar', 'Close By', 'Close By'),
('rapid', 'dam_name','varchar', 'Dam Name', 'Dam Name'),
('rapid', 'partial_dam_removal_code','integer', 'Partial Dam Removal Code', 'Partial Dam Removal Code'),
('rapid', 'partial_dam_removal','varchar', 'Partial Dam Removal', 'Partial Dam Removal'),
('rapid', 'downstream_direction_image','varchar', 'Downstream Direction Image', 'Downstream Direction Image'),
('rapid', 'downstream_side_image','varchar', 'Downstream Side Image', 'Downstream Side Image'),
('rapid', 'water_passing_code','integer', 'Water Passing Code', 'Water Passing Code'),
('rapid', 'water_passing','varchar', 'Water Passing', 'Water Passing'),
('rapid', 'dam_size_code','integer', 'Dam Size Code', 'Dam Size Code'),
('rapid', 'dam_size','varchar', 'Dam Size', 'Dam Size'),
('rapid', 'has_fish_structure','boolean', 'Has Fish Structure', 'Has Fish Structure'),
('rapid', 'fishway_image','varchar', 'Fishway Image', 'Fishway Image'),
('rapid', 'upstream_direction_image','varchar', 'Upstream Direction Image', 'Upstream Direction Image'),
('rapid', 'upstream_side_image','varchar', 'Upstream Side Image', 'Upstream Side Image'),
('rapid', 'notes','varchar', 'Notes', 'Notes');



drop view if exists stream_crossings.assessment_rapid_en;

create view stream_crossings.assessment_rapid_en as
select 
a.id as assessment_id,
'rapid' as assessment_type,
a.cabd_id,
a.uploaded_datetime as assessment_date,
a.feature_type_code,
ftc.name_en as feature_type,
to_feature_type_code,
ftc2.name_en as to_feature_type,
a.latitude,
a.longitude,
a.site_accessible_code,
sa.name_en as site_accessible,
a.no_access_reason_code,
nar.name_en as no_access_reason,
a.crossing_type_code,
ct.name_en as crossing_type,
a.road_type_code,
rt.name_en as road_type,
a.transportation_route_image,
a.obs_constriction_code,
oc.name_en as obs_constriction,
a.water_flowing_upstream_code,
wfup.name_en as water_flowing_upstream,
a.structure_outlet_image,
a.structure_inlet_image,
a.upstream_physical_blockages_code,
uppb.name_en as upstream_physical_blockages,
a.upstream_blockage_image,
a.upstream_blockage_height_code,
ubh.name_en as upstream_blockage_height,
a.downstream_physical_blockages_code,
downph.name_en as downstream_physical_blockages,
a.downstream_blockage_height_code,
dbh.name_en as downstream_blockage_height,
a.downstream_blockage_image,
a.water_flowing_under_code,
wfu.name_en as water_flowing_under,
a.outlet_drop_code,
od.name_en as outlet_drop,
a.multiple_closed_bottom_code,
mcb.name_en as multiple_closed_bottom,
a.cbs_constriction_code,
cc.name_en as cbs_constriction,
a.structure_count,
a.water_flowing_through_code,
wft.name_en as water_flowing_through,
a.ford_type_code,
ft.name_en as ford_type,
a.water_flowing_over_code,
wfo.name_en as water_flowing_over,
a.site_image,
a.structure_signs_code,
ss.name_en as structure_signs,
a.stream_at_site_code,
a.water_existed_code,
we.name_en as water_existed,
a.trail_end_code,
te.name_en as trail_end,
a.access_method_code,
am.name_en as access_method,
a.close_by_code,
cb.name_en as close_by,
a.dam_name,
a.partial_dam_removal_code,
pdr.name_en as partial_dam_removal,
a.downstream_direction_image,
a.downstream_side_image,
a.water_passing_code,
wp.name_en as water_passing,
a.dam_size_code,
ds.name_en as dam_size,
a.has_fish_structure,
a.fishway_image,
a.upstream_direction_image,
a.upstream_side_image,
a.notes
from stream_crossings.stream_crossings_community_holding a
left join cabd.feature_type_codes ftc on ftc.code = a.feature_type_code
left join cabd.feature_type_codes ftc2 on ftc2.code = a.to_feature_type_code
left join cabd.response_codes sa on sa.code = a.site_accessible_code
left join cabd.no_access_reason_codes nar on nar.code = a.no_access_reason_code
left join stream_crossings.outlet_drop_codes dbh on dbh.code = a.downstream_blockage_height_code
left join stream_crossings.outlet_drop_codes ubh on ubh.code = a.upstream_blockage_height_code
left join cabd.response_codes mcb on mcb.code = a.multiple_closed_bottom_code
left join cabd.flowing_codes wfup on wfup.code = a.water_flowing_upstream_code
left join cabd.flowing_codes wft on wft.code = a.water_flowing_through_code
left join cabd.response_codes pdr on pdr.code = a.partial_dam_removal_code
left join cabd.flowing_codes wfu on wfu.code = a.water_flowing_under_code
left join cabd.access_method_codes am on am.code = a.access_method_code
left join stream_crossings.cbs_constriction_codes cc on cc.code = a.cbs_constriction_code
left join cabd.response_codes cb on cb.code = a.close_by_code
left join stream_crossings.crossing_type_codes ct on ct.code = a.crossing_type_code
left join dams.size_codes ds on ds.code = a.dam_size_code
left join stream_crossings.ford_type_codes ft on ft.code = a.ford_type_code
left join stream_crossings.obs_constriction_codes oc on oc.code = a.obs_constriction_code
left join stream_crossings.outlet_drop_codes od on od.code = a.outlet_drop_code
left join cabd.road_type_codes rt on rt.code = a.road_type_code
left join cabd.response_codes ss on ss.code = a.structure_signs_code
left join cabd.response_codes te on te.code = a.trail_end_code 
left join cabd.response_codes we on we.code = a.water_existed_code
left join cabd.response_codes wfo on wfo.code = a.water_flowing_over_code
left join dams.side_channel_bypass_codes wp on wp.code = a.water_passing_code

left join 
    (
        select id,
            array_agg(cc.name_en) AS name_en
        from stream_crossings.stream_crossings_community_holding 
	    join lateral unnest(upstream_physical_blockages_code) as code_ids on true
	    left join cabd.blockage_type_codes cc on cc.code = code_ids
	    group by id
    ) as uppb on uppb.id = a.id

left join 
    (
        select id,
            array_agg(cc.name_en) AS name_en
        from stream_crossings.stream_crossings_community_holding 
	    join lateral unnest(downstream_physical_blockages_code) as code_ids on true
	    left join cabd.blockage_type_codes cc on cc.code = code_ids
	    group by id
    ) as downph on downph.id = a.id
where a.status = 'PROCESSED';


drop view if exists stream_crossings.assessment_rapid_fr;

create view stream_crossings.assessment_rapid_fr as
select 
a.id as assessment_id,
'rapid' as assessment_type,
a.cabd_id,
a.uploaded_datetime as assessment_date,
a.feature_type_code,
ftc.name_fr as feature_type,
to_feature_type_code,
ftc2.name_fr as to_feature_type,
a.latitude,
a.longitude,
a.site_accessible_code,
sa.name_fr as site_accessible,
a.no_access_reason_code,
nar.name_fr as no_access_reason,
a.crossing_type_code,
ct.name_fr as crossing_type,
a.road_type_code,
rt.name_fr as road_type,
a.transportation_route_image,
a.obs_constriction_code,
oc.name_fr as obs_constriction,
a.water_flowing_upstream_code,
wfup.name_fr as water_flowing_upstream,
a.structure_outlet_image,
a.structure_inlet_image,
a.upstream_physical_blockages_code,
uppb.name_fr as upstream_physical_blockages,
a.upstream_blockage_image,
a.upstream_blockage_height_code,
ubh.name_fr as upstream_blockage_height,
a.downstream_physical_blockages_code,
downph.name_fr as downstream_physical_blockages,
a.downstream_blockage_height_code,
dbh.name_fr as downstream_blockage_height,
a.downstream_blockage_image,
a.water_flowing_under_code,
wfu.name_fr as water_flowing_under,
a.outlet_drop_code,
od.name_fr as outlet_drop,
a.multiple_closed_bottom_code,
mcb.name_fr as multiple_closed_bottom,
a.cbs_constriction_code,
cc.name_fr as cbs_constriction,
a.structure_count,
a.water_flowing_through_code,
wft.name_fr as water_flowing_through,
a.ford_type_code,
ft.name_fr as ford_type,
a.water_flowing_over_code,
wfo.name_fr as water_flowing_over,
a.site_image,
a.structure_signs_code,
ss.name_fr as structure_signs,
a.stream_at_site_code,
a.water_existed_code,
we.name_fr as water_existed,
a.trail_end_code,
te.name_fr as trail_end,
a.access_method_code,
am.name_fr as access_method,
a.close_by_code,
cb.name_fr as close_by,
a.dam_name,
a.partial_dam_removal_code,
pdr.name_fr as partial_dam_removal,
a.downstream_direction_image,
a.downstream_side_image,
a.water_passing_code,
wp.name_fr as water_passing,
a.dam_size_code,
ds.name_fr as dam_size,
a.has_fish_structure,
a.fishway_image,
a.upstream_direction_image,
a.upstream_side_image,
a.notes
from stream_crossings.stream_crossings_community_holding a
left join cabd.feature_type_codes ftc on ftc.code = a.feature_type_code
left join cabd.feature_type_codes ftc2 on ftc2.code = a.to_feature_type_code
left join cabd.response_codes sa on sa.code = a.site_accessible_code
left join cabd.no_access_reason_codes nar on nar.code = a.no_access_reason_code
left join stream_crossings.outlet_drop_codes dbh on dbh.code = a.downstream_blockage_height_code
left join stream_crossings.outlet_drop_codes ubh on ubh.code = a.upstream_blockage_height_code
left join cabd.response_codes mcb on mcb.code = a.multiple_closed_bottom_code
left join cabd.flowing_codes wfup on wfup.code = a.water_flowing_upstream_code
left join cabd.flowing_codes wft on wft.code = a.water_flowing_through_code
left join cabd.response_codes pdr on pdr.code = a.partial_dam_removal_code
left join cabd.flowing_codes wfu on wfu.code = a.water_flowing_under_code
left join cabd.access_method_codes am on am.code = a.access_method_code
left join stream_crossings.cbs_constriction_codes cc on cc.code = a.cbs_constriction_code
left join cabd.response_codes cb on cb.code = a.close_by_code
left join stream_crossings.crossing_type_codes ct on ct.code = a.crossing_type_code
left join dams.size_codes ds on ds.code = a.dam_size_code
left join stream_crossings.ford_type_codes ft on ft.code = a.ford_type_code
left join stream_crossings.obs_constriction_codes oc on oc.code = a.obs_constriction_code
left join stream_crossings.outlet_drop_codes od on od.code = a.outlet_drop_code
left join cabd.road_type_codes rt on rt.code = a.road_type_code
left join cabd.response_codes ss on ss.code = a.structure_signs_code
left join cabd.response_codes te on te.code = a.trail_end_code 
left join cabd.response_codes we on we.code = a.water_existed_code
left join cabd.response_codes wfo on wfo.code = a.water_flowing_over_code
left join dams.side_channel_bypass_codes wp on wp.code = a.water_passing_code

left join 
    (
        select id,
            array_agg(cc.name_fr) AS name_fr
        from stream_crossings.stream_crossings_community_holding 
	    join lateral unnest(upstream_physical_blockages_code) as code_ids on true
	    left join cabd.blockage_type_codes cc on cc.code = code_ids
	    group by id
    ) as uppb on uppb.id = a.id

left join 
    (
        select id,
            array_agg(cc.name_fr) AS name_fr
        from stream_crossings.stream_crossings_community_holding 
	    join lateral unnest(downstream_physical_blockages_code) as code_ids on true
	    left join cabd.blockage_type_codes cc on cc.code = code_ids
	    group by id
    ) as downph on downph.id = a.id
where a.status = 'PROCESSED';


insert into cabd.assessment_type_metadata (type, field_name, data_type, name_en, name_fr) values
('long-form', 'assessment_id', 'uuid', 'Assessment Id', 'Assessment Id'),
('long-form', 'cabd_id', 'uuid', 'CABD Feature Id', 'CABD Feature Id'),
('long-form', 'assessment_type', 'text', 'Assessment Type', 'Assessment Type'),
('long-form', 'other_id', 'varchar', 'Other Id', 'Other Id'),
('long-form', 'assessment_date', 'date', 'Assessment Date', 'Assessment Date'),
('long-form', 'lead_assessor', 'varchar', 'Lead Assessor', 'Lead Assessor'),
('long-form', 'municipality', 'varchar', 'Municipality', 'Municipality'),
('long-form', 'stream_name', 'varchar', 'Stream Name', 'Stream Name'),
('long-form', 'road_name', 'varchar', 'Road Name', 'Road Name'),
('long-form', 'road_type_code', 'integer', 'Road Type Code', 'Road Type Code'),
('long-form', 'road_type', 'varchar', 'Road Type', 'Road Type'),
('long-form', 'latitude', 'numeric', 'Latitude', 'Latitude'),
('long-form', 'longitude', 'numeric', 'Longitude', 'Longitude'),
('long-form', 'location_description', 'varchar', 'Location Description', 'Location Description'),
('long-form', 'land_ownership_context', 'varchar', 'Land Ownership Context', 'Land Ownership Context'),
('long-form', 'incomplete_assess_code', 'integer', 'Incomplete Assess Code', 'Incomplete Assess Code'),
('long-form', 'incomplete_assess', 'varchar', 'Incomplete Assess', 'Incomplete Assess'),
('long-form', 'crossing_type_code', 'integer', 'Crossing Type Code', 'Crossing Type Code'),
('long-form', 'crossing_type', 'varchar', 'Crossing Type', 'Crossing Type'),
('long-form', 'num_structures', 'integer', 'Num Structures', 'Num Structures'),
('long-form', 'photo_id_inlet', 'varchar', 'Photo Id Inlet', 'Photo Id Inlet'),
('long-form', 'photo_id_outlet', 'varchar', 'Photo Id Outlet', 'Photo Id Outlet'),
('long-form', 'photo_id_upstream', 'varchar', 'Photo Id Upstream', 'Photo Id Upstream'),
('long-form', 'photo_id_downstream', 'varchar', 'Photo Id Downstream', 'Photo Id Downstream'),
('long-form', 'photo_id_road_surface', 'varchar', 'Photo Id Road Surface', 'Photo Id Road Surface'),
('long-form', 'photo_id_other_a', 'varchar', 'Photo Id Other A', 'Photo Id Other A'),
('long-form', 'photo_id_other_b', 'varchar', 'Photo Id Other B', 'Photo Id Other B'),
('long-form', 'photo_id_other_c', 'varchar', 'Photo Id Other C', 'Photo Id Other C'),
('long-form', 'flow_condition_code', 'integer', 'Flow Condition Code', 'Flow Condition Code'),
('long-form', 'flow_condition', 'varchar', 'Flow Condition', 'Flow Condition'),
('long-form', 'crossing_condition_code', 'integer', 'Crossing Condition Code', 'Crossing Condition Code'),
('long-form', 'crossing_condition', 'varchar', 'Crossing Condition', 'Crossing Condition'),
('long-form', 'site_type_code', 'integer', 'Site Type Code', 'Site Type Code'),
('long-form', 'site_type', 'varchar', 'Site Type', 'Site Type'),
('long-form', 'alignment_code', 'integer', 'Alignment Code', 'Alignment Code'),
('long-form', 'alignment', 'varchar', 'Alignment', 'Alignment'),
('long-form', 'road_fill_height_m', 'numeric', 'Road Fill Height M', 'Road Fill Height M'),
('long-form', 'bankfull_width_upstr_a_m', 'numeric', 'Bankfull Width Upstr A M', 'Bankfull Width Upstr A M'),
('long-form', 'bankfull_width_upstr_b_m', 'numeric', 'Bankfull Width Upstr B M', 'Bankfull Width Upstr B M'),
('long-form', 'bankfull_width_upstr_c_m', 'numeric', 'Bankfull Width Upstr C M', 'Bankfull Width Upstr C M'),
('long-form', 'bankfull_width_upstr_avg_m', 'numeric', 'Bankfull Width Upstr Avg M', 'Bankfull Width Upstr Avg M'),
('long-form', 'bankfull_width_dnstr_a_m', 'numeric', 'Bankfull Width Dnstr A M', 'Bankfull Width Dnstr A M'),
('long-form', 'bankfull_width_dnstr_b_m', 'numeric', 'Bankfull Width Dnstr B M', 'Bankfull Width Dnstr B M'),
('long-form', 'bankfull_width_dnstr_c_m', 'numeric', 'Bankfull Width Dnstr C M', 'Bankfull Width Dnstr C M'),
('long-form', 'bankfull_width_dnstr_avg_m', 'numeric', 'Bankfull Width Dnstr Avg M', 'Bankfull Width Dnstr Avg M'),
('long-form', 'bankfull_confidence_code', 'integer', 'Bankfull Confidence Code', 'Bankfull Confidence Code'),
('long-form', 'bankful_confidence', 'varchar', 'Bankful Confidence', 'Bankful Confidence'),
('long-form', 'scour_pool_tailwater_code', 'integer', 'Scour Pool Tailwater Code', 'Scour Pool Tailwater Code'),
('long-form', 'sour_pool_tailwater', 'varchar', 'Sour Pool Tailwater', 'Sour Pool Tailwater'),
('long-form', 'crossing_comments', 'varchar', 'Crossing Comments', 'Crossing Comments'),
('long-form', 'structure_info', 'json', 'Structure Info', 'Structure Info'),
('long-form.structure', 'assessment_id', 'uuid', 'Assessment Id', 'Assessment Id'),
('long-form.structure', 'structure_number', 'integer', 'Structure Number', 'Structure Number'),
('long-form.structure', 'outlet_shape_code', 'integer', 'Outlet Shape Code', 'Outlet Shape Code'),
('long-form.structure', 'outlet_shape', 'character varying', 'Outlet Shape', 'Outlet Shape'),
('long-form.structure', 'structure_material_code', 'ARRAY', 'Structure Material Code', 'Structure Material Code'),
('long-form.structure', 'structure_material', 'text', 'Structure Material', 'Structure Material'),
('long-form.structure', 'internal_structures_code', 'integer', 'Internal Structures Code', 'Internal Structures Code'),
('long-form.structure', 'internal_structures', 'character varying', 'Internal Structures', 'Internal Structures'),
('long-form.structure', 'liner_material_code', 'integer', 'Liner Material Code', 'Liner Material Code'),
('long-form.structure', 'linear_material', 'character varying', 'Linear Material', 'Linear Material'),
('long-form.structure', 'outlet_armouring_code', 'integer', 'Outlet Armouring Code', 'Outlet Armouring Code'),
('long-form.structure', 'outlet_armouring', 'character varying', 'Outlet Armouring', 'Outlet Armouring'),
('long-form.structure', 'outlet_grade_code', 'integer', 'Outlet Grade Code', 'Outlet Grade Code'),
('long-form.structure', 'outlet_grade', 'character varying', 'Outlet Grade', 'Outlet Grade'),
('long-form.structure', 'residual_pool_confidence_code', 'integer', 'Residual Pool Confidence Code', 'Residual Pool Confidence Code'),
('long-form.structure', 'residual_pool_confidence', 'character varying', 'Residual Pool Confidence', 'Residual Pool Confidence'),
('long-form.structure', 'inlet_shape_code', 'integer', 'Inlet Shape Code', 'Inlet Shape Code'),
('long-form.structure', 'inlet_shape', 'character varying', 'Inlet Shape', 'Inlet Shape'),
('long-form.structure', 'inlet_type_code', 'integer', 'Inlet Type Code', 'Inlet Type Code'),
('long-form.structure', 'inlet_type', 'character varying', 'Inlet Type', 'Inlet Type'),
('long-form.structure', 'inlet_grade_code', 'integer', 'Inlet Grade Code', 'Inlet Grade Code'),
('long-form.structure', 'inlet_grade', 'character varying', 'Inlet Grade', 'Inlet Grade'),
('long-form.structure', 'structure_slope_method_code', 'integer', 'Structure Slope Method Code', 'Structure Slope Method Code'),
('long-form.structure', 'slope_method', 'character varying', 'Slope Method', 'Slope Method'),
('long-form.structure', 'structure_slope_to_channel_code', 'integer', 'Structure Slope To Channel Code', 'Structure Slope To Channel Code'),
('long-form.structure', 'slope_to_channel', 'character varying', 'Slope To Channel', 'Slope To Channel'),
('long-form.structure', 'substrate_type_code', 'integer', 'Substrate Type Code', 'Substrate Type Code'),
('long-form.structure', 'substrate_type', 'character varying', 'Substrate Type', 'Substrate Type'),
('long-form.structure', 'substrate_matches_stream_code', 'integer', 'Substrate Matches Stream Code', 'Substrate Matches Stream Code'),
('long-form.structure', 'substrate_matches_stream', 'character varying', 'Substrate Matches Stream', 'Substrate Matches Stream'),
('long-form.structure', 'substrate_coverage_code', 'integer', 'Substrate Coverage Code', 'Substrate Coverage Code'),
('long-form.structure', 'substrate_coverage', 'character varying', 'Substrate Coverage', 'Substrate Coverage'),
('long-form.structure', 'substrate_depth_consistent_code', 'integer', 'Substrate Depth Consistent Code', 'Substrate Depth Consistent Code'),
('long-form.structure', 'substrate_depth_consistent', 'character varying', 'Substrate Depth Consistent', 'Substrate Depth Consistent'),
('long-form.structure', 'backwatered_pct_code', 'integer', 'Backwatered Pct Code', 'Backwatered Pct Code'),
('long-form.structure', 'backwatered_pct', 'character varying', 'Backwatered Pct', 'Backwatered Pct'),
('long-form.structure', 'physical_blockages_code', 'ARRAY', 'Physical Blockages Code', 'Physical Blockages Code'),
('long-form.structure', 'physical_blockages', 'text', 'Physical Blockages', 'Physical Blockages'),
('long-form.structure', 'physical_blockage_severity_code', 'integer', 'Physical Blockage Severity Code', 'Physical Blockage Severity Code'),
('long-form.structure', 'physical_blockage_severity', 'character varying', 'Physical Blockage Severity', 'Physical Blockage Severity'),
('long-form.structure', 'water_depth_matches_stream_code', 'integer', 'Water Depth Matches Stream Code', 'Water Depth Matches Stream Code'),
('long-form.structure', 'water_depth_matches_stream', 'character varying', 'Water Depth Matches Stream', 'Water Depth Matches Stream'),
('long-form.structure', 'water_velocity_matches_stream_code', 'integer', 'Water Velocity Matches Stream Code', 'Water Velocity Matches Stream Code'),
('long-form.structure', 'water_velocity_matches_stream', 'character varying', 'Water Velocity Matches Stream', 'Water Velocity Matches Stream'),
('long-form.structure', 'dry_passage_code', 'integer', 'Dry Passage Code', 'Dry Passage Code'),
('long-form.structure', 'dry_passage', 'character varying', 'Dry Passage', 'Dry Passage'),
('long-form.structure', 'passability_status_code', 'integer', 'Passability Status Code', 'Passability Status Code'),
('long-form.structure', 'passability_status', 'character varying', 'Passability Status', 'Passability Status'),
('long-form.structure', 'outlet_width_m', 'numeric', 'Outlet Width M', 'Outlet Width M'),
('long-form.structure', 'outlet_height_m', 'numeric', 'Outlet Height M', 'Outlet Height M'),
('long-form.structure', 'outlet_substrate_water_width_m', 'numeric', 'Outlet Substrate Water Width M', 'Outlet Substrate Water Width M'),
('long-form.structure', 'outlet_water_depth_m', 'numeric', 'Outlet Water Depth M', 'Outlet Water Depth M'),
('long-form.structure', 'abutment_height_m', 'numeric', 'Abutment Height M', 'Abutment Height M'),
('long-form.structure', 'outlet_drop_to_water_surface_m', 'numeric', 'Outlet Drop To Water Surface M', 'Outlet Drop To Water Surface M'),
('long-form.structure', 'outlet_drop_to_stream_bottom_m', 'numeric', 'Outlet Drop To Stream Bottom M', 'Outlet Drop To Stream Bottom M'),
('long-form.structure', 'outlet_water_surface_to_residual_pool_top_m', 'numeric', 'Outlet Water Surface To Residual Pool Top M', 'Outlet Water Surface To Residual Pool Top M'),
('long-form.structure', 'structure_length_m', 'numeric', 'Structure Length M', 'Structure Length M'),
('long-form.structure', 'inlet_width_m', 'numeric', 'Inlet Width M', 'Inlet Width M'),
('long-form.structure', 'inlet_height_m', 'numeric', 'Inlet Height M', 'Inlet Height M'),
('long-form.structure', 'inlet_substrate_water_width_m', 'numeric', 'Inlet Substrate Water Width M', 'Inlet Substrate Water Width M'),
('long-form.structure', 'inlet_water_depth_m', 'numeric', 'Inlet Water Depth M', 'Inlet Water Depth M'),
('long-form.structure', 'structure_slope_pct', 'numeric', 'Structure Slope Pct', 'Structure Slope Pct'),
('long-form.structure', 'height_above_dry_passage_m', 'numeric', 'Height Above Dry Passage M', 'Height Above Dry Passage M'),
('long-form.structure', 'structure_comments', 'character varying', 'Structure Comments', 'Structure Comments');

drop view if exists stream_crossings.assessment_longform_en;
create view stream_crossings.assessment_longform_en as
with structure_data as (
select assessment_id, json_agg(to_jsonb(vw)) as structure_info
from (
select
    d.assessment_id,
    d.structure_number,
    d.outlet_shape_code,
    os.name_en AS outlet_shape,

    d.structure_material_code,
    (
        SELECT array_agg(m.name_en)
        FROM unnest(d.structure_material_code) AS mat_code
        JOIN stream_crossings.material_codes m ON m.code = mat_code
    ) AS structure_material,
    
    d.internal_structures_code,
    istruct.name_en AS internal_structures,
    d.liner_material_code,
    lm.name_en as linear_material,
    d.outlet_armouring_code,
    oa.name_en AS outlet_armouring,
    d.outlet_grade_code,
    og.name_en AS outlet_grade,
    d.residual_pool_confidence_code,
    rpc.name_en AS residual_pool_confidence,
    d.inlet_shape_code,
    ishp.name_en AS inlet_shape,
    d.inlet_type_code,
    itype.name_en AS inlet_type,
    d.inlet_grade_code,
    ig.name_en AS inlet_grade,
    d.structure_slope_method_code,
    sm.name_en AS slope_method,
    d.structure_slope_to_channel_code,
    sc.name_en AS slope_to_channel,
    d.substrate_type_code,
    st.name_en AS substrate_type,
    d.substrate_matches_stream_code,
    sms.name_en AS substrate_matches_stream,
    d.substrate_coverage_code,
    cov.name_en AS substrate_coverage,
    d.substrate_depth_consistent_code,
    sdc.name_en AS substrate_depth_consistent,
    d.backwatered_pct_code,
    bwp.name_en AS backwatered_pct,
    d.physical_blockages_code,
    (
        SELECT array_agg(pb.name_en)
        FROM unnest(d.physical_blockages_code) AS pb_code
        JOIN cabd.blockage_type_codes pb ON pb.code = pb_code
    ) AS physical_blockages,
    d.physical_blockage_severity_code,
    pbs.name_en AS physical_blockage_severity,
    d.water_depth_matches_stream_code,
    wdms.name_en AS water_depth_matches_stream,
    d.water_velocity_matches_stream_code,
    wvms.name_en AS water_velocity_matches_stream,
    d.dry_passage_code,
    dp.name_en AS dry_passage,
    d.passability_status_code,
    ps.name_en AS passability_status,
    -- Include all original metrics and attributes as needed
    d.outlet_width_m,
    d.outlet_height_m,
    d.outlet_substrate_water_width_m,
    d.outlet_water_depth_m,
    d.abutment_height_m,
    d.outlet_drop_to_water_surface_m,
    d.outlet_drop_to_stream_bottom_m,
    d.outlet_water_surface_to_residual_pool_top_m,
    d.structure_length_m,
    d.inlet_width_m,
    d.inlet_height_m,
    d.inlet_substrate_water_width_m,
    d.inlet_water_depth_m,
    d.structure_slope_pct,
    d.height_above_dry_passage_m,
    d.structure_comments
FROM stream_crossings.assessment_structure_data d
LEFT JOIN stream_crossings.shape_codes os ON os.code = d.outlet_shape_code
LEFT JOIN stream_crossings.internal_structure_codes istruct ON istruct.code = d.internal_structures_code
LEFT JOIN stream_crossings.armouring_codes oa ON oa.code = d.outlet_armouring_code
LEFT JOIN stream_crossings.grade_codes og ON og.code = d.outlet_grade_code
LEFT JOIN stream_crossings.confidence_codes rpc ON rpc.code = d.residual_pool_confidence_code
LEFT JOIN stream_crossings.shape_codes ishp ON ishp.code = d.inlet_shape_code
LEFT JOIN stream_crossings.inlet_type_codes itype ON itype.code = d.inlet_type_code
LEFT JOIN stream_crossings.grade_codes ig ON ig.code = d.inlet_grade_code
LEFT JOIN stream_crossings.slope_method_codes sm ON sm.code = d.structure_slope_method_code
LEFT JOIN stream_crossings.relative_slope_codes sc ON sc.code = d.structure_slope_to_channel_code
LEFT JOIN stream_crossings.substrate_type_codes st ON st.code = d.substrate_type_code
LEFT JOIN stream_crossings.substrate_matches_stream_codes sms ON sms.code = d.substrate_matches_stream_code
LEFT JOIN stream_crossings.structure_coverage_codes cov ON cov.code = d.substrate_coverage_code
LEFT JOIN cabd.response_codes sdc ON sdc.code = d.substrate_depth_consistent_code
LEFT JOIN stream_crossings.structure_coverage_codes bwp ON bwp.code = d.backwatered_pct_code
LEFT JOIN stream_crossings.blockage_severity_codes pbs ON pbs.code = d.physical_blockage_severity_code
LEFT JOIN stream_crossings.water_depth_matches_stream_codes wdms ON wdms.code = d.water_depth_matches_stream_code
LEFT JOIN stream_crossings.water_velocity_matches_stream_codes wvms ON wvms.code = d.water_velocity_matches_stream_code
LEFT JOIN cabd.response_codes dp ON dp.code = d.dry_passage_code
LEFT JOIN stream_crossings.material_codes lm on lm.code = d.liner_material_code
LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code
order by assessment_id, structure_number
) as vw group by assessment_id

)
SELECT
a.id as assessment_id,
a.cabd_id,
'long-form' as assessment_type,
a.other_id,
a.date_assessed as assessment_date,
a.lead_assessor,
a.municipality,
a.stream_name,
a.road_name,
a.road_type_code,
rt.name_en as road_type,
a.latitude,
a.longitude,
a.location_description,
a.land_ownership_context,
a.incomplete_assess_code,
ia.name_en as incomplete_assess,
a.crossing_type_code,
ct.name_en as crossing_type,
a.num_structures,
a.photo_id_inlet,
a.photo_id_outlet,
a.photo_id_upstream,
a.photo_id_downstream,
a.photo_id_road_surface,
a.photo_id_other_a,
a.photo_id_other_b,
a.photo_id_other_c,
a.flow_condition_code,
fc.name_en as flow_condition,
a.crossing_condition_code,
cc.name_en as crossing_condition,
a.site_type_code,
st.name_en as site_type,
a.alignment_code,
ac.name_en as alignment,
a.road_fill_height_m,
a.bankfull_width_upstr_a_m,
a.bankfull_width_upstr_b_m,
a.bankfull_width_upstr_c_m,
a.bankfull_width_upstr_avg_m,
a.bankfull_width_dnstr_a_m,
a.bankfull_width_dnstr_b_m,
a.bankfull_width_dnstr_c_m,
a.bankfull_width_dnstr_avg_m,
a.bankfull_confidence_code,
bc.name_en as bankful_confidence,
a.scour_pool_tailwater_code,
spt.name_en as sour_pool_tailwater,
a.crossing_comments,
b.structure_info as structure_data

from stream_crossings.assessment_data a
left join structure_data b on a.id = b.assessment_id
left join stream_crossings.alignment_codes ac on ac.code = a.alignment_code
left join stream_crossings.confidence_codes bc on bc.code = a.bankfull_confidence_code
left join stream_crossings.crossing_condition_codes cc on cc.code = a.crossing_condition_code
left join stream_crossings.crossing_type_codes ct on ct.code = a.crossing_type_code
left join stream_crossings.flow_condition_codes fc on fc.code = a.flow_condition_code
left join stream_crossings.incomplete_assessment_codes ia on ia.code = a.incomplete_assess_code
left join cabd.road_type_codes rt on rt.code = a.road_type_code
left join stream_crossings.scour_pool_codes spt on spt.code = a.scour_pool_tailwater_code
left join stream_crossings.site_type_codes st on st.code = a.site_type_code
where a.status = 'PROCESSED';



drop view if exists stream_crossings.assessment_longform_fr;
create view stream_crossings.assessment_longform_fr as
with structure_data as (
select assessment_id, json_agg(to_jsonb(vw)) as structure_info
from (
select
    d.assessment_id,
    d.structure_number,
    d.outlet_shape_code,
    os.name_fr AS outlet_shape,

    d.structure_material_code,
    (
        SELECT array_agg(m.name_fr)
        FROM unnest(d.structure_material_code) AS mat_code
        JOIN stream_crossings.material_codes m ON m.code = mat_code
    ) AS structure_material,
    
    d.internal_structures_code,
    istruct.name_fr AS internal_structures,
    d.liner_material_code,
    lm.name_fr as linear_material,
    d.outlet_armouring_code,
    oa.name_fr AS outlet_armouring,
    d.outlet_grade_code,
    og.name_fr AS outlet_grade,
    d.residual_pool_confidence_code,
    rpc.name_fr AS residual_pool_confidence,
    d.inlet_shape_code,
    ishp.name_fr AS inlet_shape,
    d.inlet_type_code,
    itype.name_fr AS inlet_type,
    d.inlet_grade_code,
    ig.name_fr AS inlet_grade,
    d.structure_slope_method_code,
    sm.name_fr AS slope_method,
    d.structure_slope_to_channel_code,
    sc.name_fr AS slope_to_channel,
    d.substrate_type_code,
    st.name_fr AS substrate_type,
    d.substrate_matches_stream_code,
    sms.name_fr AS substrate_matches_stream,
    d.substrate_coverage_code,
    cov.name_fr AS substrate_coverage,
    d.substrate_depth_consistent_code,
    sdc.name_fr AS substrate_depth_consistent,
    d.backwatered_pct_code,
    bwp.name_fr AS backwatered_pct,
    d.physical_blockages_code,
    (
        SELECT array_agg(pb.name_fr)
        FROM unnest(d.physical_blockages_code) AS pb_code
        JOIN cabd.blockage_type_codes pb ON pb.code = pb_code
    ) AS physical_blockages,
    d.physical_blockage_severity_code,
    pbs.name_fr AS physical_blockage_severity,
    d.water_depth_matches_stream_code,
    wdms.name_fr AS water_depth_matches_stream,
    d.water_velocity_matches_stream_code,
    wvms.name_fr AS water_velocity_matches_stream,
    d.dry_passage_code,
    dp.name_fr AS dry_passage,
    d.passability_status_code,
    ps.name_fr AS passability_status,
    -- Include all original metrics and attributes as needed
    d.outlet_width_m,
    d.outlet_height_m,
    d.outlet_substrate_water_width_m,
    d.outlet_water_depth_m,
    d.abutment_height_m,
    d.outlet_drop_to_water_surface_m,
    d.outlet_drop_to_stream_bottom_m,
    d.outlet_water_surface_to_residual_pool_top_m,
    d.structure_length_m,
    d.inlet_width_m,
    d.inlet_height_m,
    d.inlet_substrate_water_width_m,
    d.inlet_water_depth_m,
    d.structure_slope_pct,
    d.height_above_dry_passage_m,
    d.structure_comments
FROM stream_crossings.assessment_structure_data d
LEFT JOIN stream_crossings.shape_codes os ON os.code = d.outlet_shape_code
LEFT JOIN stream_crossings.internal_structure_codes istruct ON istruct.code = d.internal_structures_code
LEFT JOIN stream_crossings.armouring_codes oa ON oa.code = d.outlet_armouring_code
LEFT JOIN stream_crossings.grade_codes og ON og.code = d.outlet_grade_code
LEFT JOIN stream_crossings.confidence_codes rpc ON rpc.code = d.residual_pool_confidence_code
LEFT JOIN stream_crossings.shape_codes ishp ON ishp.code = d.inlet_shape_code
LEFT JOIN stream_crossings.inlet_type_codes itype ON itype.code = d.inlet_type_code
LEFT JOIN stream_crossings.grade_codes ig ON ig.code = d.inlet_grade_code
LEFT JOIN stream_crossings.slope_method_codes sm ON sm.code = d.structure_slope_method_code
LEFT JOIN stream_crossings.relative_slope_codes sc ON sc.code = d.structure_slope_to_channel_code
LEFT JOIN stream_crossings.substrate_type_codes st ON st.code = d.substrate_type_code
LEFT JOIN stream_crossings.substrate_matches_stream_codes sms ON sms.code = d.substrate_matches_stream_code
LEFT JOIN stream_crossings.structure_coverage_codes cov ON cov.code = d.substrate_coverage_code
LEFT JOIN cabd.response_codes sdc ON sdc.code = d.substrate_depth_consistent_code
LEFT JOIN stream_crossings.structure_coverage_codes bwp ON bwp.code = d.backwatered_pct_code
LEFT JOIN stream_crossings.blockage_severity_codes pbs ON pbs.code = d.physical_blockage_severity_code
LEFT JOIN stream_crossings.water_depth_matches_stream_codes wdms ON wdms.code = d.water_depth_matches_stream_code
LEFT JOIN stream_crossings.water_velocity_matches_stream_codes wvms ON wvms.code = d.water_velocity_matches_stream_code
LEFT JOIN cabd.response_codes dp ON dp.code = d.dry_passage_code
LEFT JOIN stream_crossings.material_codes lm on lm.code = d.liner_material_code
LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code
order by assessment_id, structure_number
) as vw group by assessment_id

)
SELECT
a.id as assessment_id,
a.cabd_id,
'long-form' as assessment_type,
a.other_id,
a.date_assessed as assessment_date,
a.lead_assessor,
a.municipality,
a.stream_name,
a.road_name,
a.road_type_code,
rt.name_fr as road_type,
a.latitude,
a.longitude,
a.location_description,
a.land_ownership_context,
a.incomplete_assess_code,
ia.name_fr as incomplete_assess,
a.crossing_type_code,
ct.name_fr as crossing_type,
a.num_structures,
a.photo_id_inlet,
a.photo_id_outlet,
a.photo_id_upstream,
a.photo_id_downstream,
a.photo_id_road_surface,
a.photo_id_other_a,
a.photo_id_other_b,
a.photo_id_other_c,
a.flow_condition_code,
fc.name_fr as flow_condition,
a.crossing_condition_code,
cc.name_fr as crossing_condition,
a.site_type_code,
st.name_fr as site_type,
a.alignment_code,
ac.name_fr as alignment,
a.road_fill_height_m,
a.bankfull_width_upstr_a_m,
a.bankfull_width_upstr_b_m,
a.bankfull_width_upstr_c_m,
a.bankfull_width_upstr_avg_m,
a.bankfull_width_dnstr_a_m,
a.bankfull_width_dnstr_b_m,
a.bankfull_width_dnstr_c_m,
a.bankfull_width_dnstr_avg_m,
a.bankfull_confidence_code,
bc.name_fr as bankful_confidence,
a.scour_pool_tailwater_code,
spt.name_fr as sour_pool_tailwater,
a.crossing_comments,
b.structure_info as structure_data

from stream_crossings.assessment_data a
left join structure_data b on a.id = b.assessment_id
left join stream_crossings.alignment_codes ac on ac.code = a.alignment_code
left join stream_crossings.confidence_codes bc on bc.code = a.bankfull_confidence_code
left join stream_crossings.crossing_condition_codes cc on cc.code = a.crossing_condition_code
left join stream_crossings.crossing_type_codes ct on ct.code = a.crossing_type_code
left join stream_crossings.flow_condition_codes fc on fc.code = a.flow_condition_code
left join stream_crossings.incomplete_assessment_codes ia on ia.code = a.incomplete_assess_code
left join cabd.road_type_codes rt on rt.code = a.road_type_code
left join stream_crossings.scour_pool_codes spt on spt.code = a.scour_pool_tailwater_code
left join stream_crossings.site_type_codes st on st.code = a.site_type_code
where a.status = 'PROCESSED';