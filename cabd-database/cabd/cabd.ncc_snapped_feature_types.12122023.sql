DROP VIEW IF EXISTS cabd.dams_ncc_view_fr;
DROP VIEW IF EXISTS cabd.dams_ncc_view_en;

CREATE OR REPLACE VIEW cabd.dams_ncc_view_en
AS SELECT d.cabd_id,
    'dams_ncc'::text AS feature_type,
    'features/datasources/'::text || d.cabd_id AS datasource_url,
    st_y(st_transform(d.snapped_ncc, 4617)) AS latitude,
    st_x(st_transform(d.snapped_ncc, 4617)) AS longitude,
    d.dam_name_en,
    d.dam_name_fr,
    d.facility_name_en,
    d.facility_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.reservoir_name_en,
    d.reservoir_name_fr,
    d.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    d.province_territory_code,
    pt.name_en AS province_territory,
    d.owner,
    d.ownership_type_code,
    ow.name_en AS ownership_type,
    d.municipality,
    d.provincial_compliance_status,
    d.federal_compliance_status,
    d.operating_notes,
    d.operating_status_code,
    os.name_en AS operating_status,
    d.removed_year,
    d.use_code,
    duc.name_en AS dam_use,
    d.use_irrigation_code,
    c1.name_en AS use_irrigation,
    d.use_electricity_code,
    c2.name_en AS use_electricity,
    d.use_supply_code,
    c3.name_en AS use_supply,
    d.use_floodcontrol_code,
    c4.name_en AS use_floodcontrol,
    d.use_recreation_code,
    c5.name_en AS use_recreation,
    d.use_navigation_code,
    c6.name_en AS use_navigation,
    d.use_fish_code,
    c7.name_en AS use_fish,
    d.use_pollution_code,
    c8.name_en AS use_pollution,
    d.use_invasivespecies_code,
    c9.name_en AS use_invasivespecies,
    d.use_conservation_code,
    c10.name_en AS use_conservation,
    d.use_other_code,
    c11.name_en AS use_other,
    d.lake_control_code,
    lk.name_en AS lake_control,
    d.construction_year,
    d.assess_schedule,
    d.expected_end_of_life,
    d.maintenance_last,
    d.maintenance_next,
    d.function_code,
    f.name_en AS function_name,
    d.condition_code,
    dc.name_en AS dam_condition,
    d.structure_type_code,
    dst.name_en AS structure_type,
    d.construction_material_code,
    dcm.name_en AS construction_material,
    d.height_m,
    d.length_m,
    d.size_class_code,
    ds.name_en AS size_class,
    d.spillway_capacity,
    d.spillway_type_code,
    dsp.name_en AS spillway_type,
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
    dt.name_en AS turbine_type,
    d.up_passage_type_code,
    up.name_en AS up_passage_type,
    d.down_passage_route_code,
    down.name_en AS down_passage_route,
    d.last_modified,
    d.use_analysis,
    d.comments,
    d.passability_status_code,
    ps.name_en AS passability_status,
    d.passability_status_note,
    d.complete_level_code,
    cl.name_en AS complete_level,
        CASE
            WHEN upd.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    d.snapped_ncc AS geometry
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
     LEFT JOIN dams.use_codes c10 ON c10.code = d.use_conservation_code
     LEFT JOIN dams.use_codes c11 ON c11.code = d.use_other_code
     LEFT JOIN dams.function_codes f ON f.code = d.function_code
     LEFT JOIN dams.condition_codes dc ON dc.code = d.condition_code
     LEFT JOIN dams.structure_type_codes dst ON dst.code = d.structure_type_code
     LEFT JOIN dams.construction_material_codes dcm ON dcm.code = d.construction_material_code
     LEFT JOIN dams.size_codes ds ON ds.code = d.size_class_code
     LEFT JOIN dams.spillway_type_codes dsp ON dsp.code = d.spillway_type_code
     LEFT JOIN dams.turbine_type_codes dt ON dt.code = d.turbine_type_code
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN dams.downstream_passage_route_codes down ON down.code = d.down_passage_route_code
     LEFT JOIN dams.dam_complete_level_codes cl ON cl.code = d.complete_level_code
     LEFT JOIN dams.lake_control_codes lk ON lk.code = d.lake_control_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code
     LEFT JOIN cabd.updates_pending upd ON upd.cabd_id = d.cabd_id;
     
     -- cabd.dams_view_fr source

CREATE OR REPLACE VIEW cabd.dams_ncc_view_fr
AS SELECT d.cabd_id,
    'dams'::text AS feature_type,
    'features/datasources/'::text || d.cabd_id AS datasource_url,
    st_y(st_transform(d.snapped_ncc, 4617)) AS latitude,
    st_x(st_transform(d.snapped_ncc, 4617)) AS longitude,
    d.dam_name_en,
    d.dam_name_fr,
    d.facility_name_en,
    d.facility_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.reservoir_name_en,
    d.reservoir_name_fr,
    d.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    d.province_territory_code,
    pt.name_fr AS province_territory,
    d.owner,
    d.ownership_type_code,
    ow.name_fr AS ownership_type,
    d.municipality,
    d.provincial_compliance_status,
    d.federal_compliance_status,
    d.operating_notes,
    d.operating_status_code,
    os.name_fr AS operating_status,
    d.removed_year,
    d.use_code,
    duc.name_fr AS dam_use,
    d.use_irrigation_code,
    c1.name_fr AS use_irrigation,
    d.use_electricity_code,
    c2.name_fr AS use_electricity,
    d.use_supply_code,
    c3.name_fr AS use_supply,
    d.use_floodcontrol_code,
    c4.name_fr AS use_floodcontrol,
    d.use_recreation_code,
    c5.name_fr AS use_recreation,
    d.use_navigation_code,
    c6.name_fr AS use_navigation,
    d.use_fish_code,
    c7.name_fr AS use_fish,
    d.use_pollution_code,
    c8.name_fr AS use_pollution,
    d.use_invasivespecies_code,
    c9.name_fr AS use_invasivespecies,
    d.use_conservation_code,
    c10.name_fr AS use_conservation,
    d.use_other_code,
    c11.name_fr AS use_other,
    d.lake_control_code,
    lk.name_fr AS lake_control,
    d.construction_year,
    d.assess_schedule,
    d.expected_end_of_life,
    d.maintenance_last,
    d.maintenance_next,
    d.function_code,
    f.name_fr AS function_name,
    d.condition_code,
    dc.name_fr AS dam_condition,
    d.structure_type_code,
    dst.name_fr AS structure_type,
    d.construction_material_code,
    dcm.name_fr AS construction_material,
    d.height_m,
    d.length_m,
    d.size_class_code,
    ds.name_fr AS size_class,
    d.spillway_capacity,
    d.spillway_type_code,
    dsp.name_fr AS spillway_type,
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
    dt.name_fr AS turbine_type,
    d.up_passage_type_code,
    up.name_fr AS up_passage_type,
    d.down_passage_route_code,
    down.name_fr AS down_passage_route,
    d.last_modified,
    d.use_analysis,
    d.comments,
    d.passability_status_code,
    ps.name_fr AS passability_status,
    d.passability_status_note,
    d.complete_level_code,
    cl.name_fr AS complete_level,
        CASE
            WHEN upd.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    d.snapped_ncc AS geometry
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
     LEFT JOIN dams.use_codes c10 ON c10.code = d.use_conservation_code
     LEFT JOIN dams.use_codes c11 ON c11.code = d.use_other_code
     LEFT JOIN dams.function_codes f ON f.code = d.function_code
     LEFT JOIN dams.condition_codes dc ON dc.code = d.condition_code
     LEFT JOIN dams.structure_type_codes dst ON dst.code = d.structure_type_code
     LEFT JOIN dams.construction_material_codes dcm ON dcm.code = d.construction_material_code
     LEFT JOIN dams.size_codes ds ON ds.code = d.size_class_code
     LEFT JOIN dams.spillway_type_codes dsp ON dsp.code = d.spillway_type_code
     LEFT JOIN dams.turbine_type_codes dt ON dt.code = d.turbine_type_code
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN dams.downstream_passage_route_codes down ON down.code = d.down_passage_route_code
     LEFT JOIN dams.dam_complete_level_codes cl ON cl.code = d.complete_level_code
     LEFT JOIN dams.lake_control_codes lk ON lk.code = d.lake_control_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code
     LEFT JOIN cabd.updates_pending upd ON upd.cabd_id = d.cabd_id;
     
     
insert into cabd.feature_types(type, data_view, name_en, attribute_source_Table, default_featurename_field, feature_source_table, name_fr, data_version, description, data_table)
values('dams_ncc', 'cabd.dams_ncc_view', 'Dams - Snapped to NCC Network', 'dams.dams_attribute_source', 'dam_name_en', 'dams.dams_feature_source', 'Dams - Snapped to NCC Network', '1.1', 'Same as the Dams feature types, except the dam locations have been snapped to the NCC hydro network.', 'dams.dams');

insert into cabd.feature_type_metadata(view_name, field_name, name_en, description_en, is_link, data_type, vw_simple_order, vw_all_order, include_vector_tile, value_options_reference, name_fr, description_fr, is_name_search, shape_field_name)
select 'cabd.dams_ncc_view', field_name, name_en, description_en, is_link, data_type, vw_simple_order, vw_all_order, include_vector_tile, value_options_reference, name_fr, description_fr, is_name_search, shape_field_name 
from cabd.feature_type_metadata
where view_name = 'cabd.dams_view';







-- cabd.waterfalls_view_en source

CREATE OR REPLACE VIEW cabd.waterfalls_ncc_view_en
AS SELECT w.cabd_id,
    'features/datasources/'::text || w.cabd_id AS datasource_url,
    'waterfalls_ncc'::text AS feature_type,
   st_y(st_transform(w.snapped_ncc, 4617)) AS latitude,
    st_x(st_transform(w.snapped_ncc, 4617)) AS longitude,
    w.fall_name_en,
    w.fall_name_fr,
    w.waterbody_name_en,
    w.waterbody_name_fr,
    w.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    w.province_territory_code,
    pt.name_en AS province_territory,
    w.municipality,
    w.fall_height_m,
    w.last_modified,
    w.use_analysis,
    w.comments,
    w.complete_level_code,
    cl.name_en AS complete_level,
    w.passability_status_code,
    ps.name_en AS passability_status,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    w.snapped_ncc AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = w.cabd_id;
     
     
     
     
-- cabd.waterfalls_view_fr source

CREATE OR REPLACE VIEW cabd.waterfalls_ncc_view_fr
AS SELECT w.cabd_id,
    'features/datasources/'::text || w.cabd_id AS datasource_url,
    'waterfalls_ncc'::text AS feature_type,
    st_y(st_transform(w.snapped_ncc, 4617)) AS latitude,
    st_x(st_transform(w.snapped_ncc, 4617)) AS longitude,
    w.fall_name_en,
    w.fall_name_fr,
    w.waterbody_name_en,
    w.waterbody_name_fr,
    w.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    w.province_territory_code,
    pt.name_fr AS province_territory,
    w.municipality,
    w.fall_height_m,
    w.last_modified,
    w.use_analysis,
    w.comments,
    w.complete_level_code,
    cl.name_fr AS complete_level,
    w.passability_status_code,
    ps.name_fr AS passability_status,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    w.snapped_ncc AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = w.cabd_id;     
     
     
     
insert into cabd.feature_types(type, data_view, name_en, attribute_source_Table, default_featurename_field, feature_source_table, name_fr, data_version, description, data_table)
values('waterfalls_ncc', 'cabd.waterfalls_ncc_view', 'Waterfalls - Snapped to NCC Network', 'waterfalls.waterfalls_attribute_source', 'fall_name_en', 'waterfalls.waterfalls_feature_source', 'Waterfalls - Snapped to NCC Network', '1', 'Same as the Waterfalls feature types, except the waterfall locations have been snapped to the NCC hydro network.', 'waterfalls.waterfalls');

insert into cabd.feature_type_metadata(view_name, field_name, name_en, description_en, is_link, data_type, vw_simple_order, vw_all_order, include_vector_tile, value_options_reference, name_fr, description_fr, is_name_search, shape_field_name)
select 'cabd.waterfalls_ncc_view', field_name, name_en, description_en, is_link, data_type, vw_simple_order, vw_all_order, include_vector_tile, value_options_reference, name_fr, description_fr, is_name_search, shape_field_name 
from cabd.feature_type_metadata
where view_name = 'cabd.waterfalls_view';


alter view cabd.waterfalls_ncc_view_en owner to cabd;     
alter view cabd.waterfalls_ncc_view_fr owner to cabd;   
alter view cabd.dams_ncc_view_en owner to cabd;     
alter view cabd.dams_ncc_view_fr owner to cabd;   


grant select on cabd.waterfalls_ncc_view_en to cwf_user;     
grant select on cabd.waterfalls_ncc_view_fr to cwf_user;   
grant select on cabd.dams_ncc_view_en owner to cwf_user;     
grant select on cabd.dams_ncc_view_fr owner to cwf_user;   