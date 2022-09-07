alter table cabd.feature_types rename column name to name_en;
alter table cabd.feature_types add column name_fr varchar(256);

alter table cabd.feature_type_metadata rename column name to name_en;
alter table cabd.feature_type_metadata add column name_fr varchar;
alter table cabd.feature_type_metadata rename column description to description_en;
alter table cabd.feature_type_metadata add column description_fr varchar;


alter table cabd.barrier_ownership_type_codes rename column name to name_en;
alter table cabd.barrier_ownership_type_codes rename column description to description_en;
alter table cabd.barrier_ownership_type_codes add column name_fr varchar(64);
alter table cabd.barrier_ownership_type_codes add column description_fr text;


alter table cabd.passability_status_codes rename column name to name_en;
alter table cabd.passability_status_codes rename column description to description_en;
alter table cabd.passability_status_codes add column name_fr varchar(64);
alter table cabd.passability_status_codes add column description_fr text;

alter table cabd.province_territory_codes  rename column name to name_en;
alter table cabd.province_territory_codes add column name_fr varchar(64);

alter table cabd.upstream_passage_type_codes  rename column name to name_en;
alter table cabd.upstream_passage_type_codes rename column description to description_en;
alter table cabd.upstream_passage_type_codes add column name_fr varchar(64);
alter table cabd.upstream_passage_type_codes add column description_fr text;

alter table dams.condition_codes rename column name to name_en;
alter table dams.condition_codes rename column description to description_en;
alter table dams.condition_codes add column name_fr varchar(64);
alter table dams.condition_codes add column description_fr text;

alter table dams.construction_type_codes rename column name to name_en;
alter table dams.construction_type_codes rename column description to description_en;
alter table dams.construction_type_codes add column name_fr varchar(64);
alter table dams.construction_type_codes add column description_fr text;

alter table dams.dam_complete_level_codes rename column name to name_en;
alter table dams.dam_complete_level_codes rename column description to description_en;
alter table dams.dam_complete_level_codes add column name_fr varchar(64);
alter table dams.dam_complete_level_codes add column description_fr text;

alter table dams.dam_use_codes rename column name to name_en;
alter table dams.dam_use_codes rename column description to description_en;
alter table dams.dam_use_codes add column name_fr varchar(64);
alter table dams.dam_use_codes add column description_fr text;

alter table dams.downstream_passage_route_codes rename column name to name_en;
alter table dams.downstream_passage_route_codes rename column description to description_en;
alter table dams.downstream_passage_route_codes add column name_fr varchar(64);
alter table dams.downstream_passage_route_codes add column description_fr text;

alter table dams.function_codes rename column name to name_en;
alter table dams.function_codes rename column description to description_en;
alter table dams.function_codes add column name_fr varchar(64);
alter table dams.function_codes add column description_fr text;

alter table dams.lake_control_codes rename column name to name_en;
alter table dams.lake_control_codes rename column description to description_en;
alter table dams.lake_control_codes add column name_fr varchar(64);
alter table dams.lake_control_codes add column description_fr text;

alter table dams.operating_status_codes rename column name to name_en;
alter table dams.operating_status_codes rename column description to description_en;
alter table dams.operating_status_codes add column name_fr varchar(64);
alter table dams.operating_status_codes add column description_fr text;

--alter table dams.passability_status_codes rename column name to name_en;
--alter table dams.passability_status_codes rename column description to description_en;
--alter table dams.passability_status_codes add column name_fr varchar(64);
--alter table dams.passability_status_codes add column description_fr text;

alter table dams.size_codes rename column name to name_en;
alter table dams.size_codes rename column description to description_en;
alter table dams.size_codes add column name_fr varchar(64);
alter table dams.size_codes add column description_fr text;

alter table dams.spillway_type_codes rename column name to name_en;
alter table dams.spillway_type_codes rename column description to description_en;
alter table dams.spillway_type_codes add column name_fr varchar(64);
alter table dams.spillway_type_codes add column description_fr text;

alter table dams.turbine_type_codes rename column name to name_en;
alter table dams.turbine_type_codes rename column description to description_en;
alter table dams.turbine_type_codes add column name_fr varchar(64);
alter table dams.turbine_type_codes add column description_fr text;

alter table dams.use_codes rename column name to name_en;
alter table dams.use_codes rename column description to description_en;
alter table dams.use_codes add column name_fr varchar(64);
alter table dams.use_codes add column description_fr text;

alter table waterfalls.waterfall_complete_level_codes  rename column name to name_en;
alter table waterfalls.waterfall_complete_level_codes rename column description to description_en;
alter table waterfalls.waterfall_complete_level_codes add column name_fr varchar(64);
alter table waterfalls.waterfall_complete_level_codes add column description_fr text;

alter table fishways.entrance_location_codes rename column name to name_en;
alter table fishways.entrance_location_codes rename column description to description_en;
alter table fishways.entrance_location_codes add column name_fr varchar(64);
alter table fishways.entrance_location_codes add column description_fr text;

alter table fishways.entrance_position_codes rename column name to name_en;
alter table fishways.entrance_position_codes rename column description to description_en;
alter table fishways.entrance_position_codes add column name_fr varchar(64);
alter table fishways.entrance_position_codes add column description_fr text;

alter table fishways.fishway_complete_level_codes rename column name to name_en;
alter table fishways.fishway_complete_level_codes rename column description to description_en;
alter table fishways.fishway_complete_level_codes add column name_fr varchar(64);
alter table fishways.fishway_complete_level_codes add column description_fr text;
alter table cabd.nhn_workunit add column name_en varchar(500);
alter table cabd.nhn_workunit add column name_fr varchar(500);

update cabd.nhn_workunit set name_en = sub_sub_drainage_area ;
update cabd.feature_type_metadata set value_options_reference = 'cabd.nhn_workunit;id;name;' where field_name ='nhn_watershed_id';
update cabd.feature_type_metadata set value_options_reference = 'dams.downstream_passage_route_codes;;name;description' where field_name ='down_passage_route' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'waterfalls.waterfall_complete_level_codes;;name;description' where field_name ='complete_level' and view_name = 'cabd.waterfalls_view';
update cabd.feature_type_metadata set value_options_reference = 'waterfalls.waterfall_complete_level_codes;code;name;description' where field_name ='complete_level_code' and view_name = 'cabd.waterfalls_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.dam_complete_level_codes;;name;description' where field_name ='complete_level' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'fishways.fishway_complete_level_codes;;name;description' where field_name ='complete_level' and view_name = 'cabd.fishways_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.lake_control_codes;;name;description' where field_name ='lake_control';
update cabd.feature_type_metadata set value_options_reference = 'dams.function_codes;;name;description' where field_name ='function_name';
update cabd.feature_type_metadata set value_options_reference = 'fishways.entrance_position_codes;;name;description' where field_name ='entrance_position';
update cabd.feature_type_metadata set value_options_reference = 'fishways.entrance_location_codes;;name;description' where field_name ='entrance_location';
update cabd.feature_type_metadata set value_options_reference = 'dams.condition_codes;;name;description' where field_name ='dam_condition';
update cabd.feature_type_metadata set value_options_reference = 'dams.construction_type_codes;;name;description' where field_name ='construction_type';
update cabd.feature_type_metadata set value_options_reference = 'cabd.barrier_ownership_type_codes;;name;description' where field_name ='ownership_type';
update cabd.feature_type_metadata set value_options_reference = 'dams.operating_status_codes;;name;description' where field_name ='operating_status';
update cabd.feature_type_metadata set value_options_reference = 'cabd.passability_status_codes;;name;description' where field_name ='passability_status';
update cabd.feature_type_metadata set value_options_reference = 'cabd.province_territory_codes;;name;' where field_name ='province_territory';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_supply';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_recreation';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_pollution';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_other';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_navigation';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_irrigation';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_invasivespecies';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_floodcontrol';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_fish';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'use_electricity';
update cabd.feature_type_metadata set value_options_reference = 'cabd.upstream_passage_type_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'up_passage_type';
update cabd.feature_type_metadata set value_options_reference = 'cabd.upstream_passage_type_codes;;name;description' where view_name = 'cabd.fishways_view' and field_name = 'fishpass_type';
update cabd.feature_type_metadata set value_options_reference = 'dams.turbine_type_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'turbine_type';
update cabd.feature_type_metadata set value_options_reference = 'dams.spillway_type_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'spillway_type';
update cabd.feature_type_metadata set value_options_reference = 'dams.size_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'size_class';



update fishways.fishway_complete_level_codes set description_fr = description_en, name_fr = name_en ;
update fishways.entrance_position_codes set description_fr = description_en, name_fr = name_en;
update fishways.entrance_location_codes set description_fr = description_en, name_fr = name_en;
update waterfalls.waterfall_complete_level_codes set description_fr = description_en, name_fr = name_en;
update dams.use_codes set description_fr = description_en, name_fr = name_en;
update dams.turbine_type_codes set description_fr = description_en, name_fr = name_en;
update dams.spillway_type_codes set description_fr = description_en, name_fr = name_en;
update dams.size_codes set description_fr = description_en, name_fr = name_en;
--update dams.passability_status_codes set description_fr = description_en, name_fr = name_en;
update dams.operating_status_codes set description_fr = description_en, name_fr = name_en;
update dams.lake_control_codes set description_fr = description_en, name_fr = name_en;
update dams.function_codes set description_fr = description_en, name_fr = name_en;
update dams.downstream_passage_route_codes set description_fr = description_en, name_fr = name_en;
update dams.dam_use_codes set description_fr = description_en, name_fr = name_en;
update dams.dam_complete_level_codes set description_fr = description_en, name_fr = name_en;
update dams.construction_type_codes set description_fr = description_en, name_fr = name_en;
update dams.condition_codes set description_fr = description_en, name_fr = name_en;
update cabd.upstream_passage_type_codes set description_fr = description_en, name_fr = name_en;
update cabd.province_territory_codes set  name_fr = name_en;
update cabd.passability_status_codes set description_fr = description_en, name_fr = name_en;
update cabd.feature_types set name_fr = name_en;
update cabd.feature_type_metadata set name_fr = name_en, description_fr = description_en;
update cabd.barrier_ownership_type_codes set description_fr = description_en, name_fr = name_en;




DROP VIEW cabd.waterfalls_view;

CREATE OR REPLACE VIEW cabd.waterfalls_view_fr
AS SELECT w.cabd_id,
    'features/datasources/'::text || w.cabd_id AS datasource_url,
    'waterfalls'::text AS feature_type,
    st_y(w.snapped_point) AS latitude,
    st_x(w.snapped_point) AS longitude,
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
    w.snapped_point AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code;
     
CREATE OR REPLACE VIEW cabd.waterfalls_view_en
AS SELECT w.cabd_id,
    'features/datasources/'::text || w.cabd_id AS datasource_url,
    'waterfalls'::text AS feature_type,
    st_y(w.snapped_point) AS latitude,
    st_x(w.snapped_point) AS longitude,
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
    w.snapped_point AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code;    
     
     
     
     
     
     
     

DROP VIEW cabd.fishways_view;


CREATE OR REPLACE VIEW cabd.fishways_view_en
AS SELECT d.cabd_id,
    'features/datasources/'::text || d.cabd_id AS datasource_url,
    'fishways'::text AS feature_type,
    st_y(d.original_point) AS latitude,
    st_x(d.original_point) AS longitude,
    d.dam_id,
    d.structure_name_en,
    d.structure_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.river_name_en,
    d.river_name_fr,
    d.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    d.province_territory_code,
    pt.name_en AS province_territory,
    d.municipality,
    d.fishpass_type_code,
    tc.name_en AS fishpass_type,
    d.monitoring_equipment,
    d.architect,
    d.contracted_by,
    d.constructed_by,
    d.plans_held_by,
    d.purpose,
    d.designed_on_biology,
    d.length_m,
    d.elevation_m,
    d.gradient,
    d.depth_m,
    d.entrance_location_code,
    elc.name_en AS entrance_location,
    d.entrance_position_code,
    epc.name_en AS entrance_position,
    d.modified,
    d.modification_year,
    d.modification_purpose,
    d.year_constructed,
    d.operated_by,
    d.operation_period,
    d.has_evaluating_studies,
    d.nature_of_evaluation_studies,
    d.engineering_notes,
    d.operating_notes,
    d.mean_fishway_velocity_ms,
    d.max_fishway_velocity_ms,
    d.estimate_of_attraction_pct,
    d.estimate_of_passage_success_pct,
    d.fishway_reference_id,
    d.complete_level_code,
    cl.name_en AS complete_level,
    sp1.species AS known_use,
    sp2.species AS known_notuse,
    d.original_point AS geometry
   FROM fishways.fishways d
     LEFT JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_watershed_id::text
     LEFT JOIN cabd.upstream_passage_type_codes tc ON tc.code = d.fishpass_type_code
     LEFT JOIN fishways.entrance_location_codes elc ON elc.code = d.entrance_location_code
     LEFT JOIN fishways.entrance_position_codes epc ON epc.code = d.entrance_position_code
     LEFT JOIN fishways.fishway_complete_level_codes cl ON cl.code = d.complete_level_code
     LEFT JOIN ( SELECT a.fishway_id,
            array_agg(b.name) AS species
           FROM fishways.species_mapping a
             JOIN cabd.fish_species b ON a.species_id = b.id
          WHERE a.known_to_use
          GROUP BY a.fishway_id) sp1 ON sp1.fishway_id = d.cabd_id
     LEFT JOIN ( SELECT a.fishway_id,
            array_agg(b.name) AS species
           FROM fishways.species_mapping a
             JOIN cabd.fish_species b ON a.species_id = b.id
          WHERE NOT a.known_to_use
          GROUP BY a.fishway_id) sp2 ON sp2.fishway_id = d.cabd_id;
          
          

CREATE OR REPLACE VIEW cabd.fishways_view_fr
AS SELECT d.cabd_id,
    'features/datasources/'::text || d.cabd_id AS datasource_url,
    'fishways'::text AS feature_type,
    st_y(d.original_point) AS latitude,
    st_x(d.original_point) AS longitude,
    d.dam_id,
    d.structure_name_en,
    d.structure_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.river_name_en,
    d.river_name_fr,
    d.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    d.province_territory_code,
    pt.name_fr AS province_territory,
    d.municipality,
    d.fishpass_type_code,
    tc.name_fr AS fishpass_type,
    d.monitoring_equipment,
    d.architect,
    d.contracted_by,
    d.constructed_by,
    d.plans_held_by,
    d.purpose,
    d.designed_on_biology,
    d.length_m,
    d.elevation_m,
    d.gradient,
    d.depth_m,
    d.entrance_location_code,
    elc.name_fr AS entrance_location,
    d.entrance_position_code,
    epc.name_fr AS entrance_position,
    d.modified,
    d.modification_year,
    d.modification_purpose,
    d.year_constructed,
    d.operated_by,
    d.operation_period,
    d.has_evaluating_studies,
    d.nature_of_evaluation_studies,
    d.engineering_notes,
    d.operating_notes,
    d.mean_fishway_velocity_ms,
    d.max_fishway_velocity_ms,
    d.estimate_of_attraction_pct,
    d.estimate_of_passage_success_pct,
    d.fishway_reference_id,
    d.complete_level_code,
    cl.name_fr AS complete_level,
    sp1.species AS known_use,
    sp2.species AS known_notuse,
    d.original_point AS geometry
   FROM fishways.fishways d
     LEFT JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_watershed_id::text
     LEFT JOIN cabd.upstream_passage_type_codes tc ON tc.code = d.fishpass_type_code
     LEFT JOIN fishways.entrance_location_codes elc ON elc.code = d.entrance_location_code
     LEFT JOIN fishways.entrance_position_codes epc ON epc.code = d.entrance_position_code
     LEFT JOIN fishways.fishway_complete_level_codes cl ON cl.code = d.complete_level_code
     LEFT JOIN ( SELECT a.fishway_id,
            array_agg(b.name) AS species
           FROM fishways.species_mapping a
             JOIN cabd.fish_species b ON a.species_id = b.id
          WHERE a.known_to_use
          GROUP BY a.fishway_id) sp1 ON sp1.fishway_id = d.cabd_id
     LEFT JOIN ( SELECT a.fishway_id,
            array_agg(b.name) AS species
           FROM fishways.species_mapping a
             JOIN cabd.fish_species b ON a.species_id = b.id
          WHERE NOT a.known_to_use
          GROUP BY a.fishway_id) sp2 ON sp2.fishway_id = d.cabd_id;
          
          
          
-- cabd.dams_view source
DROP VIEW cabd.dams_view;


CREATE OR REPLACE VIEW cabd.dams_view_en
AS SELECT d.cabd_id,
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
    d.use_other_code,
    c10.name_en AS use_other,
    d.lake_control_code,
    lk.name_en AS lake_control,
    d.construction_year,
    d.assess_schedule,
    d.expected_life,
    d.maintenance_last,
    d.maintenance_next,
    d.function_code,
    f.name_en AS function_name,
    d.condition_code,
    dc.name_en AS dam_condition,
    d.construction_type_code,
    dct.name_en AS construction_type,
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
     
     
     
CREATE OR REPLACE VIEW cabd.dams_view_fr
AS SELECT d.cabd_id,
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
    d.use_other_code,
    c10.name_fr AS use_other,
    d.lake_control_code,
    lk.name_fr AS lake_control,
    d.construction_year,
    d.assess_schedule,
    d.expected_life,
    d.maintenance_last,
    d.maintenance_next,
    d.function_code,
    f.name_fr AS function_name,
    d.condition_code,
    dc.name_fr AS dam_condition,
    d.construction_type_code,
    dct.name_fr AS construction_type,
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
     
     
     
     
     
     
     
     
     
     
-- cabd.barriers_view source

DROP VIEW cabd.barriers_view;


CREATE OR REPLACE VIEW cabd.barriers_view_en
AS SELECT barriers.cabd_id,
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

     

CREATE OR REPLACE VIEW cabd.barriers_view_fr
AS SELECT barriers.cabd_id,
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
     

DROP VIEW cabd.all_features_view;

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
           FROM fishways.fishways) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;
     
     

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
           FROM fishways.fishways) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;          
                    
grant all privileges on cabd.barriers_view_en to cabd;
grant all privileges on cabd.barriers_view_fr to cabd;   
grant all privileges on cabd.all_features_view_en to cabd;
grant all privileges on cabd.all_features_view_fr to cabd;
GRANT ALL PRIVILEGES ON cabd.dams_view_en to cabd;
GRANT ALL PRIVILEGES ON cabd.dams_view_fr to cabd;
grant all privileges on cabd.fishways_view_en to cabd;
grant all privileges on cabd.fishways_view_fr to cabd;     
grant all privileges on cabd.waterfalls_view_en to cabd;
grant all privileges on cabd.waterfalls_view_fr to cabd;
          
          
truncate cabd.vector_tile_cache;


-- NHN work unit updates
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Saint-Jean' WHERE id = '01AA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saint-Jean - Big Black' WHERE id = '01AB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saint-Jean - St. Francis' WHERE id = '01AD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saint-Jean - Verte' WHERE id = '01AF000';
update cabd.nhn_workunit set name_fr = 'Aroostook' WHERE id = '01AG000';
update cabd.nhn_workunit set name_fr = 'Tobique' WHERE id = '01AH000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saint-Jean - Becaguimec' WHERE id = '01AJA00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saint-Jean - Becaguimec' WHERE id = '01AJB00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saint-Jean - Keswick' WHERE id = '01AK000';
update cabd.nhn_workunit set name_fr = 'Nashwaak' WHERE id = '01AL000';
update cabd.nhn_workunit set name_fr = 'Oromocto' WHERE id = '01AM000';
update cabd.nhn_workunit set name_fr = 'Salmon (N.-B.)' WHERE id = '01AN000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saint-Jean - Grand Lac' WHERE id = '01AO000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saint-Jean - Kennebecasis' WHERE id = '01AP000';
update cabd.nhn_workunit set name_fr = 'Magaguadavic' WHERE id = '01AQ000';
update cabd.nhn_workunit set name_fr = 'St. Croix' WHERE id = '01AR000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Ristigouche' WHERE id = '01BA000';
update cabd.nhn_workunit set name_fr = 'Kedgwick' WHERE id = '01BB000';
update cabd.nhn_workunit set name_fr = 'Kedgwick' WHERE id = '01BB002';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Ristigouche' WHERE id = '01BC000';
update cabd.nhn_workunit set name_fr = 'Matapédia' WHERE id = '01BD000';
update cabd.nhn_workunit set name_fr = 'Upsalquitch' WHERE id = '01BE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Ristigouche - Nouvelle' WHERE id = '01BF001';
update cabd.nhn_workunit set name_fr = 'Baie des Chaleurs - Nord-ouest' WHERE id = '01BG001';
update cabd.nhn_workunit set name_fr = 'Baie des Chaleurs - Nord-ouest' WHERE id = '01BG002';
update cabd.nhn_workunit set name_fr = 'Baie des Chaleurs - Nord-ouest' WHERE id = '01BG003';
update cabd.nhn_workunit set name_fr = 'Baie des Chaleurs - Golfe du Saint-Laurent' WHERE id = '01BH001';
update cabd.nhn_workunit set name_fr = 'Baie des Chaleurs - Golfe du Saint-Laurent' WHERE id = '01BH002';
update cabd.nhn_workunit set name_fr = 'Baie des Chaleurs - Sud-ouest' WHERE id = '01BJ001';
update cabd.nhn_workunit set name_fr = 'Nepisiguit' WHERE id = '01BK000';
update cabd.nhn_workunit set name_fr = 'Péninsule acadienne' WHERE id = '01BL000';
update cabd.nhn_workunit set name_fr = 'Miramichi Sud-Ouest - Cours supérieur' WHERE id = '01BM000';
update cabd.nhn_workunit set name_fr = 'Miramichi Sud-Ouest - Cours moyen' WHERE id = '01BN000';
update cabd.nhn_workunit set name_fr = 'Miramichi - Embouchure' WHERE id = '01BO000';
update cabd.nhn_workunit set name_fr = 'Petite Miramichi Sud-Ouest' WHERE id = '01BP000';
update cabd.nhn_workunit set name_fr = 'Miramichi Nord-Ouest' WHERE id = '01BQ000';
update cabd.nhn_workunit set name_fr = 'Kouchibouguac' WHERE id = '01BR000';
update cabd.nhn_workunit set name_fr = 'Richibucto' WHERE id = '01BS000';
update cabd.nhn_workunit set name_fr = 'Kinnear' WHERE id = '01BT000';
update cabd.nhn_workunit set name_fr = 'Petitcodiac' WHERE id = '01BU000';
update cabd.nhn_workunit set name_fr = 'Point Wolfe' WHERE id = '01BV000';
update cabd.nhn_workunit set name_fr = 'Ouest de l''Île-du-Prince-Édouard' WHERE id = '01CA000';
update cabd.nhn_workunit set name_fr = 'Centre de l''Île-du-Prince-Édouard - Wilmot' WHERE id = '01CB000';
update cabd.nhn_workunit set name_fr = 'Centre de l''Île-du-Prince-Édouard - Hillsborough' WHERE id = '01CC000';
update cabd.nhn_workunit set name_fr = 'Nord-est de l''Île-du-Prince-Édouard' WHERE id = '01CD000';
update cabd.nhn_workunit set name_fr = 'Sud-est de l''Île-du-Prince-Édouard' WHERE id = '01CE000';
update cabd.nhn_workunit set name_fr = 'Meteghan' WHERE id = '01DA000';
update cabd.nhn_workunit set name_fr = 'Sissiboo' WHERE id = '01DB000';
update cabd.nhn_workunit set name_fr = 'Annapolis' WHERE id = '01DC000';
update cabd.nhn_workunit set name_fr = 'Gaspereau' WHERE id = '01DD000';
update cabd.nhn_workunit set name_fr = 'Avon' WHERE id = '01DE000';
update cabd.nhn_workunit set name_fr = 'Kennetcook' WHERE id = '01DF000';
update cabd.nhn_workunit set name_fr = 'Shubenacadie' WHERE id = '01DG000';
update cabd.nhn_workunit set name_fr = 'Salmon (N.-É)' WHERE id = '01DH000';
update cabd.nhn_workunit set name_fr = 'Economy' WHERE id = '01DJ000';
update cabd.nhn_workunit set name_fr = 'Chenal Minas - Parrsboro' WHERE id = '01DK000';
update cabd.nhn_workunit set name_fr = 'Maccan' WHERE id = '01DL000';
update cabd.nhn_workunit set name_fr = 'Tidnish' WHERE id = '01DM000';
update cabd.nhn_workunit set name_fr = 'Philip' WHERE id = '01DN000';
update cabd.nhn_workunit set name_fr = 'John' WHERE id = '01DO000';
update cabd.nhn_workunit set name_fr = 'Rivière de Pictou Est et rivière de Pictou Ouest' WHERE id = '01DP000';
update cabd.nhn_workunit set name_fr = 'French (N.-É.)' WHERE id = '01DQ000';
update cabd.nhn_workunit set name_fr = 'Antigonish Sud et Antigonish Ouest' WHERE id = '01DR000';
update cabd.nhn_workunit set name_fr = 'Tracadie' WHERE id = '01DS000';
update cabd.nhn_workunit set name_fr = 'Tusket' WHERE id = '01EA000';
update cabd.nhn_workunit set name_fr = 'Clyde' WHERE id = '01EB000';
update cabd.nhn_workunit set name_fr = 'Roseway' WHERE id = '01EC000';
update cabd.nhn_workunit set name_fr = 'Mersey' WHERE id = '01ED000';
update cabd.nhn_workunit set name_fr = 'Medway' WHERE id = '01EE000';
update cabd.nhn_workunit set name_fr = 'LaHave' WHERE id = '01EF000';
update cabd.nhn_workunit set name_fr = 'Gold' WHERE id = '01EG000';
update cabd.nhn_workunit set name_fr = 'Baie St. Margarets' WHERE id = '01EH000';
update cabd.nhn_workunit set name_fr = 'Sackville' WHERE id = '01EJ000';
update cabd.nhn_workunit set name_fr = 'Musquodoboit' WHERE id = '01EK000';
update cabd.nhn_workunit set name_fr = 'Tangier' WHERE id = '01EL000';
update cabd.nhn_workunit set name_fr = 'Rivière Sheet Harbour Est et rivière Sheet Harbour Ouest' WHERE id = '01EM000';
update cabd.nhn_workunit set name_fr = 'Liscomb' WHERE id = '01EN000';
update cabd.nhn_workunit set name_fr = 'St. Marys' WHERE id = '01EO000';
update cabd.nhn_workunit set name_fr = 'Country Harbour' WHERE id = '01EP000';
update cabd.nhn_workunit set name_fr = 'New Harbour' WHERE id = '01EQ000';
update cabd.nhn_workunit set name_fr = 'St. Francis Harbour' WHERE id = '01ER000';
update cabd.nhn_workunit set name_fr = 'Inhabitants' WHERE id = '01FA000';
update cabd.nhn_workunit set name_fr = 'Margaree' WHERE id = '01FB000';
update cabd.nhn_workunit set name_fr = 'Chéticamp' WHERE id = '01FC000';
update cabd.nhn_workunit set name_fr = 'Wreck Cove' WHERE id = '01FD000';
update cabd.nhn_workunit set name_fr = 'Indian' WHERE id = '01FE000';
update cabd.nhn_workunit set name_fr = 'Baddeck' WHERE id = '01FF000';
update cabd.nhn_workunit set name_fr = 'Denys' WHERE id = '01FG000';
update cabd.nhn_workunit set name_fr = 'Grand' WHERE id = '01FH000';
update cabd.nhn_workunit set name_fr = 'Mira' WHERE id = '01FJ000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '01GA000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '01HA000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '01HB000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '01HD000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '01KA000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0201000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0203000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0207000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0209000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0210001';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0210002';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0210003';
update cabd.nhn_workunit set name_fr = 'Pigeon' WHERE id = '02AA000';
update cabd.nhn_workunit set name_fr = 'Dog' WHERE id = '02AB000';
update cabd.nhn_workunit set name_fr = 'Black Sturgeon' WHERE id = '02AC000';
update cabd.nhn_workunit set name_fr = 'Nipigon' WHERE id = '02AD000';
update cabd.nhn_workunit set name_fr = 'Jackpine' WHERE id = '02AE000';
update cabd.nhn_workunit set name_fr = 'Petite rivière Pic' WHERE id = '02BA000';
update cabd.nhn_workunit set name_fr = 'Pic' WHERE id = '02BB000';
update cabd.nhn_workunit set name_fr = 'White' WHERE id = '02BC000';
update cabd.nhn_workunit set name_fr = 'Michipicoten - Magpie' WHERE id = '02BD000';
update cabd.nhn_workunit set name_fr = 'Agawa' WHERE id = '02BE000';
update cabd.nhn_workunit set name_fr = 'Goulais' WHERE id = '02BF000';
update cabd.nhn_workunit set name_fr = 'Garden' WHERE id = '02CAA00';
update cabd.nhn_workunit set name_fr = 'Garden' WHERE id = '02CAB00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Mississagi' WHERE id = '02CB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Mississagi' WHERE id = '02CC000';
update cabd.nhn_workunit set name_fr = 'Serpent' WHERE id = '02CD000';
update cabd.nhn_workunit set name_fr = 'Spanish' WHERE id = '02CE000';
update cabd.nhn_workunit set name_fr = 'Vermilion (Ont.)' WHERE id = '02CF000';
update cabd.nhn_workunit set name_fr = 'Killarney' WHERE id = '02CH000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Wanapitei' WHERE id = '02DA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Wanapitei' WHERE id = '02DB000';
update cabd.nhn_workunit set name_fr = 'Sturgeon (Ont.)' WHERE id = '02DC000';
update cabd.nhn_workunit set name_fr = 'French (Ont.)' WHERE id = '02DD000';
update cabd.nhn_workunit set name_fr = 'Magnetawan' WHERE id = '02EA000';
update cabd.nhn_workunit set name_fr = 'Muskoka' WHERE id = '02EB000';
update cabd.nhn_workunit set name_fr = 'Severn - Lac Simcoe' WHERE id = '02EC000';
update cabd.nhn_workunit set name_fr = 'Nottawasaga' WHERE id = '02ED000';
update cabd.nhn_workunit set name_fr = 'Péninsule Bruce' WHERE id = '02FA000';
update cabd.nhn_workunit set name_fr = 'Sud-ouest de la baie Georgienne' WHERE id = '02FB000';
update cabd.nhn_workunit set name_fr = 'Saugeen' WHERE id = '02FC000';
update cabd.nhn_workunit set name_fr = 'Penetangore' WHERE id = '02FD000';
update cabd.nhn_workunit set name_fr = 'Maitland' WHERE id = '02FE000';
update cabd.nhn_workunit set name_fr = 'Ausable' WHERE id = '02FF000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Grande' WHERE id = '02GA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Grande' WHERE id = '02GB000';
update cabd.nhn_workunit set name_fr = 'Big (Ont.)' WHERE id = '02GC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Thames' WHERE id = '02GD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Thames' WHERE id = '02GE000';
update cabd.nhn_workunit set name_fr = 'Rondeau' WHERE id = '02GF000';
update cabd.nhn_workunit set name_fr = 'Sydenham' WHERE id = '02GGA00';
update cabd.nhn_workunit set name_fr = 'Sydenham' WHERE id = '02GGB00';
update cabd.nhn_workunit set name_fr = 'Cedar' WHERE id = '02GHA00';
update cabd.nhn_workunit set name_fr = 'Cedar' WHERE id = '02GHB00';
update cabd.nhn_workunit set name_fr = 'Niagara' WHERE id = '02HAA00';
update cabd.nhn_workunit set name_fr = 'Niagara' WHERE id = '02HAC00';
update cabd.nhn_workunit set name_fr = 'Niagara' WHERE id = '02HAD00';
update cabd.nhn_workunit set name_fr = 'Credit - Sixteen Mile' WHERE id = '02HB000';
update cabd.nhn_workunit set name_fr = 'Humber - Don' WHERE id = '02HC000';
update cabd.nhn_workunit set name_fr = 'Ganaraska' WHERE id = '02HD000';
update cabd.nhn_workunit set name_fr = 'Gull' WHERE id = '02HF000';
update cabd.nhn_workunit set name_fr = 'Scugog' WHERE id = '02HG000';
update cabd.nhn_workunit set name_fr = 'Lacs Kawartha' WHERE id = '02HH000';
update cabd.nhn_workunit set name_fr = 'Otonabee' WHERE id = '02HJ000';
update cabd.nhn_workunit set name_fr = 'Trent' WHERE id = '02HKA00';
update cabd.nhn_workunit set name_fr = 'Trent' WHERE id = '02HKB00';
update cabd.nhn_workunit set name_fr = 'Moira' WHERE id = '02HL000';
update cabd.nhn_workunit set name_fr = 'Napanee' WHERE id = '02HM000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière des Outaouais - Eaux d''amont' WHERE id = '02JA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière des Outaouais - Kinojévis' WHERE id = '02JB000';
update cabd.nhn_workunit set name_fr = 'Blanche' WHERE id = '02JC000';
update cabd.nhn_workunit set name_fr = 'Montreal (Ont.)' WHERE id = '02JD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière des Outaouais - Kipawa' WHERE id = '02JE000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière des Outaouais - Dumoine' WHERE id = '02KA000';
update cabd.nhn_workunit set name_fr = 'Petawawa' WHERE id = '02KB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière des Outaouais - Bonnechere' WHERE id = '02KC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Madawaska' WHERE id = '02KD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Madawaska' WHERE id = '02KE000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière des Outaouais - Mississippi' WHERE id = '02KF000';
update cabd.nhn_workunit set name_fr = 'Coulonge' WHERE id = '02KG001';
update cabd.nhn_workunit set name_fr = 'Noire' WHERE id = '02KH001';
update cabd.nhn_workunit set name_fr = 'Dumoine' WHERE id = '02KJ001';
update cabd.nhn_workunit set name_fr = 'Rideau' WHERE id = '02LA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière des Outaouais - South Nation' WHERE id = '02LB001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière des Outaouais - South Nation' WHERE id = '02LB003';
update cabd.nhn_workunit set name_fr = 'Rouge et Nord' WHERE id = '02LC001';
update cabd.nhn_workunit set name_fr = 'Petite Nation' WHERE id = '02LD001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière du Lièvre' WHERE id = '02LE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière du Lièvre' WHERE id = '02LF001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Gatineau' WHERE id = '02LG000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Gatineau' WHERE id = '02LH001';
update cabd.nhn_workunit set name_fr = 'Cataraqui' WHERE id = '02MA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Saint-Laurent - Raisin' WHERE id = '02MCA00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Saint-Laurent - Raisin' WHERE id = '02MCD00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Saint-Laurent - Raisin' WHERE id = '02MCD01';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Saint-Laurent - Raisin' WHERE id = '02MCE00';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Saint-Maurice' WHERE id = '02NA000';
update cabd.nhn_workunit set name_fr = 'Manouane' WHERE id = '02NB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saint-Maurice' WHERE id = '02NC000';
update cabd.nhn_workunit set name_fr = 'Vermillon (Qc)' WHERE id = '02ND000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saint-Maurice' WHERE id = '02NE000';
update cabd.nhn_workunit set name_fr = 'Matawin' WHERE id = '02NF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saint-Maurice' WHERE id = '02NG000';
update cabd.nhn_workunit set name_fr = 'Île de Montréal' WHERE id = '02OAA01';
update cabd.nhn_workunit set name_fr = 'Île de Montréal' WHERE id = '02OAB00';
update cabd.nhn_workunit set name_fr = 'L''Assomption' WHERE id = '02OB000';
update cabd.nhn_workunit set name_fr = 'Loup' WHERE id = '02OC000';
update cabd.nhn_workunit set name_fr = 'Nicolet' WHERE id = '02OD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saint-François' WHERE id = '02OE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saint-François' WHERE id = '02OF000';
update cabd.nhn_workunit set name_fr = 'Yamaska' WHERE id = '02OG000';
update cabd.nhn_workunit set name_fr = 'Lac Champlain' WHERE id = '02OHA00';
update cabd.nhn_workunit set name_fr = 'Lac Champlain' WHERE id = '02OHB00';
update cabd.nhn_workunit set name_fr = 'Richelieu' WHERE id = '02OJ000';
update cabd.nhn_workunit set name_fr = 'Batiscan' WHERE id = '02PA000';
update cabd.nhn_workunit set name_fr = 'Sainte-Anne' WHERE id = '02PB000';
update cabd.nhn_workunit set name_fr = 'Jacque-Cartier' WHERE id = '02PC000';
update cabd.nhn_workunit set name_fr = 'Montmorency' WHERE id = '02PD001';
update cabd.nhn_workunit set name_fr = 'Montmorency' WHERE id = '02PD002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Saint-Laurent - Gouffre' WHERE id = '02PE001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Saint-Laurent - Gouffre' WHERE id = '02PE002';
update cabd.nhn_workunit set name_fr = 'Malbaie' WHERE id = '02PF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Saint-Laurent - Loup' WHERE id = '02PG001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Saint-Laurent - Loup' WHERE id = '02PG002';
update cabd.nhn_workunit set name_fr = 'Etchemin' WHERE id = '02PH001';
update cabd.nhn_workunit set name_fr = 'Etchemin' WHERE id = '02PH002';
update cabd.nhn_workunit set name_fr = 'Chaudière' WHERE id = '02PJ000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Saint-Laurent - Chêne' WHERE id = '02PK001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Saint-Laurent - Chêne' WHERE id = '02PK002';
update cabd.nhn_workunit set name_fr = 'Bécancour' WHERE id = '02PL000';
update cabd.nhn_workunit set name_fr = 'Rimouski' WHERE id = '02QA001';
update cabd.nhn_workunit set name_fr = 'Rimouski' WHERE id = '02QA002';
update cabd.nhn_workunit set name_fr = 'Rimouski' WHERE id = '02QA003';
update cabd.nhn_workunit set name_fr = 'Matane' WHERE id = '02QB001';
update cabd.nhn_workunit set name_fr = 'Matane' WHERE id = '02QB002';
update cabd.nhn_workunit set name_fr = 'Matane' WHERE id = '02QB003';
update cabd.nhn_workunit set name_fr = 'Madeleine' WHERE id = '02QC001';
update cabd.nhn_workunit set name_fr = 'Madeleine' WHERE id = '02QC002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Péribonca' WHERE id = '02RA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Péribonca' WHERE id = '02RB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Péribonca' WHERE id = '02RC000';
update cabd.nhn_workunit set name_fr = 'Lac Saint-Jean - Mistassini' WHERE id = '02RD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Mistassini' WHERE id = '02RE000';
update cabd.nhn_workunit set name_fr = 'Chamouchouane' WHERE id = '02RF000';
update cabd.nhn_workunit set name_fr = 'Lac Saint-Jean' WHERE id = '02RG001';
update cabd.nhn_workunit set name_fr = 'Lac Saint-Jean' WHERE id = '02RG002';
update cabd.nhn_workunit set name_fr = 'Saguenay - Embouchure' WHERE id = '02RH001';
update cabd.nhn_workunit set name_fr = 'Saguenay - Embouchure' WHERE id = '02RH002';
update cabd.nhn_workunit set name_fr = 'Saguenay - Embouchure' WHERE id = '02RH003';
update cabd.nhn_workunit set name_fr = 'Saguenay - Embouchure' WHERE id = '02RH004';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Betsiamites' WHERE id = '02SA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Betsiamites' WHERE id = '02SB000';
update cabd.nhn_workunit set name_fr = 'Portneuf' WHERE id = '02SC001';
update cabd.nhn_workunit set name_fr = 'Portneuf' WHERE id = '02SC002';
update cabd.nhn_workunit set name_fr = 'Mouchalagane' WHERE id = '02TA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Manicouagan' WHERE id = '02TB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Manicouagan' WHERE id = '02TC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière aux Outardes' WHERE id = '02TD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière aux Outardes' WHERE id = '02TE000';
update cabd.nhn_workunit set name_fr = 'Pentecôte' WHERE id = '02UA000';
update cabd.nhn_workunit set name_fr = 'Sainte-Marguerite' WHERE id = '02UB000';
update cabd.nhn_workunit set name_fr = 'Moisie - Côte' WHERE id = '02UC000';
update cabd.nhn_workunit set name_fr = 'Manitou' WHERE id = '02VA000';
update cabd.nhn_workunit set name_fr = 'Magpie' WHERE id = '02VB000';
update cabd.nhn_workunit set name_fr = 'Romaine' WHERE id = '02VC000';
update cabd.nhn_workunit set name_fr = 'Aguanus' WHERE id = '02WA000';
update cabd.nhn_workunit set name_fr = 'Natashquan' WHERE id = '02WB000';
update cabd.nhn_workunit set name_fr = 'Olomane' WHERE id = '02WC000';
update cabd.nhn_workunit set name_fr = 'Île d''Anticosti' WHERE id = '02WD000';
update cabd.nhn_workunit set name_fr = 'Îles de la Madeleine' WHERE id = '02WE000';
update cabd.nhn_workunit set name_fr = 'Petit Mécatina - Côte' WHERE id = '02XA000';
update cabd.nhn_workunit set name_fr = 'St-Augustin - Côte' WHERE id = '02XB000';
update cabd.nhn_workunit set name_fr = 'Saint-Paul - Côte' WHERE id = '02XC000';
update cabd.nhn_workunit set name_fr = 'Détroit de Belle Isle - Rive nord' WHERE id = '02XD000';
update cabd.nhn_workunit set name_fr = 'Détroit de Belle Isle - Rive sud' WHERE id = '02YA000';
update cabd.nhn_workunit set name_fr = 'Baie Hare' WHERE id = '02YB000';
update cabd.nhn_workunit set name_fr = 'Baie St. John' WHERE id = '02YC000';
update cabd.nhn_workunit set name_fr = 'Baie Canada' WHERE id = '02YD000';
update cabd.nhn_workunit set name_fr = 'Portland Creek Pond' WHERE id = '02YE000';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie White' WHERE id = '02YF000';
update cabd.nhn_workunit set name_fr = 'Sud et est de la baie White' WHERE id = '02YG000';
update cabd.nhn_workunit set name_fr = 'Baie Bonne' WHERE id = '02YH000';
update cabd.nhn_workunit set name_fr = 'Baie Port au Port' WHERE id = '02YJ000';
update cabd.nhn_workunit set name_fr = 'Humber - Cours supérieur' WHERE id = '02YK000';
update cabd.nhn_workunit set name_fr = 'Humber - Cours inférieur' WHERE id = '02YL000';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie Notre Dame' WHERE id = '02YM000';
update cabd.nhn_workunit set name_fr = 'Lac Red Indian' WHERE id = '02YN000';
update cabd.nhn_workunit set name_fr = 'Exploits' WHERE id = '02YO000';
update cabd.nhn_workunit set name_fr = 'Sud-ouest de la baie Notre Dame' WHERE id = '02YP000';
update cabd.nhn_workunit set name_fr = 'Gander' WHERE id = '02YQ000';
update cabd.nhn_workunit set name_fr = 'Nord-ouest de la baie Bonavista' WHERE id = '02YR000';
update cabd.nhn_workunit set name_fr = 'Sud et est de la baie Bonavista' WHERE id = '02YS000';
update cabd.nhn_workunit set name_fr = 'Baie St. George''s' WHERE id = '02ZA000';
update cabd.nhn_workunit set name_fr = 'Baie La Poile' WHERE id = '02ZB000';
update cabd.nhn_workunit set name_fr = 'Baie White Bear' WHERE id = '02ZC000';
update cabd.nhn_workunit set name_fr = 'Grey' WHERE id = '02ZD000';
update cabd.nhn_workunit set name_fr = 'Baie Hermitage' WHERE id = '02ZE000';
update cabd.nhn_workunit set name_fr = 'Baie Fortune' WHERE id = '02ZF000';
update cabd.nhn_workunit set name_fr = 'Sud-ouest de la baie Placentia' WHERE id = '02ZG000';
update cabd.nhn_workunit set name_fr = 'Nord-ouest de la baie Placentia' WHERE id = '02ZH000';
update cabd.nhn_workunit set name_fr = 'Baie Trinity' WHERE id = '02ZJ000';
update cabd.nhn_workunit set name_fr = 'Est de la baie Placentia et ouest de la baie St. Mary''s' WHERE id = '02ZK000';
update cabd.nhn_workunit set name_fr = 'Baie Conception' WHERE id = '02ZL000';
update cabd.nhn_workunit set name_fr = 'Côte est de Terre-Neuve' WHERE id = '02ZM000';
update cabd.nhn_workunit set name_fr = 'Est de la baie St. Mary''s' WHERE id = '02ZN000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Waswanipi' WHERE id = '03AA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Nottaway' WHERE id = '03AB000';
update cabd.nhn_workunit set name_fr = 'Bell' WHERE id = '03AC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Nottaway' WHERE id = '03AD000';
update cabd.nhn_workunit set name_fr = 'Lac Mistassini' WHERE id = '03BA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Rupert' WHERE id = '03BB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Rupert' WHERE id = '03BC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Broadback' WHERE id = '03BD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Broadback' WHERE id = '03BE000';
update cabd.nhn_workunit set name_fr = 'Pontax' WHERE id = '03BF000';
update cabd.nhn_workunit set name_fr = 'Jolicoeur' WHERE id = '03BG000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Eastmain' WHERE id = '03CA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Eastmain' WHERE id = '03CB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Eastmain' WHERE id = '03CC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Opinaca' WHERE id = '03CD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Opinaca' WHERE id = '03CE000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière La Grande' WHERE id = '03DA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière La Grande' WHERE id = '03DA002';
update cabd.nhn_workunit set name_fr = 'Kanaaupscow' WHERE id = '03DB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière La Grande' WHERE id = '03DC001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière La Grande' WHERE id = '03DC002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Sakami' WHERE id = '03DD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Sakami' WHERE id = '03DE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière La Grande' WHERE id = '03DF000';
update cabd.nhn_workunit set name_fr = 'Castor' WHERE id = '03DG000';
update cabd.nhn_workunit set name_fr = 'Vieux Comptoir' WHERE id = '03DH000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Grande rivière de la Baleine' WHERE id = '03EA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Grande rivière de la Baleine' WHERE id = '03EA002';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Grande rivière de la Baleine' WHERE id = '03EB000';
update cabd.nhn_workunit set name_fr = 'Denys' WHERE id = '03EC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Grande rivière de la Baleine' WHERE id = '03ED000';
update cabd.nhn_workunit set name_fr = 'Roggan' WHERE id = '03EE000';
update cabd.nhn_workunit set name_fr = 'Manitounuk' WHERE id = '03EF000';
update cabd.nhn_workunit set name_fr = 'Nastapoca' WHERE id = '03FA001';
update cabd.nhn_workunit set name_fr = 'Nastapoca' WHERE id = '03FA002';
update cabd.nhn_workunit set name_fr = 'Eau Claire' WHERE id = '03FB001';
update cabd.nhn_workunit set name_fr = 'Eau Claire' WHERE id = '03FB002';
update cabd.nhn_workunit set name_fr = 'Petite rivière de la Baleine' WHERE id = '03FC001';
update cabd.nhn_workunit set name_fr = 'Petite rivière de la Baleine' WHERE id = '03FC002';
update cabd.nhn_workunit set name_fr = 'Kikkerteluc' WHERE id = '03GA001';
update cabd.nhn_workunit set name_fr = 'Kikkerteluc' WHERE id = '03GA002';
update cabd.nhn_workunit set name_fr = 'Innuksuac' WHERE id = '03GB001';
update cabd.nhn_workunit set name_fr = 'Innuksuac' WHERE id = '03GB002';
update cabd.nhn_workunit set name_fr = 'Innuksuac' WHERE id = '03GB003';
update cabd.nhn_workunit set name_fr = 'Kogaluc' WHERE id = '03GC001';
update cabd.nhn_workunit set name_fr = 'Kogaluc' WHERE id = '03GC002';
update cabd.nhn_workunit set name_fr = 'Kogaluc' WHERE id = '03GC003';
update cabd.nhn_workunit set name_fr = 'Kogaluc' WHERE id = '03GC004';
update cabd.nhn_workunit set name_fr = 'Povungnituk' WHERE id = '03GD001';
update cabd.nhn_workunit set name_fr = 'Povungnituk' WHERE id = '03GD002';
update cabd.nhn_workunit set name_fr = 'Povungnituk' WHERE id = '03GD003';
update cabd.nhn_workunit set name_fr = 'Povungnituk' WHERE id = '03GD004';
update cabd.nhn_workunit set name_fr = 'Kovic' WHERE id = '03GE001';
update cabd.nhn_workunit set name_fr = 'Kovic' WHERE id = '03GE002';
update cabd.nhn_workunit set name_fr = 'Kovic' WHERE id = '03GE003';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Arnaud' WHERE id = '03HA001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Arnaud' WHERE id = '03HA002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Arnaud' WHERE id = '03HA003';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Arnaud' WHERE id = '03HA004';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Arnaud' WHERE id = '03HA005';
update cabd.nhn_workunit set name_fr = 'Lepelle' WHERE id = '03HB000';
update cabd.nhn_workunit set name_fr = 'Rive sud du détroit d''Hudson' WHERE id = '03HC001';
update cabd.nhn_workunit set name_fr = 'Rive sud du détroit d''Hudson' WHERE id = '03HC002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière aux Feuilles' WHERE id = '03JA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière aux Feuilles' WHERE id = '03JA002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière aux Feuilles' WHERE id = '03JA003';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière aux Feuilles' WHERE id = '03JA004';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière aux Feuilles' WHERE id = '03JA005';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière aux Feuilles' WHERE id = '03JB001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière aux Feuilles' WHERE id = '03JB002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière aux Feuilles' WHERE id = '03JB003';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière aux Feuilles' WHERE id = '03JB004';
update cabd.nhn_workunit set name_fr = 'Baie aux Feuilles' WHERE id = '03JC001';
update cabd.nhn_workunit set name_fr = 'Baie aux Feuilles' WHERE id = '03JC002';
update cabd.nhn_workunit set name_fr = 'Rive ouest de la baie d''Ungava' WHERE id = '03JD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière aux Mélèzes' WHERE id = '03KA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière aux Mélèzes' WHERE id = '03KA002';
update cabd.nhn_workunit set name_fr = 'Gué' WHERE id = '03KB001';
update cabd.nhn_workunit set name_fr = 'Gué' WHERE id = '03KB002';
update cabd.nhn_workunit set name_fr = 'Gué' WHERE id = '03KB003';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière aux Mélèzes' WHERE id = '03KC000';
update cabd.nhn_workunit set name_fr = 'Lefebvre' WHERE id = '03KD000';
update cabd.nhn_workunit set name_fr = 'Koksoak - Embouchure' WHERE id = '03KE000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Caniapiscau' WHERE id = '03LA001';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Caniapiscau' WHERE id = '03LA002';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Caniapiscau' WHERE id = '03LA003';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Caniapiscau' WHERE id = '03LB000';
update cabd.nhn_workunit set name_fr = 'Sable' WHERE id = '03LC000';
update cabd.nhn_workunit set name_fr = 'Swampy Bay' WHERE id = '03LD000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Caniapiscau' WHERE id = '03LE001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Caniapiscau' WHERE id = '03LE002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Caniapiscau' WHERE id = '03LF000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière à la Baleine' WHERE id = '03MA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière à la Baleine' WHERE id = '03MA002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière à la Baleine' WHERE id = '03MB000';
update cabd.nhn_workunit set name_fr = 'Tunulic' WHERE id = '03MC001';
update cabd.nhn_workunit set name_fr = 'Tunulic' WHERE id = '03MC002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la George' WHERE id = '03MD001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la George' WHERE id = '03MD002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la George' WHERE id = '03MD003';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la George' WHERE id = '03ME000';
update cabd.nhn_workunit set name_fr = 'Koroc' WHERE id = '03MF001';
update cabd.nhn_workunit set name_fr = 'Koroc' WHERE id = '03MF002';
update cabd.nhn_workunit set name_fr = 'Labrador - Northern' WHERE id = '03NA000';
update cabd.nhn_workunit set name_fr = 'Saglek et Hebron' WHERE id = '03NB000';
update cabd.nhn_workunit set name_fr = 'Baie Okuk' WHERE id = '03NC000';
update cabd.nhn_workunit set name_fr = 'Tikkoatokuk' WHERE id = '03ND000';
update cabd.nhn_workunit set name_fr = 'Kogaluk et Notakwanon' WHERE id = '03NE001';
update cabd.nhn_workunit set name_fr = 'Kogaluk et Notakwanon' WHERE id = '03NE002';
update cabd.nhn_workunit set name_fr = 'Adlatok' WHERE id = '03NF000';
update cabd.nhn_workunit set name_fr = 'Kanairiktok' WHERE id = '03NG001';
update cabd.nhn_workunit set name_fr = 'Kanairiktok' WHERE id = '03NG002';
update cabd.nhn_workunit set name_fr = 'Big (T.-N.-L.)' WHERE id = '03NH000';
update cabd.nhn_workunit set name_fr = 'Ashuanipi' WHERE id = '03OA001';
update cabd.nhn_workunit set name_fr = 'Ashuanipi' WHERE id = '03OA002';
update cabd.nhn_workunit set name_fr = 'Réservoir Smallwood' WHERE id = '03OB001';
update cabd.nhn_workunit set name_fr = 'Réservoir Smallwood' WHERE id = '03OB002';
update cabd.nhn_workunit set name_fr = 'Atikosak et cours supérieur du Churchill (T.-N.-L.)' WHERE id = '03OC001';
update cabd.nhn_workunit set name_fr = 'Atikosak et cours supérieur du Churchill (T.-N.-L.)' WHERE id = '03OC002';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Churchill (T.-N.-L.)' WHERE id = '03OD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Churchill (T.-N.-L.)' WHERE id = '03OE000';
update cabd.nhn_workunit set name_fr = 'Naskaupi' WHERE id = '03PA000';
update cabd.nhn_workunit set name_fr = 'Lac Grand' WHERE id = '03PB000';
update cabd.nhn_workunit set name_fr = 'Baie Goose' WHERE id = '03PC000';
update cabd.nhn_workunit set name_fr = 'Lac Melville' WHERE id = '03PD000';
update cabd.nhn_workunit set name_fr = 'Inlet Hamilton' WHERE id = '03PE000';
update cabd.nhn_workunit set name_fr = 'Eagle - Paradise' WHERE id = '03QA000';
update cabd.nhn_workunit set name_fr = 'Alexis' WHERE id = '03QB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Hayes (Man.)' WHERE id = '04AA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Hayes (Man.)' WHERE id = '04AB000';
update cabd.nhn_workunit set name_fr = 'Lac Gods' WHERE id = '04AC000';
update cabd.nhn_workunit set name_fr = 'Gods' WHERE id = '04AD000';
update cabd.nhn_workunit set name_fr = 'Echoing' WHERE id = '04AE000';
update cabd.nhn_workunit set name_fr = 'Fox' WHERE id = '04AF000';
update cabd.nhn_workunit set name_fr = 'Kaskattama' WHERE id = '04BA000';
update cabd.nhn_workunit set name_fr = 'Black Duck' WHERE id = '04BB000';
update cabd.nhn_workunit set name_fr = 'Niskibi' WHERE id = '04BC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Severn' WHERE id = '04CA000';
update cabd.nhn_workunit set name_fr = 'Windigo' WHERE id = '04CB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Severn' WHERE id = '04CC000';
update cabd.nhn_workunit set name_fr = 'Sachigo' WHERE id = '04CD000';
update cabd.nhn_workunit set name_fr = 'Fawn' WHERE id = '04CE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Severn' WHERE id = '04CF000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Winisk' WHERE id = '04DA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Winisk' WHERE id = '04DB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Winisk' WHERE id = '04DC000';
update cabd.nhn_workunit set name_fr = 'Shagamu' WHERE id = '04DD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Ekwan' WHERE id = '04EA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Ekwan' WHERE id = '04EB000';
update cabd.nhn_workunit set name_fr = 'Opinnagau' WHERE id = '04EC000';
update cabd.nhn_workunit set name_fr = 'Sutton' WHERE id = '04ED000';
update cabd.nhn_workunit set name_fr = 'Otoskwin' WHERE id = '04FA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Attawapiskat' WHERE id = '04FB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de l''Attawapiskat' WHERE id = '04FC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Albany - Cat' WHERE id = '04GA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Ogoki' WHERE id = '04GBA00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Ogoki' WHERE id = '04GBB00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Albany - Misehkow' WHERE id = '04GC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Albany - Lac Makokibatan' WHERE id = '04GD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de l''Ogoki' WHERE id = '04GE000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Albany - Muswabik' WHERE id = '04GF000';
update cabd.nhn_workunit set name_fr = 'Albany - Embouchure' WHERE id = '04HA000';
update cabd.nhn_workunit set name_fr = 'Kinosheo' WHERE id = '04HB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Kapiskau' WHERE id = '04HC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kapiskau' WHERE id = '04HD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Kabinakagami' WHERE id = '04JA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kabinakagami' WHERE id = '04JB000';
update cabd.nhn_workunit set name_fr = 'Nagagami' WHERE id = '04JC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Kenogami' WHERE id = '04JDA00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Kenogami' WHERE id = '04JDB00';
update cabd.nhn_workunit set name_fr = 'Drowning' WHERE id = '04JE000';
update cabd.nhn_workunit set name_fr = 'Petite rivière Current' WHERE id = '04JF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kenogami' WHERE id = '04JG000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Moose' WHERE id = '04KA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Moose' WHERE id = '04KB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Mattagami' WHERE id = '04LA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Mattagami' WHERE id = '04LB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Groundhog' WHERE id = '04LC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Groundhog' WHERE id = '04LD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Kapuskasing' WHERE id = '04LE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kapuskasing' WHERE id = '04LF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Mattagami' WHERE id = '04LG000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Missinaibi' WHERE id = '04LH000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Missinaibi - Mattawitchewan' WHERE id = '04LJ000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Missinaibi - Mattawishkwia' WHERE id = '04LK000';
update cabd.nhn_workunit set name_fr = 'Opasatika' WHERE id = '04LL000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Missinaibi' WHERE id = '04LM000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Abitibi' WHERE id = '04MA000';
update cabd.nhn_workunit set name_fr = 'Black' WHERE id = '04MB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de l''Abitibi' WHERE id = '04MC000';
update cabd.nhn_workunit set name_fr = 'Frederick House' WHERE id = '04MD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de l''Abitibi' WHERE id = '04ME000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Harricana' WHERE id = '04NA000';
update cabd.nhn_workunit set name_fr = 'Turgeon' WHERE id = '04NB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Harricana' WHERE id = '04NC000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0501001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Oldman' WHERE id = '05AA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Oldman - Willow' WHERE id = '05AB000';
update cabd.nhn_workunit set name_fr = 'Petite rivière Bow' WHERE id = '05AC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Oldman - Belly' WHERE id = '05AD000';
update cabd.nhn_workunit set name_fr = 'St. Mary' WHERE id = '05AE000';
update cabd.nhn_workunit set name_fr = 'Lac Pakowki - Ne contribue pas' WHERE id = '05AF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Oldman' WHERE id = '05AG000';
update cabd.nhn_workunit set name_fr = 'Seven Persons' WHERE id = '05AH000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saskatchewan Sud - Cours supérieur' WHERE id = '05AJ000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saskatchewan Sud - Cours inférieur' WHERE id = '05AK000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Bow' WHERE id = '05BA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Bow - Redearth' WHERE id = '05BB000';
update cabd.nhn_workunit set name_fr = 'Spray' WHERE id = '05BC000';
update cabd.nhn_workunit set name_fr = 'Cascade' WHERE id = '05BD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Bow - Policeman' WHERE id = '05BE000';
update cabd.nhn_workunit set name_fr = 'Kananaskis' WHERE id = '05BF000';
update cabd.nhn_workunit set name_fr = 'Ghost' WHERE id = '05BG000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Bow - Jumpingpond' WHERE id = '05BH000';
update cabd.nhn_workunit set name_fr = 'Elbow' WHERE id = '05BJ000';
update cabd.nhn_workunit set name_fr = 'Fish (Alb.)' WHERE id = '05BK000';
update cabd.nhn_workunit set name_fr = 'Highwood' WHERE id = '05BL000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Bow - Crowfoot' WHERE id = '05BM000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Bow - Embouchure' WHERE id = '05BN000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Red Deer' WHERE id = '05CA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Red Deer - Petite rivière Red Deer' WHERE id = '05CB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Red Deer - Blindman' WHERE id = '05CC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Red Deer - Tail' WHERE id = '05CD000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Red Deer - Rosebud' WHERE id = '05CE000';
update cabd.nhn_workunit set name_fr = 'Lac Dowling - Ne contribue pas' WHERE id = '05CF000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Red Deer - Bullpound' WHERE id = '05CG000';
update cabd.nhn_workunit set name_fr = 'Berry' WHERE id = '05CH000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Red Deer - Matzhiwin' WHERE id = '05CJ000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Red Deer - Blood Indian' WHERE id = '05CK000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Saskatchewan Nord' WHERE id = '05DA000';
update cabd.nhn_workunit set name_fr = 'Clearwater (Alb.- Saskatchewan Nord)' WHERE id = '05DB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saskatchewan Nord - Ram' WHERE id = '05DC000';
update cabd.nhn_workunit set name_fr = 'Brazeau' WHERE id = '05DD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saskatchewan Nord - Wabamun' WHERE id = '05DE000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saskatchewan Nord - Strawberry' WHERE id = '05DF000';
update cabd.nhn_workunit set name_fr = 'Sturgeon (Alb.)' WHERE id = '05EA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saskatchewan Nord - Beaverhill' WHERE id = '05EB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saskatchewan Nord - Redwater' WHERE id = '05EC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saskatchewan Nord - Lac Frog' WHERE id = '05ED000';
update cabd.nhn_workunit set name_fr = 'Vermilion (Alb.)' WHERE id = '05EE000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saskatchewan Nord - Big Gully' WHERE id = '05EF000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saskatchewan Nord - Turtlelake' WHERE id = '05EG000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Battle' WHERE id = '05FA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Battle - Iron' WHERE id = '05FB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Battle - Meeting' WHERE id = '05FC000';
update cabd.nhn_workunit set name_fr = 'Ribstone' WHERE id = '05FD000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Battle - Blackfoot' WHERE id = '05FE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Battle - Embouchure' WHERE id = '05FF000';
update cabd.nhn_workunit set name_fr = 'Lac Manitou  - Ne contribue pas' WHERE id = '05GA000';
update cabd.nhn_workunit set name_fr = 'Lac Kiyiu  - Ne contribue pas' WHERE id = '05GB001';
update cabd.nhn_workunit set name_fr = 'Lac Kiyiu  - Ne contribue pas' WHERE id = '05GB002';
update cabd.nhn_workunit set name_fr = 'Eagle' WHERE id = '05GC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Nord - Marshy' WHERE id = '05GD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Nord - Radouga' WHERE id = '05GE000';
update cabd.nhn_workunit set name_fr = 'Sturgeon (Sask.)' WHERE id = '05GF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Nord - Spruce' WHERE id = '05GG000';
update cabd.nhn_workunit set name_fr = 'Lac Crane  - Ne contribue pas' WHERE id = '05HA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Sud - Happyland' WHERE id = '05HB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Sud - Miry-Antelope' WHERE id = '05HC000';
update cabd.nhn_workunit set name_fr = 'Swift Current' WHERE id = '05HD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Sud - Snakebite' WHERE id = '05HE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Sud - Diefenbaker' WHERE id = '05HF001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Sud - Diefenbaker' WHERE id = '05HF002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Sud - Brightwater' WHERE id = '05HG001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Sud - Brightwater' WHERE id = '05HG002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Sud - Brightwater' WHERE id = '05HG003';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan Sud - Embouchure' WHERE id = '05HH000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Wood - Ne contribue pas' WHERE id = '05JA000';
update cabd.nhn_workunit set name_fr = 'Notukeu - Ne contribue pas' WHERE id = '05JB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Wood - Ne contribue pas' WHERE id = '05JC000';
update cabd.nhn_workunit set name_fr = 'Lac Old Wives - Ne contribue pas' WHERE id = '05JD001';
update cabd.nhn_workunit set name_fr = 'Lac Old Wives - Ne contribue pas' WHERE id = '05JD002';
update cabd.nhn_workunit set name_fr = 'Moose Jaw' WHERE id = '05JE000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Qu''Appelle - Wascana' WHERE id = '05JF000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Qu''Appelle - Thunder' WHERE id = '05JG000';
update cabd.nhn_workunit set name_fr = 'Last Mountain' WHERE id = '05JH000';
update cabd.nhn_workunit set name_fr = 'Lanigan' WHERE id = '05JJ000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Qu''Appelle - Loon' WHERE id = '05JK000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Qu''Appelle - Pheasant' WHERE id = '05JL000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Qu''Appelle - Embouchure' WHERE id = '05JM000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Carrot' WHERE id = '05KA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Carrot - Leather' WHERE id = '05KB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Carrot - Man' WHERE id = '05KC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Saskatchewan' WHERE id = '05KD000';
update cabd.nhn_workunit set name_fr = 'Torch' WHERE id = '05KE000';
update cabd.nhn_workunit set name_fr = 'Lac Deschambault' WHERE id = '05KF000';
update cabd.nhn_workunit set name_fr = 'Sturgeon-Weir' WHERE id = '05KG000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saskatchewan - Cours inférieur de la Carrot' WHERE id = '05KH000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Saskatchewan - Pasquia' WHERE id = '05KJ001';
update cabd.nhn_workunit set name_fr = 'Moose (Man.)' WHERE id = '05KK001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Saskatchewan - Embouchure' WHERE id = '05KL001';
update cabd.nhn_workunit set name_fr = '' WHERE id = '05L0001';
update cabd.nhn_workunit set name_fr = '' WHERE id = '05L1001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Red Deer' WHERE id = '05LA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Red Deer' WHERE id = '05LB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Red Deer' WHERE id = '05LC000';
update cabd.nhn_workunit set name_fr = 'Overflowing' WHERE id = '05LD001';
update cabd.nhn_workunit set name_fr = 'Shoal' WHERE id = '05LE000';
update cabd.nhn_workunit set name_fr = 'Nord du lac Winnipegeosis - Steeprock' WHERE id = '05LF001';
update cabd.nhn_workunit set name_fr = 'Nord du lac Winnipegeosis - Steeprock' WHERE id = '05LF002';
update cabd.nhn_workunit set name_fr = 'Nord du lac Winnipegeosis - Steeprock' WHERE id = '05LF003';
update cabd.nhn_workunit set name_fr = 'Sud-ouest du lac Winnipeg - Pine (Man.)' WHERE id = '05LG001';
update cabd.nhn_workunit set name_fr = 'Sud-est du lac Winnipeg - Waterhen (Man.)' WHERE id = '05LH001';
update cabd.nhn_workunit set name_fr = 'Mossy' WHERE id = '05LJ000';
update cabd.nhn_workunit set name_fr = 'Nord du lac Manitoba' WHERE id = '05LK001';
update cabd.nhn_workunit set name_fr = 'Sud-ouest du lac Manitoba - Whitemud' WHERE id = '05LL001';
update cabd.nhn_workunit set name_fr = 'Dauphin' WHERE id = '05LM000';
update cabd.nhn_workunit set name_fr = 'Sud-est du lac Manitoba' WHERE id = '05LN001';
update cabd.nhn_workunit set name_fr = 'Lacs Quill - Ne contribuent pas' WHERE id = '05MA000';
update cabd.nhn_workunit set name_fr = 'Whitesand' WHERE id = '05MB000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de l''Assiniboine' WHERE id = '05MC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Assiniboine - Shell' WHERE id = '05MD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Assiniboine - Birdtail' WHERE id = '05ME000';
update cabd.nhn_workunit set name_fr = 'Petite rivière Saskatchewan' WHERE id = '05MF000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de l''Assiniboine - Oak' WHERE id = '05MG000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de l''Assiniboine - Cypress' WHERE id = '05MH000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de l''Assiniboine - Embouchure' WHERE id = '05MJ000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Long' WHERE id = '05NA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Souris' WHERE id = '05NB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Moose Mountain' WHERE id = '05NC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Souris - Moose Mountain' WHERE id = '05NDA00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Souris - Moose Mountain' WHERE id = '05NDB00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Pipestone' WHERE id = '05NE000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Souris - Antler' WHERE id = '05NFA00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Souris - Antler' WHERE id = '05NFB00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Souris - Antler' WHERE id = '05NFC00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Souris - Embouchure' WHERE id = '05NG000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Pembina (Man.)' WHERE id = '05OA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Pembina (Man.)' WHERE id = '05OB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Rouge' WHERE id = '05OCB00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Rouge' WHERE id = '05OCC00';
update cabd.nhn_workunit set name_fr = 'Roseau' WHERE id = '05OD000';
update cabd.nhn_workunit set name_fr = 'Rat et Tourond' WHERE id = '05OE000';
update cabd.nhn_workunit set name_fr = 'Morris' WHERE id = '05OF000';
update cabd.nhn_workunit set name_fr = 'La Salle' WHERE id = '05OG000';
update cabd.nhn_workunit set name_fr = 'Seine' WHERE id = '05OH001';
update cabd.nhn_workunit set name_fr = 'Seine' WHERE id = '05OH002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Rouge' WHERE id = '05OJ001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière à la Pluie' WHERE id = '05PAB00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière à la Pluie' WHERE id = '05PBA00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière à la Pluie' WHERE id = '05PBB00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière à la Pluie' WHERE id = '05PCD00';
update cabd.nhn_workunit set name_fr = 'Lac des Bois' WHERE id = '05PDA00';
update cabd.nhn_workunit set name_fr = 'Lac des Bois' WHERE id = '05PDB00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Winnipeg' WHERE id = '05PE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Winnipeg' WHERE id = '05PF000';
update cabd.nhn_workunit set name_fr = 'Whiteshell' WHERE id = '05PG000';
update cabd.nhn_workunit set name_fr = 'Whitemouth' WHERE id = '05PH000';
update cabd.nhn_workunit set name_fr = 'Bird' WHERE id = '05PJ000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la English' WHERE id = '05QA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la English - Lac Seul' WHERE id = '05QB000';
update cabd.nhn_workunit set name_fr = 'Chukuni' WHERE id = '05QC000';
update cabd.nhn_workunit set name_fr = 'Wabigoon' WHERE id = '05QD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la English' WHERE id = '05QE000';
update cabd.nhn_workunit set name_fr = 'Manigotagan' WHERE id = '05RA001';
update cabd.nhn_workunit set name_fr = 'Bloodvein' WHERE id = '05RB001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Berens' WHERE id = '05RC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Berens' WHERE id = '05RD001';
update cabd.nhn_workunit set name_fr = 'Poplar' WHERE id = '05RE001';
update cabd.nhn_workunit set name_fr = 'Mukutawa' WHERE id = '05RF001';
update cabd.nhn_workunit set name_fr = 'Brokenhead' WHERE id = '05SA001';
update cabd.nhn_workunit set name_fr = 'Willow' WHERE id = '05SB001';
update cabd.nhn_workunit set name_fr = 'Icelandic' WHERE id = '05SC001';
update cabd.nhn_workunit set name_fr = 'Fisher' WHERE id = '05SD001';
update cabd.nhn_workunit set name_fr = 'Mantagao' WHERE id = '05SE001';
update cabd.nhn_workunit set name_fr = 'Warpath' WHERE id = '05SF001';
update cabd.nhn_workunit set name_fr = 'Twin Nord et Twin Sud' WHERE id = '05SG001';
update cabd.nhn_workunit set name_fr = 'William' WHERE id = '05SH001';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Grass' WHERE id = '05TA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Grass' WHERE id = '05TB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Grass' WHERE id = '05TC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Grass' WHERE id = '05TD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Burntwood' WHERE id = '05TE000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Burntwood' WHERE id = '05TF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Burntwood' WHERE id = '05TG000';
update cabd.nhn_workunit set name_fr = 'Gunisao' WHERE id = '05UA001';
update cabd.nhn_workunit set name_fr = 'Source du Nelson' WHERE id = '05UB001';
update cabd.nhn_workunit set name_fr = 'Minago' WHERE id = '05UC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Nelson' WHERE id = '05UD000';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Nelson - Cours supérieur' WHERE id = '05UE000';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Nelson - Cours inférieur' WHERE id = '05UF000';
update cabd.nhn_workunit set name_fr = 'Limestone' WHERE id = '05UG000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Nelson - Embouchure' WHERE id = '05UH000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Beaver (Alb.-Sask.)' WHERE id = '06AA000';
update cabd.nhn_workunit set name_fr = 'Sand' WHERE id = '06AB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Beaver (Alb.-Sask.)' WHERE id = '06AC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Beaver (Alb.-Sask.)' WHERE id = '06AD000';
update cabd.nhn_workunit set name_fr = 'Cowan' WHERE id = '06AE000';
update cabd.nhn_workunit set name_fr = 'Waterhen (Sask.)' WHERE id = '06AF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Beaver (Alb.-Sask.)' WHERE id = '06AG000';
update cabd.nhn_workunit set name_fr = 'Lac Churchill' WHERE id = '06BA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Churchill (Man.) - Lac Île-à-la-Crosse' WHERE id = '06BB000';
update cabd.nhn_workunit set name_fr = 'Mudjatik' WHERE id = '06BC000';
update cabd.nhn_workunit set name_fr = 'Haultain' WHERE id = '06BD000';
update cabd.nhn_workunit set name_fr = 'Montreal (Sask.)' WHERE id = '06CA000';
update cabd.nhn_workunit set name_fr = 'Rapid' WHERE id = '06CB000';
update cabd.nhn_workunit set name_fr = 'Smoothstone' WHERE id = '06CC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Churchill (Man.) - Lac Pinehouse' WHERE id = '06CD000';
update cabd.nhn_workunit set name_fr = 'Foster' WHERE id = '06CE000';
update cabd.nhn_workunit set name_fr = 'Cochrane' WHERE id = '06DA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Reindeer - Lac Reindeer' WHERE id = '06DB000';
update cabd.nhn_workunit set name_fr = 'Wathaman' WHERE id = '06DC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Reindeer - Embouchure' WHERE id = '06DD000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Churchill (Man.) - Lac Highrock' WHERE id = '06EA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Churchill (Man.) - Hughes' WHERE id = '06EB000';
update cabd.nhn_workunit set name_fr = 'Lac Southern Indian' WHERE id = '06EC000';
update cabd.nhn_workunit set name_fr = 'Lac Norhern Indian' WHERE id = '06FA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Churchill (Man.) - Petite rivière Beaver' WHERE id = '06FB000';
update cabd.nhn_workunit set name_fr = 'Petite rivière Churchill' WHERE id = '06FC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Churchill (Man.) - Embouchure' WHERE id = '06FD001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Churchill (Man.) - Embouchure' WHERE id = '06FD002';
update cabd.nhn_workunit set name_fr = 'Baie d''Hudson - Owl' WHERE id = '06FE001';
update cabd.nhn_workunit set name_fr = 'Baie d''Hudson - Owl' WHERE id = '06FE002';
update cabd.nhn_workunit set name_fr = 'Baie d''Hudson - Owl' WHERE id = '06FE003';
update cabd.nhn_workunit set name_fr = 'Seal Sud' WHERE id = '06GA000';
update cabd.nhn_workunit set name_fr = 'Seal Nord' WHERE id = '06GB000';
update cabd.nhn_workunit set name_fr = 'Knife Nord et Knife Sud' WHERE id = '06GC001';
update cabd.nhn_workunit set name_fr = 'Knife Nord et Knife Sud' WHERE id = '06GC002';
update cabd.nhn_workunit set name_fr = 'Seal' WHERE id = '06GD000';
update cabd.nhn_workunit set name_fr = 'Caribou' WHERE id = '06GE000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Thlewiaza - Lac Nueltin' WHERE id = '06HA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Thlewiaza - Lac Nueltin' WHERE id = '06HA002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Thlewiaza - Embouchure' WHERE id = '06HB000';
update cabd.nhn_workunit set name_fr = 'Geillini' WHERE id = '06HC001';
update cabd.nhn_workunit set name_fr = 'Geillini' WHERE id = '06HC002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Tha-anne - Kognak' WHERE id = '06HD001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Tha-anne - Kognak' WHERE id = '06HD002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Tha-anne' WHERE id = '06HE000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Thelon - Lac Lynx' WHERE id = '06JA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Thelon - Lac Lynx' WHERE id = '06JA002';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Thelon - Hanbury' WHERE id = '06JB001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Thelon - Hanbury' WHERE id = '06JB002';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Thelon - Lac Beverly' WHERE id = '06JC001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Thelon - Lac Beverly' WHERE id = '06JC002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Thelon - Embouchure' WHERE id = '06JD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Dubawnt' WHERE id = '06KA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Dubawnt' WHERE id = '06KA002';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Dubawnt' WHERE id = '06KB001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Dubawnt' WHERE id = '06KB002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Dubawnt' WHERE id = '06KC001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Dubawnt' WHERE id = '06KC002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Kazan - Lac Ennadai' WHERE id = '06LA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Kazan' WHERE id = '06LB001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Kazan' WHERE id = '06LB002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kazan' WHERE id = '06LC001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kazan' WHERE id = '06LC002';
update cabd.nhn_workunit set name_fr = 'Lac Baker' WHERE id = '06MA000';
update cabd.nhn_workunit set name_fr = 'Quoich' WHERE id = '06MB001';
update cabd.nhn_workunit set name_fr = 'Quoich' WHERE id = '06MB002';
update cabd.nhn_workunit set name_fr = 'Quoich' WHERE id = '06MB003';
update cabd.nhn_workunit set name_fr = 'Inlet Chesterfield - Embouchure' WHERE id = '06MC001';
update cabd.nhn_workunit set name_fr = 'Inlet Chesterfield - Embouchure' WHERE id = '06MC002';
update cabd.nhn_workunit set name_fr = 'Inlet Chesterfield - Embouchure' WHERE id = '06MC003';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Maguse' WHERE id = '06NA001';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Maguse' WHERE id = '06NA002';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Ferguson' WHERE id = '06NB001';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Ferguson' WHERE id = '06NB002';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Wilson' WHERE id = '06NC001';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Wilson' WHERE id = '06NC002';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Lorillard' WHERE id = '06OA001';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Lorillard' WHERE id = '06OA002';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Lorillard' WHERE id = '06OA003';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Lorillard' WHERE id = '06OA004';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Baie Wager' WHERE id = '06OB001';
update cabd.nhn_workunit set name_fr = 'Ouest de la baie d''Hudson - Baie Wager' WHERE id = '06OB002';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Southampton' WHERE id = '06PA001';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Southampton' WHERE id = '06PA002';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Southampton' WHERE id = '06PA003';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Southampton' WHERE id = '06PA004';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Southampton' WHERE id = '06PA005';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Southampton' WHERE id = '06PA006';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Southampton' WHERE id = '06PA007';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Southampton' WHERE id = '06PA008';
update cabd.nhn_workunit set name_fr = 'Nord de l''île Southampton' WHERE id = '06QA000';
update cabd.nhn_workunit set name_fr = 'Baie Repulse' WHERE id = '06RA000';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Barrow' WHERE id = '06RB001';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Barrow' WHERE id = '06RB002';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Kingora' WHERE id = '06RC001';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Kingora' WHERE id = '06RC002';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Kingora' WHERE id = '06RC003';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Gifford' WHERE id = '06SA001';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Gifford' WHERE id = '06SA002';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - MacDonald' WHERE id = '06SB001';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - MacDonald' WHERE id = '06SB002';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - MacDonald' WHERE id = '06SB003';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - MacDonald' WHERE id = '06SB004';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - MacDonald' WHERE id = '06SB005';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - MacDonald' WHERE id = '06SB006';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Île Prince-Charles' WHERE id = '06SC000';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Koukdjuak' WHERE id = '06SD001';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Koukdjuak' WHERE id = '06SD002';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Koukdjuak' WHERE id = '06SD003';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Koukdjuak' WHERE id = '06SD004';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Koukdjuak' WHERE id = '06SD005';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Aukpar' WHERE id = '06SE001';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Aukpar' WHERE id = '06SE002';
update cabd.nhn_workunit set name_fr = 'Bassin Fox - Baffin - Aukpar' WHERE id = '06SE003';
update cabd.nhn_workunit set name_fr = 'Île de Baffin - Détroit d''Hudson' WHERE id = '06TA001';
update cabd.nhn_workunit set name_fr = 'Île de Baffin - Détroit d''Hudson' WHERE id = '06TA002';
update cabd.nhn_workunit set name_fr = 'Île de Baffin - Détroit d''Hudson' WHERE id = '06TA003';
update cabd.nhn_workunit set name_fr = 'Île de Baffin - Détroit d''Hudson' WHERE id = '06TA004';
update cabd.nhn_workunit set name_fr = 'Est de l''île Southampton' WHERE id = '06TB000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0701000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '0710000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de l''Athabasca' WHERE id = '07AA000';
update cabd.nhn_workunit set name_fr = 'Snake Indian' WHERE id = '07AB000';
update cabd.nhn_workunit set name_fr = 'Berland' WHERE id = '07AC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Athabasca - Oldman' WHERE id = '07AD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Athabasca - Windfall' WHERE id = '07AE000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la McLeod' WHERE id = '07AF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la McLeod' WHERE id = '07AG000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de l''Athabasca - Freeman' WHERE id = '07AH000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '07B0000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Pembina (Alb.)' WHERE id = '07BA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Pembina (Alb.)' WHERE id = '07BB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Pembina (Alb.)' WHERE id = '07BC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de l''Athabasca - Timeu' WHERE id = '07BD000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de l''Athabasca - Tawatinaw' WHERE id = '07BE000';
update cabd.nhn_workunit set name_fr = 'Heart Sud' WHERE id = '07BF000';
update cabd.nhn_workunit set name_fr = 'Nord du Petit lac des Esclaves' WHERE id = '07BG000';
update cabd.nhn_workunit set name_fr = 'Sud-ouest du Petit lac des Esclaves' WHERE id = '07BH000';
update cabd.nhn_workunit set name_fr = 'Sud-est du Petit lac des Esclaves' WHERE id = '07BJ000';
update cabd.nhn_workunit set name_fr = 'Petit lac des Esclaves' WHERE id = '07BK000';
update cabd.nhn_workunit set name_fr = 'La Biche' WHERE id = '07CA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de l''Athabasca - House' WHERE id = '07CB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de l''Athabasca - Horse' WHERE id = '07CC000';
update cabd.nhn_workunit set name_fr = 'Clearwater (Alb.-Athabasca)' WHERE id = '07CD000';
update cabd.nhn_workunit set name_fr = 'Christina' WHERE id = '07CE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de l''Athabasca - Ells' WHERE id = '07DA000';
update cabd.nhn_workunit set name_fr = 'MacKay' WHERE id = '07DB000';
update cabd.nhn_workunit set name_fr = 'Firebag' WHERE id = '07DC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de l''Athabasca - Embouchure' WHERE id = '07DD000';
update cabd.nhn_workunit set name_fr = 'Finlay' WHERE id = '07EA001';
update cabd.nhn_workunit set name_fr = 'Finlay' WHERE id = '07EA002';
update cabd.nhn_workunit set name_fr = 'Finlay' WHERE id = '07EA003';
update cabd.nhn_workunit set name_fr = 'Finlay' WHERE id = '07EA005';
update cabd.nhn_workunit set name_fr = 'Finlay' WHERE id = '07EA0X4';
update cabd.nhn_workunit set name_fr = 'Nord du lac Williston' WHERE id = '07EB001';
update cabd.nhn_workunit set name_fr = 'Nord du lac Williston' WHERE id = '07EB002';
update cabd.nhn_workunit set name_fr = 'Omineca' WHERE id = '07EC001';
update cabd.nhn_workunit set name_fr = 'Omineca' WHERE id = '07EC003';
update cabd.nhn_workunit set name_fr = 'Omineca' WHERE id = '07EC0X2';
update cabd.nhn_workunit set name_fr = 'Nation' WHERE id = '07ED000';
update cabd.nhn_workunit set name_fr = 'Sud du lac Williston - Parsnip' WHERE id = '07EE000';
update cabd.nhn_workunit set name_fr = 'Est du lac Williston' WHERE id = '07EF000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière de la Paix - Halfway' WHERE id = '07FA000';
update cabd.nhn_workunit set name_fr = 'Pine (C.-B.)' WHERE id = '07FB000';
update cabd.nhn_workunit set name_fr = 'Beatton' WHERE id = '07FC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière de la Paix - Kiskatinaw' WHERE id = '07FD000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Smoky' WHERE id = '07GA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Smoky' WHERE id = '07GB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Wapiti' WHERE id = '07GC000';
update cabd.nhn_workunit set name_fr = 'Redwillow' WHERE id = '07GD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Wapiti' WHERE id = '07GE000';
update cabd.nhn_workunit set name_fr = 'Simonette' WHERE id = '07GF000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Petite rivière Smoky' WHERE id = '07GG000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Petite rivière Smoky' WHERE id = '07GH000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Smoky' WHERE id = '07GJ000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière de la Paix - Whitemud' WHERE id = '07HA000';
update cabd.nhn_workunit set name_fr = 'Cadotte' WHERE id = '07HB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière de la Paix - Notikewin' WHERE id = '07HC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière de la Paix - Scully' WHERE id = '07HD000';
update cabd.nhn_workunit set name_fr = 'Wolverine et Buffalo' WHERE id = '07HE000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière de la Paix - Keg' WHERE id = '07HF000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Wabasca' WHERE id = '07JA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Wabasca' WHERE id = '07JB000';
update cabd.nhn_workunit set name_fr = 'Loon' WHERE id = '07JC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Wabasca' WHERE id = '07JD000';
update cabd.nhn_workunit set name_fr = 'Mikkwa' WHERE id = '07JE000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière de la Paix - Boyer' WHERE id = '07JF000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière de la Paix - Wentzel' WHERE id = '07KA000';
update cabd.nhn_workunit set name_fr = 'Jackfish' WHERE id = '07KB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière de la Paix - Embouchure' WHERE id = '07KC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Birch' WHERE id = '07KD000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Birch' WHERE id = '07KE000';
update cabd.nhn_workunit set name_fr = 'Claire' WHERE id = '07KF000';
update cabd.nhn_workunit set name_fr = 'Porcupine Est' WHERE id = '07LA000';
update cabd.nhn_workunit set name_fr = 'Waterfound' WHERE id = '07LB000';
update cabd.nhn_workunit set name_fr = 'Lac Black - Fond du Lac' WHERE id = '07LC000';
update cabd.nhn_workunit set name_fr = 'Cree' WHERE id = '07LD000';
update cabd.nhn_workunit set name_fr = 'Est du lac Athabasca - Fond du Lac' WHERE id = '07LEA00';
update cabd.nhn_workunit set name_fr = 'Est du lac Athabasca - Fond du Lac' WHERE id = '07LEB00';
update cabd.nhn_workunit set name_fr = 'Est du lac Athabasca - Fond du Lac' WHERE id = '07LEC00';
update cabd.nhn_workunit set name_fr = 'Sud-ouest du lac Athabasca' WHERE id = '07MA000';
update cabd.nhn_workunit set name_fr = 'Sud-est du lac Athabasca' WHERE id = '07MB000';
update cabd.nhn_workunit set name_fr = 'Nord-est du lac Athabasca' WHERE id = '07MC000';
update cabd.nhn_workunit set name_fr = 'Nord-ouest du lac Athabasca' WHERE id = '07MD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la rivière des Esclaves' WHERE id = '07NA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la rivière des Esclaves' WHERE id = '07NB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la rivière des Esclaves' WHERE id = '07NC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Hay' WHERE id = '07OA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Hay' WHERE id = '07OB000';
update cabd.nhn_workunit set name_fr = 'Chinchaga' WHERE id = '07OC000';
update cabd.nhn_workunit set name_fr = 'Sud du Grand lac des Esclaves - Buffalo' WHERE id = '07PA000';
update cabd.nhn_workunit set name_fr = 'Sud du Grand lac des Esclaves - Petite rivière Buffalo' WHERE id = '07PB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Buffalo' WHERE id = '07PC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Taltson' WHERE id = '07QA000';
update cabd.nhn_workunit set name_fr = 'Snowdrift' WHERE id = '07QB001';
update cabd.nhn_workunit set name_fr = 'Snowdrift' WHERE id = '07QB002';
update cabd.nhn_workunit set name_fr = 'Tazin' WHERE id = '07QC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Taltson' WHERE id = '07QD001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Taltson' WHERE id = '07QD002';
update cabd.nhn_workunit set name_fr = 'Lac MacKay' WHERE id = '07RA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Lockhart' WHERE id = '07RB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Lockhart' WHERE id = '07RC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Lockhart' WHERE id = '07RD000';
update cabd.nhn_workunit set name_fr = 'Snare' WHERE id = '07SA001';
update cabd.nhn_workunit set name_fr = 'Snare' WHERE id = '07SA002';
update cabd.nhn_workunit set name_fr = 'Yellowknife' WHERE id = '07SB000';
update cabd.nhn_workunit set name_fr = 'Grand lac des Esclaves - Bras est - Rive nord' WHERE id = '07SC001';
update cabd.nhn_workunit set name_fr = 'Grand lac des Esclaves - Bras est - Rive nord' WHERE id = '07SC002';
update cabd.nhn_workunit set name_fr = 'La Martre' WHERE id = '07TA000';
update cabd.nhn_workunit set name_fr = 'Marian - Embouchure' WHERE id = '07TB000';
update cabd.nhn_workunit set name_fr = 'Grand lac des Esclaves - Bras nord - Rive ouest' WHERE id = '07UA000';
update cabd.nhn_workunit set name_fr = 'Grand lac des Esclaves - Décharge' WHERE id = '07UB001';
update cabd.nhn_workunit set name_fr = 'Grand lac des Esclaves - Décharge' WHERE id = '07UB002';
update cabd.nhn_workunit set name_fr = 'Kakisa' WHERE id = '07UC001';
update cabd.nhn_workunit set name_fr = 'Dezadeash' WHERE id = '08AA000';
update cabd.nhn_workunit set name_fr = 'Golfe d''Alaska - Alsek' WHERE id = '08AB000';
update cabd.nhn_workunit set name_fr = 'Tatshenshini' WHERE id = '08ACA00';
update cabd.nhn_workunit set name_fr = 'Tatshenshini' WHERE id = '08ACB00';
update cabd.nhn_workunit set name_fr = 'Golfe d''Alaska - Baie Yakutat' WHERE id = '08AD000';
update cabd.nhn_workunit set name_fr = 'Inklin' WHERE id = '08BAA00';
update cabd.nhn_workunit set name_fr = 'Inklin' WHERE id = '08BAB00';
update cabd.nhn_workunit set name_fr = 'Inklin' WHERE id = '08BAC00';
update cabd.nhn_workunit set name_fr = 'Taku' WHERE id = '08BBA00';
update cabd.nhn_workunit set name_fr = 'Taku' WHERE id = '08BBB00';
update cabd.nhn_workunit set name_fr = 'Chenal Stephens' WHERE id = '08BCA00';
update cabd.nhn_workunit set name_fr = 'Chenal Lynn' WHERE id = '08BD000';
update cabd.nhn_workunit set name_fr = 'Inlets Chilkat et Chilkoot' WHERE id = '08BE000';
update cabd.nhn_workunit set name_fr = 'Baie Glacier' WHERE id = '08BF000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Stikine' WHERE id = '08CA001';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Stikine' WHERE id = '08CA002';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Stikine' WHERE id = '08CA003';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Stikine' WHERE id = '08CA004';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Stikine' WHERE id = '08CB001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Stikine' WHERE id = '08CB002';
update cabd.nhn_workunit set name_fr = 'Klappan' WHERE id = '08CC000';
update cabd.nhn_workunit set name_fr = 'Tuya' WHERE id = '08CD000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Stikine' WHERE id = '08CE001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Stikine' WHERE id = '08CE002';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Stikine' WHERE id = '08CE003';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Stikine' WHERE id = '08CE004';
update cabd.nhn_workunit set name_fr = 'Eaux côtières de la C.-B. - Cours inférieur de la Stikine' WHERE id = '08CFA00';
update cabd.nhn_workunit set name_fr = 'Eaux côtières de la C.-B. - Cours inférieur de la Stikine' WHERE id = '08CFB00';
update cabd.nhn_workunit set name_fr = 'Eaux côtières de la C.-B. - Cours inférieur de la Stikine' WHERE id = '08CFC00';
update cabd.nhn_workunit set name_fr = 'Iskut' WHERE id = '08CG002';
update cabd.nhn_workunit set name_fr = 'Iskut' WHERE id = '08CG003';
update cabd.nhn_workunit set name_fr = 'Iskut' WHERE id = '08CGC00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Nass' WHERE id = '08DA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Nass' WHERE id = '08DA002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Nass' WHERE id = '08DA003';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Nass' WHERE id = '08DA004';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Nass' WHERE id = '08DA005';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Nass' WHERE id = '08DA006';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Nass' WHERE id = '08DBA00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Nass' WHERE id = '08DBB00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Nass' WHERE id = '08DBC00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Nass' WHERE id = '08DBD00';
update cabd.nhn_workunit set name_fr = 'Eaux côtières de la C.-B. - Détroit Clarence' WHERE id = '08DDA00';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Skeena' WHERE id = '08EA001';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Skeena' WHERE id = '08EA002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Skeena' WHERE id = '08EB001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Skeena' WHERE id = '08EB002';
update cabd.nhn_workunit set name_fr = 'Babine' WHERE id = '08EC001';
update cabd.nhn_workunit set name_fr = 'Babine' WHERE id = '08EC002';
update cabd.nhn_workunit set name_fr = 'Morice' WHERE id = '08ED000';
update cabd.nhn_workunit set name_fr = 'Bulkley' WHERE id = '08EE0X0';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Skeena' WHERE id = '08EF001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Skeena' WHERE id = '08EGB03';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Skeena' WHERE id = '08EGBX1';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Skeena' WHERE id = '08EGBX2';
update cabd.nhn_workunit set name_fr = 'Détroit de la Reine-Charlotte - Inlet Rivers' WHERE id = '08FAAX1';
update cabd.nhn_workunit set name_fr = 'Détroit de la Reine-Charlotte - Inlet Rivers' WHERE id = '08FABX1';
update cabd.nhn_workunit set name_fr = 'Détroit de la Reine-Charlotte - Bella Coola' WHERE id = '08FB002';
update cabd.nhn_workunit set name_fr = 'Détroit de la Reine-Charlotte - Bella Coola' WHERE id = '08FB0X1';
update cabd.nhn_workunit set name_fr = 'Détroit de la Reine-Charlotte - Dean' WHERE id = '08FC001';
update cabd.nhn_workunit set name_fr = 'Détroit de la Reine-Charlotte - Dean' WHERE id = '08FC0X2';
update cabd.nhn_workunit set name_fr = 'Chenal Milbanke' WHERE id = '08FD002';
update cabd.nhn_workunit set name_fr = 'Chenal Milbanke' WHERE id = '08FD004';
update cabd.nhn_workunit set name_fr = 'Chenal Milbanke' WHERE id = '08FD0X1';
update cabd.nhn_workunit set name_fr = 'Chenal Milbanke' WHERE id = '08FD0X3';
update cabd.nhn_workunit set name_fr = 'Chenal Milbanke' WHERE id = '08FD0X5';
update cabd.nhn_workunit set name_fr = 'Détroit d''Hécate - Chenal Gardner' WHERE id = '08FE0X1';
update cabd.nhn_workunit set name_fr = 'Détroit d''Hécate - Chenal Gardner' WHERE id = '08FE0X2';
update cabd.nhn_workunit set name_fr = 'Détroit d''Hécate - Kitimat' WHERE id = '08FF001';
update cabd.nhn_workunit set name_fr = 'Détroit d''Hécate - Chenal Principe' WHERE id = '08FG001';
update cabd.nhn_workunit set name_fr = 'Détroit d''Hécate - Chenal Principe' WHERE id = '08FG002';
update cabd.nhn_workunit set name_fr = 'Détroit d''Hécate - Chenal Principe' WHERE id = '08FG003';
update cabd.nhn_workunit set name_fr = 'Détroit d''Hécate - Chenal Principe' WHERE id = '08FG004';
update cabd.nhn_workunit set name_fr = 'Détroit d''Hécate - Chenal Principe' WHERE id = '08FG005';
update cabd.nhn_workunit set name_fr = 'Détroit d''Hécate - Chenal Principe' WHERE id = '08FG006';
update cabd.nhn_workunit set name_fr = 'Détroit de Georgia - Squamish' WHERE id = '08GABX1';
update cabd.nhn_workunit set name_fr = 'Détroit de Georgia - Inlet Jervis' WHERE id = '08GB0X1';
update cabd.nhn_workunit set name_fr = 'Détroit de Georgia - Inlet Toba' WHERE id = '08GC0X0';
update cabd.nhn_workunit set name_fr = 'Détroit de Georgia - Inlet Bute' WHERE id = '08GD001';
update cabd.nhn_workunit set name_fr = 'Klinaklini' WHERE id = '08GE0X0';
update cabd.nhn_workunit set name_fr = 'Est du détroit de la Reine-Charlotte' WHERE id = '08GF001';
update cabd.nhn_workunit set name_fr = 'Est du détroit de la Reine-Charlotte' WHERE id = '08GF002';
update cabd.nhn_workunit set name_fr = 'Sud de l''île de Vancouver' WHERE id = '08HA0X2';
update cabd.nhn_workunit set name_fr = 'Sud de l''île de Vancouver' WHERE id = '08HA0X3';
update cabd.nhn_workunit set name_fr = 'Sud de l''île de Vancouver' WHERE id = '08HAC00';
update cabd.nhn_workunit set name_fr = 'Sud de l''île de Vancouver' WHERE id = '08HAD00';
update cabd.nhn_workunit set name_fr = 'Centre sud de l''île de Vancouver' WHERE id = '08HB001';
update cabd.nhn_workunit set name_fr = 'Centre sud de l''île de Vancouver' WHERE id = '08HB002';
update cabd.nhn_workunit set name_fr = 'Centre sud de l''île de Vancouver' WHERE id = '08HB0X3';
update cabd.nhn_workunit set name_fr = 'Centre ouest de l''île de Vancouver' WHERE id = '08HC0X1';
update cabd.nhn_workunit set name_fr = 'Centre est de l''île de Vancouver' WHERE id = '08HD001';
update cabd.nhn_workunit set name_fr = 'Centre est de l''île de Vancouver' WHERE id = '08HD002';
update cabd.nhn_workunit set name_fr = 'Nord-ouest de l''île de Vancouver' WHERE id = '08HE002';
update cabd.nhn_workunit set name_fr = 'Nord-ouest de l''île de Vancouver' WHERE id = '08HE003';
update cabd.nhn_workunit set name_fr = 'Nord-ouest de l''île de Vancouver' WHERE id = '08HE0X1';
update cabd.nhn_workunit set name_fr = 'Nord-est de l''île de Vancouver' WHERE id = '08HF001';
update cabd.nhn_workunit set name_fr = 'Nord-est de l''île de Vancouver' WHERE id = '08HF002';
update cabd.nhn_workunit set name_fr = 'Nord-est de l''île de Vancouver' WHERE id = '08HF003';
update cabd.nhn_workunit set name_fr = 'Nord-est de l''île de Vancouver' WHERE id = '08HF0X4';
update cabd.nhn_workunit set name_fr = 'Réservoir Nechako' WHERE id = '08JAA00';
update cabd.nhn_workunit set name_fr = 'Réservoir Nechako' WHERE id = '08JAB01';
update cabd.nhn_workunit set name_fr = 'Réservoir Nechako' WHERE id = '08JAB02';
update cabd.nhn_workunit set name_fr = 'Réservoir Nechako' WHERE id = '08JAB03';
update cabd.nhn_workunit set name_fr = 'Réservoir Nechako' WHERE id = '08JAB04';
update cabd.nhn_workunit set name_fr = 'Nautley' WHERE id = '08JB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Nechako' WHERE id = '08JC001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Nechako' WHERE id = '08JC002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Nechako' WHERE id = '08JC003';
update cabd.nhn_workunit set name_fr = 'Middle' WHERE id = '08JD002';
update cabd.nhn_workunit set name_fr = 'Middle' WHERE id = '08JD003';
update cabd.nhn_workunit set name_fr = 'Middle' WHERE id = '08JD0X1';
update cabd.nhn_workunit set name_fr = 'Stuart' WHERE id = '08JE001';
update cabd.nhn_workunit set name_fr = 'Stuart' WHERE id = '08JE002';
update cabd.nhn_workunit set name_fr = 'Stuart' WHERE id = '08JE003';
update cabd.nhn_workunit set name_fr = 'Stuart' WHERE id = '08JE004';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont du Fraser' WHERE id = '08KA002';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont du Fraser' WHERE id = '08KA0X1';
update cabd.nhn_workunit set name_fr = 'McGregor' WHERE id = '08KB001';
update cabd.nhn_workunit set name_fr = 'McGregor' WHERE id = '08KB002';
update cabd.nhn_workunit set name_fr = 'Salmon (C.-B.)' WHERE id = '08KC001';
update cabd.nhn_workunit set name_fr = 'Salmon (C.-B.)' WHERE id = '08KC002';
update cabd.nhn_workunit set name_fr = 'Salmon (C.-B.)' WHERE id = '08KC003';
update cabd.nhn_workunit set name_fr = 'Willow - Bowron' WHERE id = '08KD001';
update cabd.nhn_workunit set name_fr = 'Willow - Bowron' WHERE id = '08KD002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Fraser - Baker' WHERE id = '08KEB01';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Fraser - Baker' WHERE id = '08KEB03';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Fraser - Baker' WHERE id = '08KEBX2';
update cabd.nhn_workunit set name_fr = 'Nazko' WHERE id = '08KF001';
update cabd.nhn_workunit set name_fr = 'West Road' WHERE id = '08KG001';
update cabd.nhn_workunit set name_fr = 'West Road' WHERE id = '08KG002';
update cabd.nhn_workunit set name_fr = 'West Road' WHERE id = '08KG003';
update cabd.nhn_workunit set name_fr = 'Quesnel' WHERE id = '08KH001';
update cabd.nhn_workunit set name_fr = 'Quesnel' WHERE id = '08KH002';
update cabd.nhn_workunit set name_fr = 'Quesnel' WHERE id = '08KH003';
update cabd.nhn_workunit set name_fr = 'Clearwater (C.-B.)' WHERE id = '08LA001';
update cabd.nhn_workunit set name_fr = 'Clearwater (C.-B.)' WHERE id = '08LA002';
update cabd.nhn_workunit set name_fr = 'Clearwater (C.-B.)' WHERE id = '08LA003';
update cabd.nhn_workunit set name_fr = 'Clearwater (C.-B.)' WHERE id = '08LA004';
update cabd.nhn_workunit set name_fr = 'Thompson Nord' WHERE id = '08LB001';
update cabd.nhn_workunit set name_fr = 'Thompson Nord' WHERE id = '08LB002';
update cabd.nhn_workunit set name_fr = 'Shuswap' WHERE id = '08LC000';
update cabd.nhn_workunit set name_fr = 'Adams' WHERE id = '08LD000';
update cabd.nhn_workunit set name_fr = 'Thompson Sud' WHERE id = '08LE001';
update cabd.nhn_workunit set name_fr = 'Thompson Sud' WHERE id = '08LE002';
update cabd.nhn_workunit set name_fr = 'Thompson - Embouchure' WHERE id = '08LF001';
update cabd.nhn_workunit set name_fr = 'Thompson - Embouchure' WHERE id = '08LF002';
update cabd.nhn_workunit set name_fr = 'Thompson - Embouchure' WHERE id = '08LF003';
update cabd.nhn_workunit set name_fr = 'Thompson - Embouchure' WHERE id = '08LF004';
update cabd.nhn_workunit set name_fr = 'Nicola' WHERE id = '08LG001';
update cabd.nhn_workunit set name_fr = 'Nicola' WHERE id = '08LG002';
update cabd.nhn_workunit set name_fr = 'Nicola' WHERE id = '08LG003';
update cabd.nhn_workunit set name_fr = 'Chilko' WHERE id = '08MA001';
update cabd.nhn_workunit set name_fr = 'Chilko' WHERE id = '08MA002';
update cabd.nhn_workunit set name_fr = 'Chilcotin' WHERE id = '08MB001';
update cabd.nhn_workunit set name_fr = 'Chilcotin' WHERE id = '08MB003';
update cabd.nhn_workunit set name_fr = 'Chilcotin' WHERE id = '08MB0X2';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Lac William' WHERE id = '08MC001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Lac William' WHERE id = '08MC003';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Lac William' WHERE id = '08MC0X2';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Dog' WHERE id = '08MD001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Dog' WHERE id = '08MD002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Bridge' WHERE id = '08MEAX0';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Nahatlatch' WHERE id = '08MF001';
update cabd.nhn_workunit set name_fr = 'Harrison' WHERE id = '08MG001';
update cabd.nhn_workunit set name_fr = 'Harrison' WHERE id = '08MG0X2';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Côte' WHERE id = '08MHA00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Côte' WHERE id = '08MHBX1';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Côte' WHERE id = '08MHE00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Fraser - Côte' WHERE id = '08MHF00';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont du Columbia' WHERE id = '08NA001';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont du Columbia' WHERE id = '08NA002';
update cabd.nhn_workunit set name_fr = 'Lac McNaughton - Passage du Columbia' WHERE id = '08NB000';
update cabd.nhn_workunit set name_fr = 'Lac McNaughton - Passage de la Canoe' WHERE id = '08NC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Columbia' WHERE id = '08ND000';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Columbia' WHERE id = '08NE002';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Columbia' WHERE id = '08NEA00';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Columbia' WHERE id = '08NEB00';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Columbia' WHERE id = '08NEC00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Kootenay' WHERE id = '08NF000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Kootenay' WHERE id = '08NG001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Kootenay' WHERE id = '08NGB00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kootenay' WHERE id = '08NH002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kootenay' WHERE id = '08NHA00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kootenay' WHERE id = '08NHB00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Kootenay' WHERE id = '08NHC00';
update cabd.nhn_workunit set name_fr = 'Slocan' WHERE id = '08NJ000';
update cabd.nhn_workunit set name_fr = 'Elk' WHERE id = '08NK000';
update cabd.nhn_workunit set name_fr = 'Similkameen' WHERE id = '08NL000';
update cabd.nhn_workunit set name_fr = 'Okanagan' WHERE id = '08NM000';
update cabd.nhn_workunit set name_fr = 'Kettle' WHERE id = '08NN000';
update cabd.nhn_workunit set name_fr = 'Flathead' WHERE id = '08NP000';
update cabd.nhn_workunit set name_fr = 'Île Graham' WHERE id = '08OA000';
update cabd.nhn_workunit set name_fr = 'Île Moresby' WHERE id = '08OB000';
update cabd.nhn_workunit set name_fr = 'Skagit (C.-B.-Wash.)' WHERE id = '08PA000';
update cabd.nhn_workunit set name_fr = 'Tagish' WHERE id = '09AAA00';
update cabd.nhn_workunit set name_fr = 'Tagish' WHERE id = '09AAB00';
update cabd.nhn_workunit set name_fr = 'Tagish' WHERE id = '09AAC00';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont du Yukon - Lac Laberge' WHERE id = '09AB000';
update cabd.nhn_workunit set name_fr = 'Takhini' WHERE id = '09AC000';
update cabd.nhn_workunit set name_fr = 'Nisutlin' WHERE id = '09AD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Teslin' WHERE id = '09AE000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Teslin' WHERE id = '09AF000';
update cabd.nhn_workunit set name_fr = 'Big Salmon' WHERE id = '09AG000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont du Yukon - Nordenskiold' WHERE id = '09AH000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Pelly' WHERE id = '09BA000';
update cabd.nhn_workunit set name_fr = 'Macmillan' WHERE id = '09BB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Pelly' WHERE id = '09BC000';
update cabd.nhn_workunit set name_fr = 'Donjek' WHERE id = '09CA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la White' WHERE id = '09CB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la White' WHERE id = '09CC000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Yukon - White' WHERE id = '09CD000';
update cabd.nhn_workunit set name_fr = 'Hess' WHERE id = '09DA000';
update cabd.nhn_workunit set name_fr = 'Beaver (Yn)' WHERE id = '09DB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Stewart' WHERE id = '09DC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Stewart' WHERE id = '09DD000';
update cabd.nhn_workunit set name_fr = 'Klondike' WHERE id = '09EA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Yukon - Sixty Mile' WHERE id = '09EB000';
update cabd.nhn_workunit set name_fr = 'Fortymile' WHERE id = '09EC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Yukon - Tatonduk' WHERE id = '09ED000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Porcupine' WHERE id = '09FA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Porcupine - Bell' WHERE id = '09FB000';
update cabd.nhn_workunit set name_fr = 'Old Crow' WHERE id = '09FC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Porcupine - Coleen' WHERE id = '09FDA00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Porcupine - Embouchure' WHERE id = '09FEA00';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Tanana' WHERE id = '09HA000';
update cabd.nhn_workunit set name_fr = 'Chitina - É.-U.' WHERE id = '09MAA00';
update cabd.nhn_workunit set name_fr = 'Chitina - É.-U.' WHERE id = '09MAB00';
update cabd.nhn_workunit set name_fr = '' WHERE id = '1001000';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Liard' WHERE id = '10AA001';
update cabd.nhn_workunit set name_fr = 'Eaux d''amont de la Liard' WHERE id = '10AA002';
update cabd.nhn_workunit set name_fr = 'Frances' WHERE id = '10AB000';
update cabd.nhn_workunit set name_fr = 'Dease' WHERE id = '10AC003';
update cabd.nhn_workunit set name_fr = 'Dease' WHERE id = '10AC004';
update cabd.nhn_workunit set name_fr = 'Dease' WHERE id = '10AC005';
update cabd.nhn_workunit set name_fr = 'Dease' WHERE id = '10AC0X1';
update cabd.nhn_workunit set name_fr = 'Dease' WHERE id = '10AC0X2';
update cabd.nhn_workunit set name_fr = 'Hyland' WHERE id = '10AD000';
update cabd.nhn_workunit set name_fr = 'Turnagain' WHERE id = '10BA000';
update cabd.nhn_workunit set name_fr = 'Kechika' WHERE id = '10BB001';
update cabd.nhn_workunit set name_fr = 'Kechika' WHERE id = '10BB002';
update cabd.nhn_workunit set name_fr = 'Kechika' WHERE id = '10BB003';
update cabd.nhn_workunit set name_fr = 'Kechika' WHERE id = '10BB004';
update cabd.nhn_workunit set name_fr = 'Coal' WHERE id = '10BC000';
update cabd.nhn_workunit set name_fr = 'Beaver (Yn-C.-B.)' WHERE id = '10BD000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Liard - Toad' WHERE id = '10BE001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Liard - Toad' WHERE id = '10BE002';
update cabd.nhn_workunit set name_fr = 'Fontas' WHERE id = '10CA000';
update cabd.nhn_workunit set name_fr = 'Sikanni Chief' WHERE id = '10CB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Fort Nelson' WHERE id = '10CC000';
update cabd.nhn_workunit set name_fr = 'Muskwa' WHERE id = '10CD000';
update cabd.nhn_workunit set name_fr = 'Sahtaneh' WHERE id = '10CE0X0';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Fort Nelson' WHERE id = '10CF000';
update cabd.nhn_workunit set name_fr = 'Petitot' WHERE id = '10DA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Liard - La Biche' WHERE id = '10DB000';
update cabd.nhn_workunit set name_fr = 'Flat' WHERE id = '10EA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Nahanni Sud' WHERE id = '10EB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Nahanni Sud' WHERE id = '10EC000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Liard - Embouchure' WHERE id = '10ED000';
update cabd.nhn_workunit set name_fr = 'Trout' WHERE id = '10FA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Mackenzie - Jean Marie' WHERE id = '10FB000';
update cabd.nhn_workunit set name_fr = 'Horn' WHERE id = '10FC000';
update cabd.nhn_workunit set name_fr = 'Root' WHERE id = '10GA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Mackenzie - Willowlake' WHERE id = '10GB000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur du Mackenzie - Martin' WHERE id = '10GC000';
update cabd.nhn_workunit set name_fr = 'Nahanni Nord' WHERE id = '10GD000';
update cabd.nhn_workunit set name_fr = 'Keele' WHERE id = '10HA000';
update cabd.nhn_workunit set name_fr = 'Redstone' WHERE id = '10HB000';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Mackenzie - Blackwater' WHERE id = '10HC000';
update cabd.nhn_workunit set name_fr = '' WHERE id = '10J1000';
update cabd.nhn_workunit set name_fr = 'Grand lac de l''Ours - Camsell' WHERE id = '10JA001';
update cabd.nhn_workunit set name_fr = 'Grand lac de l''Ours - Camsell' WHERE id = '10JA002';
update cabd.nhn_workunit set name_fr = 'Grand lac de l''Ours - Johnny Ho' WHERE id = '10JB001';
update cabd.nhn_workunit set name_fr = 'Grand lac de l''Ours - Johnny Ho' WHERE id = '10JB002';
update cabd.nhn_workunit set name_fr = 'Great Bear - Embouchure' WHERE id = '10JC000';
update cabd.nhn_workunit set name_fr = 'Grand lac de l''Ours - Nord-ouest' WHERE id = '10JD001';
update cabd.nhn_workunit set name_fr = 'Grand lac de l''Ours - Nord-ouest' WHERE id = '10JD002';
update cabd.nhn_workunit set name_fr = 'Grand lac de l''Ours - Nord-est' WHERE id = '10JE000';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Mackenzie -  Petite rivière Bear' WHERE id = '10KA000';
update cabd.nhn_workunit set name_fr = 'Carcajou' WHERE id = '10KB000';
update cabd.nhn_workunit set name_fr = 'Mountain' WHERE id = '10KC000';
update cabd.nhn_workunit set name_fr = 'Cours moyen du Mackenzie - Ramparts' WHERE id = '10KD000';
update cabd.nhn_workunit set name_fr = 'Arctic Red' WHERE id = '10LA000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur du Mackenzie - Ontaratue' WHERE id = '10LB000';
update cabd.nhn_workunit set name_fr = 'Delta est du Mackenzie' WHERE id = '10LC000';
update cabd.nhn_workunit set name_fr = 'Hare Indian' WHERE id = '10LD000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Peel' WHERE id = '10MA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Peel' WHERE id = '10MB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Peel et delta ouest du Mackenzie' WHERE id = '10MC001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Peel et delta ouest du Mackenzie' WHERE id = '10MC002';
update cabd.nhn_workunit set name_fr = 'Sud-ouest de la mer de Beaufort (Yn)' WHERE id = '10MD000';
update cabd.nhn_workunit set name_fr = 'Carnwath' WHERE id = '10NA000';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Anderson' WHERE id = '10NB001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Anderson' WHERE id = '10NB002';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Anderson' WHERE id = '10NC000';
update cabd.nhn_workunit set name_fr = 'Sud de la mer de Beaufort - Lacs Eskimo' WHERE id = '10ND000';
update cabd.nhn_workunit set name_fr = 'Sud-est de la mer de Beaufort' WHERE id = '10NE000';
update cabd.nhn_workunit set name_fr = 'Horton' WHERE id = '10OA001';
update cabd.nhn_workunit set name_fr = 'Horton' WHERE id = '10OA002';
update cabd.nhn_workunit set name_fr = 'Hornaday' WHERE id = '10OB000';
update cabd.nhn_workunit set name_fr = 'Rae' WHERE id = '10OC001';
update cabd.nhn_workunit set name_fr = 'Rae' WHERE id = '10OC002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Coppermine - Lac de Gras' WHERE id = '10PA000';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Coppermine - Lac Point' WHERE id = '10PB000';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Coppermine - Embouchure' WHERE id = '10PC000';
update cabd.nhn_workunit set name_fr = 'Baie du Couronnement - Tree' WHERE id = '10QA001';
update cabd.nhn_workunit set name_fr = 'Baie du Couronnement - Tree' WHERE id = '10QA002';
update cabd.nhn_workunit set name_fr = 'Inlet Bathurst - Hood' WHERE id = '10QB000';
update cabd.nhn_workunit set name_fr = 'Inlet Bathurst - Burnside' WHERE id = '10QC001';
update cabd.nhn_workunit set name_fr = 'Inlet Bathurst - Burnside' WHERE id = '10QC002';
update cabd.nhn_workunit set name_fr = 'Inlet Bathurst - Burnside' WHERE id = '10QC003';
update cabd.nhn_workunit set name_fr = 'Inlet Bathurst - Burnside' WHERE id = '10QC004';
update cabd.nhn_workunit set name_fr = 'Baie de la Reine-Maud - Ellice' WHERE id = '10QD001';
update cabd.nhn_workunit set name_fr = 'Baie de la Reine-Maud - Ellice' WHERE id = '10QD002';
update cabd.nhn_workunit set name_fr = 'Baie de la Reine-Maud - Ellice' WHERE id = '10QD003';
update cabd.nhn_workunit set name_fr = 'Baie de la Reine-Maud - Ellice' WHERE id = '10QD004';
update cabd.nhn_workunit set name_fr = 'Baie de la Reine-Maud - Simpson' WHERE id = '10QE001';
update cabd.nhn_workunit set name_fr = 'Baie de la Reine-Maud - Simpson' WHERE id = '10QE002';
update cabd.nhn_workunit set name_fr = 'Baie de la Reine-Maud - Kaleet' WHERE id = '10QF001';
update cabd.nhn_workunit set name_fr = 'Baie de la Reine-Maud - Kaleet' WHERE id = '10QF002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Back' WHERE id = '10RA001';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Back' WHERE id = '10RA002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Back' WHERE id = '10RA003';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Back' WHERE id = '10RA004';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Back' WHERE id = '10RA005';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Back' WHERE id = '10RB001';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Back' WHERE id = '10RB002';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Back' WHERE id = '10RB003';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Back' WHERE id = '10RC001';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Back' WHERE id = '10RC002';
update cabd.nhn_workunit set name_fr = 'Hayes (Nt)' WHERE id = '10RD001';
update cabd.nhn_workunit set name_fr = 'Hayes (Nt)' WHERE id = '10RD002';
update cabd.nhn_workunit set name_fr = 'Bassins St. Roch et Rasmussen' WHERE id = '10SA001';
update cabd.nhn_workunit set name_fr = 'Bassins St. Roch et Rasmussen' WHERE id = '10SA002';
update cabd.nhn_workunit set name_fr = 'Ouest de la péninsule de Boothia' WHERE id = '10SB000';
update cabd.nhn_workunit set name_fr = 'Est de la péninsule de Boothia' WHERE id = '10SC000';
update cabd.nhn_workunit set name_fr = 'Baie Pelly' WHERE id = '10SD001';
update cabd.nhn_workunit set name_fr = 'Baie Pelly' WHERE id = '10SD002';
update cabd.nhn_workunit set name_fr = 'Baie Comité' WHERE id = '10SE001';
update cabd.nhn_workunit set name_fr = 'Baie Comité' WHERE id = '10SE002';
update cabd.nhn_workunit set name_fr = 'Ouest de l''île Banks' WHERE id = '10TA001';
update cabd.nhn_workunit set name_fr = 'Ouest de l''île Banks' WHERE id = '10TA002';
update cabd.nhn_workunit set name_fr = 'Ouest de l''île Banks' WHERE id = '10TA003';
update cabd.nhn_workunit set name_fr = 'Ouest de l''île Banks' WHERE id = '10TA004';
update cabd.nhn_workunit set name_fr = 'Est de l''île Banks' WHERE id = '10TB001';
update cabd.nhn_workunit set name_fr = 'Est de l''île Banks' WHERE id = '10TB002';
update cabd.nhn_workunit set name_fr = 'Nord-ouest de l''île Victoria' WHERE id = '10TC001';
update cabd.nhn_workunit set name_fr = 'Nord-ouest de l''île Victoria' WHERE id = '10TC002';
update cabd.nhn_workunit set name_fr = 'Île Victoria - Baie Hadley' WHERE id = '10TD001';
update cabd.nhn_workunit set name_fr = 'Île Victoria - Baie Hadley' WHERE id = '10TD002';
update cabd.nhn_workunit set name_fr = 'Est de l''île Victoria' WHERE id = '10TE001';
update cabd.nhn_workunit set name_fr = 'Est de l''île Victoria' WHERE id = '10TE002';
update cabd.nhn_workunit set name_fr = 'Est de l''île Victoria' WHERE id = '10TE003';
update cabd.nhn_workunit set name_fr = 'Est de l''île Victoria' WHERE id = '10TE004';
update cabd.nhn_workunit set name_fr = 'Est de l''île Victoria' WHERE id = '10TE005';
update cabd.nhn_workunit set name_fr = 'Est de l''île Victoria' WHERE id = '10TE006';
update cabd.nhn_workunit set name_fr = 'Est de l''île Victoria' WHERE id = '10TE007';
update cabd.nhn_workunit set name_fr = 'Est de l''île Victoria' WHERE id = '10TE008';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Victoria' WHERE id = '10TF001';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Victoria' WHERE id = '10TF002';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Victoria' WHERE id = '10TF003';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Victoria' WHERE id = '10TF004';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Victoria' WHERE id = '10TF005';
update cabd.nhn_workunit set name_fr = 'Sud de l''île Victoria' WHERE id = '10TF006';
update cabd.nhn_workunit set name_fr = 'Île Victoria - Détroit de Prince-Albert' WHERE id = '10TG001';
update cabd.nhn_workunit set name_fr = 'Île Victoria - Détroit de Prince-Albert' WHERE id = '10TG002';
update cabd.nhn_workunit set name_fr = 'Île Victoria - Île Minto' WHERE id = '10TH000';
update cabd.nhn_workunit set name_fr = 'Île du Roi-Guillaume' WHERE id = '10TJ001';
update cabd.nhn_workunit set name_fr = 'Île du Roi-Guillaume' WHERE id = '10TJ002';
update cabd.nhn_workunit set name_fr = 'Ouest de l''île Prince-de-Galles' WHERE id = '10TK000';
update cabd.nhn_workunit set name_fr = 'Est de l''île Prince-de-Galles' WHERE id = '10TL000';
update cabd.nhn_workunit set name_fr = 'Ouest de l''île Somerset' WHERE id = '10TM000';
update cabd.nhn_workunit set name_fr = 'Est de l''île Somerset' WHERE id = '10TN000';
update cabd.nhn_workunit set name_fr = 'Ouest de la presqu''île Brodeur' WHERE id = '10UA001';
update cabd.nhn_workunit set name_fr = 'Ouest de la presqu''île Brodeur' WHERE id = '10UA002';
update cabd.nhn_workunit set name_fr = 'Inlet Admiralty' WHERE id = '10UB001';
update cabd.nhn_workunit set name_fr = 'Inlet Admiralty' WHERE id = '10UB002';
update cabd.nhn_workunit set name_fr = 'Détroit d''Éclipse' WHERE id = '10UC000';
update cabd.nhn_workunit set name_fr = 'Sud-ouest de l''île de Baffin' WHERE id = '10UD001';
update cabd.nhn_workunit set name_fr = 'Sud-ouest de l''île de Baffin' WHERE id = '10UD002';
update cabd.nhn_workunit set name_fr = 'Nord-ouest du détroit de Davis' WHERE id = '10UE001';
update cabd.nhn_workunit set name_fr = 'Nord-ouest du détroit de Davis' WHERE id = '10UE002';
update cabd.nhn_workunit set name_fr = 'Nord de la baie Cumberland' WHERE id = '10UF000';
update cabd.nhn_workunit set name_fr = 'Sud de la baie Cumberland' WHERE id = '10UG001';
update cabd.nhn_workunit set name_fr = 'Sud de la baie Cumberland' WHERE id = '10UG002';
update cabd.nhn_workunit set name_fr = 'Baie Frobisher' WHERE id = '10UH001';
update cabd.nhn_workunit set name_fr = 'Baie Frobisher' WHERE id = '10UH002';
update cabd.nhn_workunit set name_fr = 'Île Prince-Patrick' WHERE id = '10VA001';
update cabd.nhn_workunit set name_fr = 'Île Prince-Patrick' WHERE id = '10VA002';
update cabd.nhn_workunit set name_fr = 'Île Melville' WHERE id = '10VB001';
update cabd.nhn_workunit set name_fr = 'Île Melville' WHERE id = '10VB002';
update cabd.nhn_workunit set name_fr = 'Île Melville' WHERE id = '10VB003';
update cabd.nhn_workunit set name_fr = 'Îles Bathurst et Cornwallis' WHERE id = '10VC001';
update cabd.nhn_workunit set name_fr = 'Îles Bathurst et Cornwallis' WHERE id = '10VC002';
update cabd.nhn_workunit set name_fr = 'Ouest de l''île Devon' WHERE id = '10VD001';
update cabd.nhn_workunit set name_fr = 'Ouest de l''île Devon' WHERE id = '10VD002';
update cabd.nhn_workunit set name_fr = 'Est de l''île Devon' WHERE id = '10VE001';
update cabd.nhn_workunit set name_fr = 'Est de l''île Devon' WHERE id = '10VE002';
update cabd.nhn_workunit set name_fr = 'Îles Sverdrup' WHERE id = '10VF001';
update cabd.nhn_workunit set name_fr = 'Îles Sverdrup' WHERE id = '10VF002';
update cabd.nhn_workunit set name_fr = 'Îles Sverdrup' WHERE id = '10VF003';
update cabd.nhn_workunit set name_fr = 'Îles Sverdrup' WHERE id = '10VF004';
update cabd.nhn_workunit set name_fr = 'Détroits de Nansen et d''Eureka' WHERE id = '10VG001';
update cabd.nhn_workunit set name_fr = 'Détroits de Nansen et d''Eureka' WHERE id = '10VG002';
update cabd.nhn_workunit set name_fr = 'Détroits de Nansen et d''Eureka' WHERE id = '10VG003';
update cabd.nhn_workunit set name_fr = 'Détroits de Nansen et d''Eureka' WHERE id = '10VG004';
update cabd.nhn_workunit set name_fr = 'Fjord Greely' WHERE id = '10VH001';
update cabd.nhn_workunit set name_fr = 'Fjord Greely' WHERE id = '10VH002';
update cabd.nhn_workunit set name_fr = 'Océan Arctique et mer de Lincoln' WHERE id = '10VJ000';
update cabd.nhn_workunit set name_fr = 'Nord-est de l''île d''Ellesmere' WHERE id = '10VK001';
update cabd.nhn_workunit set name_fr = 'Nord-est de l''île d''Ellesmere' WHERE id = '10VK002';
update cabd.nhn_workunit set name_fr = 'Sud-est de l''île d''Ellesmere' WHERE id = '10VL001';
update cabd.nhn_workunit set name_fr = 'Sud-est de l''île d''Ellesmere' WHERE id = '10VL002';
update cabd.nhn_workunit set name_fr = 'Sud de l''île d''Ellesmere' WHERE id = '10VM001';
update cabd.nhn_workunit set name_fr = 'Sud de l''île d''Ellesmere' WHERE id = '10VM002';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Milk' WHERE id = '11AAA00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Milk' WHERE id = '11AAB00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Milk' WHERE id = '11AAC00';
update cabd.nhn_workunit set name_fr = 'Cours supérieur de la Milk' WHERE id = '11AAD00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Milk' WHERE id = '11ABA00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Milk' WHERE id = '11ABC00';
update cabd.nhn_workunit set name_fr = 'Cours moyen de la Milk' WHERE id = '11ABD00';
update cabd.nhn_workunit set name_fr = 'Frenchman' WHERE id = '11AC000';
update cabd.nhn_workunit set name_fr = 'Whitewater' WHERE id = '11ADA00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Milk et Poplar' WHERE id = '11AEA00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Milk et Poplar' WHERE id = '11AEB00';
update cabd.nhn_workunit set name_fr = 'Cours inférieur de la Milk et Poplar' WHERE id = '11AEC00';
update cabd.nhn_workunit set name_fr = 'Big Muddy' WHERE id = '11AFA00';
update cabd.nhn_workunit set name_fr = 'Big Muddy' WHERE id = '11AFB00';




--NAME Search metadata
alter table cabd.feature_type_metadata add column is_name_search boolean default false;

update cabd.feature_type_metadata set is_name_search = true 
where 
(view_name = 'cabd.all_features_view' and field_name in ('name_en', 'name_fr')) 
or (view_name = 'cabd.barriers_view' and field_name in ('name_en', 'name_fr')) 
or (view_name = 'cabd.dams_view' and field_name in ('dam_name_en', 'dam_name_fr')) 
or (view_name = 'cabd.fishways_view' and field_name in ('structure_name_en', 'structure_name_fr'))
or (view_name = 'cabd.waterfalls_view' and field_name in ('fall_name_en', 'fall_name_fr'))
                    





alter table medium.code_attribute_1 rename column name to name_en;
alter table medium.code_attribute_1 rename column description to description_en;
alter table medium.code_attribute_1 add column name_fr varchar(64);
alter table medium.code_attribute_1 add column description_fr text;
alter table medium.code_attribute_2 rename column name to name_en;
alter table medium.code_attribute_2 rename column description to description_en;
alter table medium.code_attribute_2 add column name_fr varchar(64);
alter table medium.code_attribute_2 add column description_fr text;
alter table medium.code_attribute_3 rename column name to name_en;
alter table medium.code_attribute_3 rename column description to description_en;
alter table medium.code_attribute_3 add column name_fr varchar(64);
alter table medium.code_attribute_3 add column description_fr text;
alter table medium.code_attribute_4 rename column name to name_en;
alter table medium.code_attribute_4 rename column description to description_en;
alter table medium.code_attribute_4 add column name_fr varchar(64);
alter table medium.code_attribute_4 add column description_fr text;
alter table medium.code_attribute_5 rename column name to name_en;
alter table medium.code_attribute_5 rename column description to description_en;
alter table medium.code_attribute_5 add column name_fr varchar(64);
alter table medium.code_attribute_5 add column description_fr text;
alter table medium.code_attribute_6 rename column name to name_en;
alter table medium.code_attribute_6 rename column description to description_en;
alter table medium.code_attribute_6 add column name_fr varchar(64);
alter table medium.code_attribute_6 add column description_fr text;
alter table medium.code_attribute_7 rename column name to name_en;
alter table medium.code_attribute_7 rename column description to description_en;
alter table medium.code_attribute_7 add column name_fr varchar(64);
alter table medium.code_attribute_7 add column description_fr text;
alter table medium.code_attribute_8 rename column name to name_en;
alter table medium.code_attribute_8 rename column description to description_en;
alter table medium.code_attribute_8 add column name_fr varchar(64);
alter table medium.code_attribute_8 add column description_fr text;
alter table medium.code_attribute_9 rename column name to name_en;
alter table medium.code_attribute_9 rename column description to description_en;
alter table medium.code_attribute_9 add column name_fr varchar(64);
alter table medium.code_attribute_9 add column description_fr text;
alter table medium.code_attribute_10 rename column name to name_en;
alter table medium.code_attribute_10 rename column description to description_en;
alter table medium.code_attribute_10 add column name_fr varchar(64);
alter table medium.code_attribute_10 add column description_fr text;

update medium.code_attribute_1 set name_fr = name_en, description_fr = description_en;
update medium.code_attribute_2 set name_fr = name_en, description_fr = description_en;
update medium.code_attribute_3 set name_fr = name_en, description_fr = description_en;
update medium.code_attribute_4 set name_fr = name_en, description_fr = description_en;
update medium.code_attribute_5 set name_fr = name_en, description_fr = description_en;
update medium.code_attribute_6 set name_fr = name_en, description_fr = description_en;
update medium.code_attribute_7 set name_fr = name_en, description_fr = description_en;
update medium.code_attribute_8 set name_fr = name_en, description_fr = description_en;
update medium.code_attribute_9 set name_fr = name_en, description_fr = description_en;
update medium.code_attribute_10 set name_fr = name_en, description_fr = description_en;

alter table big.code_attribute_1 rename column name to name_en;
alter table big.code_attribute_1 rename column description to description_en;
alter table big.code_attribute_1 add column name_fr varchar(64);
alter table big.code_attribute_1 add column description_fr text;
alter table big.code_attribute_2 rename column name to name_en;
alter table big.code_attribute_2 rename column description to description_en;
alter table big.code_attribute_2 add column name_fr varchar(64);
alter table big.code_attribute_2 add column description_fr text;
alter table big.code_attribute_3 rename column name to name_en;
alter table big.code_attribute_3 rename column description to description_en;
alter table big.code_attribute_3 add column name_fr varchar(64);
alter table big.code_attribute_3 add column description_fr text;
alter table big.code_attribute_4 rename column name to name_en;
alter table big.code_attribute_4 rename column description to description_en;
alter table big.code_attribute_4 add column name_fr varchar(64);
alter table big.code_attribute_4 add column description_fr text;
alter table big.code_attribute_5 rename column name to name_en;
alter table big.code_attribute_5 rename column description to description_en;
alter table big.code_attribute_5 add column name_fr varchar(64);
alter table big.code_attribute_5 add column description_fr text;
alter table big.code_attribute_6 rename column name to name_en;
alter table big.code_attribute_6 rename column description to description_en;
alter table big.code_attribute_6 add column name_fr varchar(64);
alter table big.code_attribute_6 add column description_fr text;
alter table big.code_attribute_7 rename column name to name_en;
alter table big.code_attribute_7 rename column description to description_en;
alter table big.code_attribute_7 add column name_fr varchar(64);
alter table big.code_attribute_7 add column description_fr text;
alter table big.code_attribute_8 rename column name to name_en;
alter table big.code_attribute_8 rename column description to description_en;
alter table big.code_attribute_8 add column name_fr varchar(64);
alter table big.code_attribute_8 add column description_fr text;
alter table big.code_attribute_9 rename column name to name_en;
alter table big.code_attribute_9 rename column description to description_en;
alter table big.code_attribute_9 add column name_fr varchar(64);
alter table big.code_attribute_9 add column description_fr text;
alter table big.code_attribute_10 rename column name to name_en;
alter table big.code_attribute_10 rename column description to description_en;
alter table big.code_attribute_10 add column name_fr varchar(64);
alter table big.code_attribute_10 add column description_fr text;

update big.code_attribute_1 set name_fr = name_en, description_fr = description_en;
update big.code_attribute_2 set name_fr = name_en, description_fr = description_en;
update big.code_attribute_3 set name_fr = name_en, description_fr = description_en;
update big.code_attribute_4 set name_fr = name_en, description_fr = description_en;
update big.code_attribute_5 set name_fr = name_en, description_fr = description_en;
update big.code_attribute_6 set name_fr = name_en, description_fr = description_en;
update big.code_attribute_7 set name_fr = name_en, description_fr = description_en;
update big.code_attribute_8 set name_fr = name_en, description_fr = description_en;
update big.code_attribute_9 set name_fr = name_en, description_fr = description_en;
update big.code_attribute_10 set name_fr = name_en, description_fr = description_en;

update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_1;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_1_name';
update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_2;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_2_name';
update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_3;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_3_name';
update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_4;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_4_name';
update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_5;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_5_name';
update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_6;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_6_name';
update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_7;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_7_name';
update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_8;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_8_name';
update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_9;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_9_name';
update cabd.feature_type_metadata set value_options_reference = 'medium.code_attribute_10;;name;description' where view_name = 'cabd.medium_view' and field_name = 'code_attribute_10_name';

update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_1;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_1_name';
update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_2;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_2_name';
update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_3;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_3_name';
update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_4;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_4_name';
update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_5;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_5_name';
update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_6;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_6_name';
update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_7;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_7_name';
update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_8;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_8_name';
update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_9;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_9_name';
update cabd.feature_type_metadata set value_options_reference = 'big.code_attribute_10;;name;description' where view_name = 'cabd.big_view' and field_name = 'code_attribute_10_name';



-- cabd.big_view source
drop view cabd.big_view;

CREATE OR REPLACE VIEW cabd.big_view_en
AS SELECT 'big'::text AS feature_type,
    d.cabd_id,
    st_y(d.snapped_point) AS latitude,
    st_x(d.snapped_point) AS longitude,
    d.name_en,
    d.name_fr,
    d.nhn_workunit_id,
    nhn.sub_sub_drainage_area,
    d.province_territory_code,
    pt.name_en AS province_territory,
    d.passability_status_code,
    ps.name_en AS passability_status,
    d.up_passage_type_code,
    up.name_en AS up_passage_type,
    d.code_attribute_1,
    d1.name_en AS code_attribute_1_name,
    d.code_attribute_2,
    d2.name_en AS code_attribute_2_name,
    d.code_attribute_3,
    d3.name_en AS code_attribute_3_name,
    d.code_attribute_4,
    d4.name_en AS code_attribute_4_name,
    d.code_attribute_5,
    d5.name_en AS code_attribute_5_name,
    d.code_attribute_6,
    d6.name_en AS code_attribute_6_name,
    d.code_attribute_7,
    d7.name_en AS code_attribute_7_name,
    d.code_attribute_8,
    d8.name_en AS code_attribute_8_name,
    d.code_attribute_9,
    d9.name_en AS code_attribute_9_name,
    d.code_attribute_10,
    d10.name_en AS code_attribute_10_name,
    d.number_attribute_1,
    d.number_attribute_2,
    d.number_attribute_3,
    d.number_attribute_4,
    d.number_attribute_5,
    d.number_attribute_6,
    d.number_attribute_7,
    d.number_attribute_8,
    d.number_attribute_9,
    d.number_attribute_10,
    d.text_attribute_1,
    d.text_attribute_2,
    d.text_attribute_3,
    d.text_attribute_4,
    d.text_attribute_5,
    d.text_attribute_6,
    d.text_attribute_7,
    d.text_attribute_8,
    d.text_attribute_9,
    d.text_attribute_10,
    d.date_attribute_1,
    d.date_attribute_2,
    d.date_attribute_3,
    d.date_attribute_4,
    d.date_attribute_5,
    d.date_attribute_6,
    d.date_attribute_7,
    d.date_attribute_8,
    d.date_attribute_9,
    d.date_attribute_10,
    d.snapped_point AS geometry
   FROM big.big d
     JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_workunit_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code
     LEFT JOIN big.code_attribute_1 d1 ON d1.code = d.code_attribute_1
     LEFT JOIN big.code_attribute_2 d2 ON d2.code = d.code_attribute_2
     LEFT JOIN big.code_attribute_3 d3 ON d3.code = d.code_attribute_3
     LEFT JOIN big.code_attribute_4 d4 ON d4.code = d.code_attribute_4
     LEFT JOIN big.code_attribute_5 d5 ON d5.code = d.code_attribute_5
     LEFT JOIN big.code_attribute_6 d6 ON d6.code = d.code_attribute_6
     LEFT JOIN big.code_attribute_7 d7 ON d7.code = d.code_attribute_7
     LEFT JOIN big.code_attribute_8 d8 ON d8.code = d.code_attribute_8
     LEFT JOIN big.code_attribute_9 d9 ON d9.code = d.code_attribute_9
     LEFT JOIN big.code_attribute_10 d10 ON d10.code = d.code_attribute_10;
     
     

CREATE OR REPLACE VIEW cabd.big_view_fr
AS SELECT 'big'::text AS feature_type,
    d.cabd_id,
    st_y(d.snapped_point) AS latitude,
    st_x(d.snapped_point) AS longitude,
    d.name_en,
    d.name_fr,
    d.nhn_workunit_id,
    nhn.sub_sub_drainage_area,
    d.province_territory_code,
    pt.name_fr AS province_territory,
    d.passability_status_code,
    ps.name_fr AS passability_status,
    d.up_passage_type_code,
    up.name_fr AS up_passage_type,
    d.code_attribute_1,
    d1.name_fr AS code_attribute_1_name,
    d.code_attribute_2,
    d2.name_fr AS code_attribute_2_name,
    d.code_attribute_3,
    d3.name_fr AS code_attribute_3_name,
    d.code_attribute_4,
    d4.name_fr AS code_attribute_4_name,
    d.code_attribute_5,
    d5.name_fr AS code_attribute_5_name,
    d.code_attribute_6,
    d6.name_fr AS code_attribute_6_name,
    d.code_attribute_7,
    d7.name_fr AS code_attribute_7_name,
    d.code_attribute_8,
    d8.name_fr AS code_attribute_8_name,
    d.code_attribute_9,
    d9.name_fr AS code_attribute_9_name,
    d.code_attribute_10,
    d10.name_fr AS code_attribute_10_name,
    d.number_attribute_1,
    d.number_attribute_2,
    d.number_attribute_3,
    d.number_attribute_4,
    d.number_attribute_5,
    d.number_attribute_6,
    d.number_attribute_7,
    d.number_attribute_8,
    d.number_attribute_9,
    d.number_attribute_10,
    d.text_attribute_1,
    d.text_attribute_2,
    d.text_attribute_3,
    d.text_attribute_4,
    d.text_attribute_5,
    d.text_attribute_6,
    d.text_attribute_7,
    d.text_attribute_8,
    d.text_attribute_9,
    d.text_attribute_10,
    d.date_attribute_1,
    d.date_attribute_2,
    d.date_attribute_3,
    d.date_attribute_4,
    d.date_attribute_5,
    d.date_attribute_6,
    d.date_attribute_7,
    d.date_attribute_8,
    d.date_attribute_9,
    d.date_attribute_10,
    d.snapped_point AS geometry
   FROM big.big d
     JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_workunit_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code
     LEFT JOIN big.code_attribute_1 d1 ON d1.code = d.code_attribute_1
     LEFT JOIN big.code_attribute_2 d2 ON d2.code = d.code_attribute_2
     LEFT JOIN big.code_attribute_3 d3 ON d3.code = d.code_attribute_3
     LEFT JOIN big.code_attribute_4 d4 ON d4.code = d.code_attribute_4
     LEFT JOIN big.code_attribute_5 d5 ON d5.code = d.code_attribute_5
     LEFT JOIN big.code_attribute_6 d6 ON d6.code = d.code_attribute_6
     LEFT JOIN big.code_attribute_7 d7 ON d7.code = d.code_attribute_7
     LEFT JOIN big.code_attribute_8 d8 ON d8.code = d.code_attribute_8
     LEFT JOIN big.code_attribute_9 d9 ON d9.code = d.code_attribute_9
     LEFT JOIN big.code_attribute_10 d10 ON d10.code = d.code_attribute_10;
     
     
     
     
     
     
     
     
     
     
     
     
-- cabd.medium_view source
drop view cabd.medium_view;

CREATE OR REPLACE VIEW cabd.medium_view_en
AS SELECT 'medium'::text AS feature_type,
    d.cabd_id,
    st_y(d.snapped_point) AS latitude,
    st_x(d.snapped_point) AS longitude,
    d.name_en,
    d.name_fr,
    d.nhn_workunit_id,
    nhn.sub_sub_drainage_area,
    d.province_territory_code,
    pt.name_en AS province_territory,
    d.passability_status_code,
    ps.name_en AS passability_status,
    d.up_passage_type_code,
    up.name_en AS up_passage_type,
    d.code_attribute_1,
    d1.name_en AS code_attribute_1_name,
    d.code_attribute_2,
    d2.name_en AS code_attribute_2_name,
    d.code_attribute_3,
    d3.name_en AS code_attribute_3_name,
    d.code_attribute_4,
    d4.name_en AS code_attribute_4_name,
    d.code_attribute_5,
    d5.name_en AS code_attribute_5_name,
    d.code_attribute_6,
    d6.name_en AS code_attribute_6_name,
    d.code_attribute_7,
    d7.name_en AS code_attribute_7_name,
    d.code_attribute_8,
    d8.name_en AS code_attribute_8_name,
    d.code_attribute_9,
    d9.name_en AS code_attribute_9_name,
    d.code_attribute_10,
    d10.name_en AS code_attribute_10_name,
    d.number_attribute_1,
    d.number_attribute_2,
    d.number_attribute_3,
    d.number_attribute_4,
    d.number_attribute_5,
    d.number_attribute_6,
    d.number_attribute_7,
    d.number_attribute_8,
    d.number_attribute_9,
    d.number_attribute_10,
    d.text_attribute_1,
    d.text_attribute_2,
    d.text_attribute_3,
    d.text_attribute_4,
    d.text_attribute_5,
    d.text_attribute_6,
    d.text_attribute_7,
    d.text_attribute_8,
    d.text_attribute_9,
    d.text_attribute_10,
    d.date_attribute_1,
    d.date_attribute_2,
    d.date_attribute_3,
    d.date_attribute_4,
    d.date_attribute_5,
    d.date_attribute_6,
    d.date_attribute_7,
    d.date_attribute_8,
    d.date_attribute_9,
    d.date_attribute_10,
    d.snapped_point AS geometry
   FROM medium.medium d
     JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_workunit_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code
     LEFT JOIN medium.code_attribute_1 d1 ON d1.code = d.code_attribute_1
     LEFT JOIN medium.code_attribute_2 d2 ON d2.code = d.code_attribute_2
     LEFT JOIN medium.code_attribute_3 d3 ON d3.code = d.code_attribute_3
     LEFT JOIN medium.code_attribute_4 d4 ON d4.code = d.code_attribute_4
     LEFT JOIN medium.code_attribute_5 d5 ON d5.code = d.code_attribute_5
     LEFT JOIN medium.code_attribute_6 d6 ON d6.code = d.code_attribute_6
     LEFT JOIN medium.code_attribute_7 d7 ON d7.code = d.code_attribute_7
     LEFT JOIN medium.code_attribute_8 d8 ON d8.code = d.code_attribute_8
     LEFT JOIN medium.code_attribute_9 d9 ON d9.code = d.code_attribute_9
     LEFT JOIN medium.code_attribute_10 d10 ON d10.code = d.code_attribute_10;     
     
     

CREATE OR REPLACE VIEW cabd.medium_view_fr
AS SELECT 'medium'::text AS feature_type,
    d.cabd_id,
    st_y(d.snapped_point) AS latitude,
    st_x(d.snapped_point) AS longitude,
    d.name_en,
    d.name_fr,
    d.nhn_workunit_id,
    nhn.sub_sub_drainage_area,
    d.province_territory_code,
    pt.name_fr AS province_territory,
    d.passability_status_code,
    ps.name_fr AS passability_status,
    d.up_passage_type_code,
    up.name_fr AS up_passage_type,
    d.code_attribute_1,
    d1.name_fr AS code_attribute_1_name,
    d.code_attribute_2,
    d2.name_fr AS code_attribute_2_name,
    d.code_attribute_3,
    d3.name_fr AS code_attribute_3_name,
    d.code_attribute_4,
    d4.name_fr AS code_attribute_4_name,
    d.code_attribute_5,
    d5.name_fr AS code_attribute_5_name,
    d.code_attribute_6,
    d6.name_fr AS code_attribute_6_name,
    d.code_attribute_7,
    d7.name_fr AS code_attribute_7_name,
    d.code_attribute_8,
    d8.name_fr AS code_attribute_8_name,
    d.code_attribute_9,
    d9.name_fr AS code_attribute_9_name,
    d.code_attribute_10,
    d10.name_fr AS code_attribute_10_name,
    d.number_attribute_1,
    d.number_attribute_2,
    d.number_attribute_3,
    d.number_attribute_4,
    d.number_attribute_5,
    d.number_attribute_6,
    d.number_attribute_7,
    d.number_attribute_8,
    d.number_attribute_9,
    d.number_attribute_10,
    d.text_attribute_1,
    d.text_attribute_2,
    d.text_attribute_3,
    d.text_attribute_4,
    d.text_attribute_5,
    d.text_attribute_6,
    d.text_attribute_7,
    d.text_attribute_8,
    d.text_attribute_9,
    d.text_attribute_10,
    d.date_attribute_1,
    d.date_attribute_2,
    d.date_attribute_3,
    d.date_attribute_4,
    d.date_attribute_5,
    d.date_attribute_6,
    d.date_attribute_7,
    d.date_attribute_8,
    d.date_attribute_9,
    d.date_attribute_10,
    d.snapped_point AS geometry
   FROM medium.medium d
     JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_workunit_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code
     LEFT JOIN medium.code_attribute_1 d1 ON d1.code = d.code_attribute_1
     LEFT JOIN medium.code_attribute_2 d2 ON d2.code = d.code_attribute_2
     LEFT JOIN medium.code_attribute_3 d3 ON d3.code = d.code_attribute_3
     LEFT JOIN medium.code_attribute_4 d4 ON d4.code = d.code_attribute_4
     LEFT JOIN medium.code_attribute_5 d5 ON d5.code = d.code_attribute_5
     LEFT JOIN medium.code_attribute_6 d6 ON d6.code = d.code_attribute_6
     LEFT JOIN medium.code_attribute_7 d7 ON d7.code = d.code_attribute_7
     LEFT JOIN medium.code_attribute_8 d8 ON d8.code = d.code_attribute_8
     LEFT JOIN medium.code_attribute_9 d9 ON d9.code = d.code_attribute_9
     LEFT JOIN medium.code_attribute_10 d10 ON d10.code = d.code_attribute_10;          
     
update cabd.feature_type_metadata set value_options_reference = 'cabd.upstream_passage_type_codes;;name;description' where view_name = 'cabd.big_view' and field_name = 'up_passage_type';
update cabd.feature_type_metadata set value_options_reference = 'cabd.upstream_passage_type_codes;;name;description' where view_name = 'cabd.medium_view' and field_name = 'up_passage_type';


grant all privileges on cabd.medium_view_en to cabd;
grant all privileges on cabd.medium_view_fr to cabd;   
grant all privileges on cabd.big_view_en to cabd;
grant all privileges on cabd.big_view_fr to cabd;