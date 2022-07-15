alter table cabd.feature_types rename column name to name_en;
alter table cabd.feature_types add column name_fr varchar(256);

alter table cabd.feature_type_metadata rename column name to name_en;
alter table cabd.feature_type_metadata add column name_fr varchar;
alter table cabd.feature_type_metadata rename column description to description_en;
alter table cabd.feature_type_metadata add column description_fr varchar;


alter table cabd.barrier_ownership_type_codes rename column name to name_en;
alter table cabd.barrier_ownership_type_codes rename column description to description_en;
alter table cabd.barrier_ownership_type_codes add column name_fr varchar(32);
alter table cabd.barrier_ownership_type_codes add column description_fr text;


alter table cabd.passability_status_codes rename column name to name_en;
alter table cabd.passability_status_codes rename column description to description_en;
alter table cabd.passability_status_codes add column name_fr varchar(32);
alter table cabd.passability_status_codes add column description_fr text;

alter table cabd.province_territory_codes  rename column name to name_en;
alter table cabd.province_territory_codes add column name_fr varchar(32);

alter table cabd.upstream_passage_type_codes  rename column name to name_en;
alter table cabd.upstream_passage_type_codes rename column description to description_en;
alter table cabd.upstream_passage_type_codes add column name_fr varchar(32);
alter table cabd.upstream_passage_type_codes add column description_fr text;

alter table dams.condition_codes rename column name to name_en;
alter table dams.condition_codes rename column description to description_en;
alter table dams.condition_codes add column name_fr varchar(32);
alter table dams.condition_codes add column description_fr text;

alter table dams.construction_type_codes rename column name to name_en;
alter table dams.construction_type_codes rename column description to description_en;
alter table dams.construction_type_codes add column name_fr varchar(32);
alter table dams.construction_type_codes add column description_fr text;

alter table dams.dam_complete_level_codes rename column name to name_en;
alter table dams.dam_complete_level_codes rename column description to description_en;
alter table dams.dam_complete_level_codes add column name_fr varchar(32);
alter table dams.dam_complete_level_codes add column description_fr text;

alter table dams.dam_use_codes rename column name to name_en;
alter table dams.dam_use_codes rename column description to description_en;
alter table dams.dam_use_codes add column name_fr varchar(32);
alter table dams.dam_use_codes add column description_fr text;

alter table dams.downstream_passage_route_codes rename column name to name_en;
alter table dams.downstream_passage_route_codes rename column description to description_en;
alter table dams.downstream_passage_route_codes add column name_fr varchar(32);
alter table dams.downstream_passage_route_codes add column description_fr text;

alter table dams.function_codes rename column name to name_en;
alter table dams.function_codes rename column description to description_en;
alter table dams.function_codes add column name_fr varchar(64);
alter table dams.function_codes add column description_fr text;

alter table dams.lake_control_codes rename column name to name_en;
alter table dams.lake_control_codes rename column description to description_en;
alter table dams.lake_control_codes add column name_fr varchar(32);
alter table dams.lake_control_codes add column description_fr text;

alter table dams.operating_status_codes rename column name to name_en;
alter table dams.operating_status_codes rename column description to description_en;
alter table dams.operating_status_codes add column name_fr varchar(32);
alter table dams.operating_status_codes add column description_fr text;

alter table dams.passability_status_codes rename column name to name_en;
alter table dams.passability_status_codes rename column description to description_en;
alter table dams.passability_status_codes add column name_fr varchar(32);
alter table dams.passability_status_codes add column description_fr text;

alter table dams.size_codes rename column name to name_en;
alter table dams.size_codes rename column description to description_en;
alter table dams.size_codes add column name_fr varchar(32);
alter table dams.size_codes add column description_fr text;

alter table dams.spillway_type_codes rename column name to name_en;
alter table dams.spillway_type_codes rename column description to description_en;
alter table dams.spillway_type_codes add column name_fr varchar(32);
alter table dams.spillway_type_codes add column description_fr text;

alter table dams.turbine_type_codes rename column name to name_en;
alter table dams.turbine_type_codes rename column description to description_en;
alter table dams.turbine_type_codes add column name_fr varchar(32);
alter table dams.turbine_type_codes add column description_fr text;

alter table dams.use_codes rename column name to name_en;
alter table dams.use_codes rename column description to description_en;
alter table dams.use_codes add column name_fr varchar(32);
alter table dams.use_codes add column description_fr text;

alter table waterfalls.waterfall_complete_level_codes  rename column name to name_en;
alter table waterfalls.waterfall_complete_level_codes rename column description to description_en;
alter table waterfalls.waterfall_complete_level_codes add column name_fr varchar(32);
alter table waterfalls.waterfall_complete_level_codes add column description_fr text;

alter table fishways.entrance_location_codes rename column name to name_en;
alter table fishways.entrance_location_codes rename column description to description_en;
alter table fishways.entrance_location_codes add column name_fr varchar(32);
alter table fishways.entrance_location_codes add column description_fr text;

alter table fishways.entrance_position_codes rename column name to name_en;
alter table fishways.entrance_position_codes rename column description to description_en;
alter table fishways.entrance_position_codes add column name_fr varchar(32);
alter table fishways.entrance_position_codes add column description_fr text;

alter table fishways.fishway_complete_level_codes rename column name to name_en;
alter table fishways.fishway_complete_level_codes rename column description to description_en;
alter table fishways.fishway_complete_level_codes add column name_fr varchar(32);
alter table fishways.fishway_complete_level_codes add column description_fr text;
alter table cabd.nhn_workunit add column name_en varchar(500);
alter table cabd.nhn_workunit add column name_fr varchar(500);
update cabd.nhn_workunit set name_en = sub_sub_drainage_area ;
update cabd.feature_type_metadata set value_options_reference = 'cabd.nhn_workunit;id;name;' where field_name ='nhn_workunit_id'
update cabd.feature_type_metadata set value_options_reference = 'dams.downstream_passage_route_codes;;name;description' where field_name ='down_passage_route' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'waterfalls.waterfall_complete_level_codes;;name;description' where field_name ='complete_level' and view_name = 'cabd.waterfalls_view';
update cabd.feature_type_metadata set value_options_reference = 'waterfalls.waterfall_complete_level_codes;code;name;description' where field_name ='complete_level_code' and view_name = 'cabd.waterfalls_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.dam_complete_level_codes;;name;description' where field_name ='complete_level' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'fishways.fishway_complete_level_codes;;name;description' where field_name ='complete_level' and view_name = 'cabd.fishways_view'
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
update cabd.feature_type_metadata set value_options_reference = 'dams.turbine_type_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'turbine_type';
update cabd.feature_type_metadata set value_options_reference = 'dams.spillway_type_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'spillway_type';
update cabd.feature_type_metadata set value_options_reference = 'dams.size_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'size_class';


update cabd.nhn_workunit set name_fr = 'FR<' || name_en || '>';
update fishways.fishway_complete_level_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update fishways.entrance_position_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update fishways.entrance_location_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update waterfalls.waterfall_complete_level_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.use_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.turbine_type_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.spillway_type_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.size_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.passability_status_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.operating_status_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.lake_control_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.function_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.downstream_passage_route_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.dam_use_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.dam_complete_level_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.construction_type_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update dams.condition_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update cabd.upstream_passage_type_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>';
update cabd.province_territory_codes set  name_fr = 'FR<' || name_en || '>';
update cabd.passability_status_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>'
update cabd.feature_types set name_fr = 'FR<' || name_en || '>'
update cabd.feature_type_metadata set name_fr = 'FR<' || name_en || '>', description_fr = 'FR<' || description_en || '>'
update cabd.barrier_ownership_type_codes set description_fr = 'FR<' || description_en || '>', name_fr = 'FR<' || name_en || '>'




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
            waterfalls.snapped_point
           FROM waterfalls.waterfalls) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;     
     


-- cabd.all_features_view source

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
                    