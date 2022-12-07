create table cabd.contacts(
  id uuid default gen_random_uuid(),
  email varchar unique not null,
  name varchar not null,
  organization varchar,
  datasource_id uuid references cabd.data_source(id),
  primary key (id)
);

grant all privileges on cabd.contacts to cabd;

alter table cabd.feature_types add column data_version varchar;

update cabd.feature_types set data_version = '1';

create table cabd.user_feature_updates(
 id uuid not null default gen_random_uuid(),
 datetime timestamp without time zone not null default now(),
 contact_id uuid not null,
 cabd_id uuid not null,
 cabd_type varchar not null, 
 user_description varchar,
 user_data_source varchar,
 status varchar,
 primary key(id)
);
alter table cabd.user_feature_updates add constraint user_feature_update_contact_fk foreign key (contact_id) references cabd.contacts(id);
alter table cabd.user_feature_updates add constraint user_feature_update_status_chk CHECK (status IN ('needs_review', 'done'));


--TODO: add other staging tables to this
--create view cabd.updates_pending as 
--select cabd_id from cabd.user_feature_updates;
create view cabd.updates_pending as 
select null::uuid as cabd_id;

 
DROP VIEW cabd.all_features_view_en;
DROP VIEW cabd.all_features_view_fr;
DROP VIEW cabd.barriers_view_en;
DROP VIEW cabd.barriers_view_fr;
DROP VIEW cabd.dams_view_en;
DROP VIEW cabd.dams_view_fr;
DROP VIEW cabd.fishways_view_en;
DROP VIEW cabd.fishways_view_fr;
DROP VIEW cabd.waterfalls_view_en;
DROP VIEW cabd.waterfalls_view_fr;


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
    case when up.cabd_id is not null then true else false end as updates_pending,
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
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up on up.cabd_id = barriers.cabd_id;
     
     
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
    case when up.cabd_id is not null then true else false end as updates_pending,    
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
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up on up.cabd_id = barriers.cabd_id;
     
     
-- cabd.barriers_view_en source

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
    case when up.cabd_id is not null then true else false end as updates_pending,
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
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up on up.cabd_id = barriers.cabd_id;
         
-- cabd.barriers_view_fr source

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
    case when up.cabd_id is not null then true else false end as updates_pending,
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
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up on up.cabd_id = barriers.cabd_id;
     
     

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
    case when upd.cabd_id is not null then true else false end as updates_pending,    
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
     LEFT JOIN cabd.updates_pending upd on upd.cabd_id = d.cabd_id;    
         
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
    case when upd.cabd_id is not null then true else false end as updates_pending,    
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
     LEFT JOIN cabd.updates_pending upd on upd.cabd_id = d.cabd_id;         
     
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
    case when up.cabd_id is not null then true else false end as updates_pending,    
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
          GROUP BY a.fishway_id) sp2 ON sp2.fishway_id = d.cabd_id
     LEFT JOIN cabd.updates_pending up on up.cabd_id = d.cabd_id;             
     
     
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
    case when up.cabd_id is not null then true else false end as updates_pending,    
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
          GROUP BY a.fishway_id) sp2 ON sp2.fishway_id = d.cabd_id
     LEFT JOIN cabd.updates_pending up on up.cabd_id = d.cabd_id;
     
     
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
    case when up.cabd_id is not null then true else false end as updates_pending,    
    w.snapped_point AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code
     LEFT JOIN cabd.updates_pending up on up.cabd_id = w.cabd_id;
     

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
    case when up.cabd_id is not null then true else false end as updates_pending,    
    w.snapped_point AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code     
     LEFT JOIN cabd.updates_pending up on up.cabd_id = w.cabd_id;


insert into cabd.feature_type_metadata
   (view_name, field_name, name_en, description_en, is_link, data_type, vw_simple_order, vw_all_order, include_vector_tile, value_options_reference, name_fr, description_fr, is_name_search)
values
   ('cabd.all_features_view', 'updates_pending', 'Updates Pending', 'There are updates pending for this feature.', false, 'boolean', null, 18, false, null, 'Updates Pending', 'There are updates pending for this feature', false),
   ('cabd.barriers_view', 'updates_pending', 'Updates Pending', 'There are updates pending for this feature.', false, 'boolean', null, 18, false, null, 'Updates Pending', 'There are updates pending for this feature', false),
   ('cabd.fishways_view', 'updates_pending', 'Updates Pending', 'There are updates pending for this feature.', false, 'boolean', null, 52, false, null, 'Updates Pending', 'There are updates pending for this feature', false),
   ('cabd.dams_view', 'updates_pending', 'Updates Pending', 'There are updates pending for this feature.', false, 'boolean', null, 97, false, null, 'Updates Pending', 'There are updates pending for this feature', false),
   ('cabd.waterfalls_view', 'updates_pending', 'Updates Pending', 'There are updates pending for this feature.', false, 'boolean', null, 23, false, null, 'Updates Pending', 'There are updates pending for this feature', false);

GRANT ALL PRIVILEGES ON cabd.all_features_view_en to cabd;
GRANT ALL PRIVILEGES ON cabd.all_features_view_fr to cabd;
GRANT ALL PRIVILEGES ON cabd.barriers_view_en to cabd;
GRANT ALL PRIVILEGES ON cabd.barriers_view_fr to cabd;
GRANT ALL PRIVILEGES ON cabd.fishways_view_en to cabd;
GRANT ALL PRIVILEGES ON cabd.fishways_view_fr to cabd;
GRANT ALL PRIVILEGES ON cabd.dams_view_en to cabd;
GRANT ALL PRIVILEGES ON cabd.dams_view_fr to cabd;
GRANT ALL PRIVILEGES ON cabd.waterfalls_view_en to cabd;
GRANT ALL PRIVILEGES ON cabd.waterfalls_view_fr to cabd;





------------------------------------------------------------------
-- AUDIT LOG --
------------------------------------------------------------------
--https://github.com/glynastill/pg_jsonb_delete_op/blob/master/pg_jsonb_delete_op.sql
CREATE OR REPLACE FUNCTION jsonb_delete_left(a jsonb, b jsonb) 
RETURNS jsonb AS 
$BODY$
    SELECT COALESCE(    	
        (
            SELECT ('{' || string_agg(to_json(key) || ':' || value, ',') || '}')
            FROM jsonb_each(a)
            WHERE NOT ('{' || to_json(key) || ':' || value || '}')::jsonb <@ b
        )
    , '{}')::jsonb;
$BODY$
LANGUAGE sql IMMUTABLE STRICT;
COMMENT ON FUNCTION jsonb_delete_left(jsonb, jsonb) IS 'delete matching pairs in second argument from first argument';

CREATE OPERATOR - ( PROCEDURE = jsonb_delete_left, LEFTARG = jsonb, RIGHTARG = jsonb);
COMMENT ON OPERATOR - (jsonb, jsonb) IS 'delete matching pairs from left operand';


CREATE TABLE cabd.audit_log (
  revision serial not null primary key,
  datetime timestamp without time zone not null default now(),
  username varchar(32) not null default CURRENT_USER,
  schemaname varchar(64) not null,
  tablename varchar(64) not null,
  action varchar(6),
  cabd_id uuid,
  datasource_id uuid,
  id_pk uuid,
  oldvalues jsonb,
  newvalues jsonb
);

------------------------------------------------------------------
--triggers for rows with cabd_id as primary key
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cabd.audit_cabdid_insert()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, newvalues) VALUES('INSERT', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.cabd_id, to_jsonb(NEW)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_cabdid_delete()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, oldvalues) VALUES('DELETE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, OLD.cabd_id, to_jsonb(OLD)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_cabdid_update()
RETURNS TRIGGER AS $$
DECLARE
  js_new jsonb := row_to_json(NEW)::jsonb;
  js_old jsonb := row_to_json(OLD)::jsonb;
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, oldvalues, newvalues) VALUES('UPDATE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.cabd_id, js_old - js_new, js_new - js_old);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------
--triggers for rows with cabd_id & datasource as primary key
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cabd.audit_cabddatasourceid_insert()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, datasource_id, newvalues) VALUES('INSERT', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.cabd_id, NEW.datasource_id, to_jsonb(NEW)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_cabddatasourceid_delete()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, datasource_id, oldvalues) VALUES('DELETE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, OLD.cabd_id, OLD.datasource_id, to_jsonb(OLD)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_cabddatasourceid_update()
RETURNS TRIGGER AS $$
DECLARE
  js_new jsonb := row_to_json(NEW)::jsonb;
  js_old jsonb := row_to_json(OLD)::jsonb;
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, datasource_id, oldvalues, newvalues) VALUES('UPDATE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.cabd_id, NEW.datasource_id, js_old - js_new, js_new - js_old);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------
--  triggers for rows with data_source table audit log ---
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cabd.audit_datasourcetable_insert()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, datasource_id, newvalues) VALUES('INSERT', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.id, to_jsonb(NEW)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_datasourcetable_delete()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, datasource_id, oldvalues) VALUES('DELETE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, OLD.id, to_jsonb(OLD)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_datasourcetable_update()
RETURNS TRIGGER AS $$
DECLARE
  js_new jsonb := row_to_json(NEW)::jsonb;
  js_old jsonb := row_to_json(OLD)::jsonb;
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, datasource_id, oldvalues, newvalues) VALUES('UPDATE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text,  NEW.id, js_old - js_new, js_new - js_old);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;



------------------------------------------------------------------
--  triggers for rows with id primary key table audit log ---
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cabd.audit_id_insert()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, id_pk, newvalues) VALUES('INSERT', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.id, to_jsonb(NEW)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_id_delete()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, id_pk, oldvalues) VALUES('DELETE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, OLD.id, to_jsonb(OLD)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_id_update()
RETURNS TRIGGER AS $$
DECLARE
  js_new jsonb := row_to_json(NEW)::jsonb;
  js_old jsonb := row_to_json(OLD)::jsonb;
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, id_pk, oldvalues, newvalues) VALUES('UPDATE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text,  NEW.id, js_old - js_new, js_new - js_old);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


------------------------------------------------------------------
--triggers for fishways.species_mapping table
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cabd.audit_speciesmappingtable_insert()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, id_pk, newvalues) VALUES('INSERT', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.fishway_id, NEW.species_id, to_jsonb(NEW)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_speciesmappingtable_delete()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, id_pk, oldvalues) VALUES('DELETE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, OLD.fishway_id, OLD.species_id, to_jsonb(OLD)::jsonb);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION cabd.audit_speciesmappingtable_update()
RETURNS TRIGGER AS $$
DECLARE
  js_new jsonb := row_to_json(NEW)::jsonb;
  js_old jsonb := row_to_json(OLD)::jsonb;
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, id_pk, oldvalues, newvalues) VALUES('UPDATE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.fishway_id, NEW.species_id, js_old - js_new, js_new - js_old);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


------------------------------------------------------------------
--triggers 
------------------------------------------------------------------
CREATE TRIGGER dams_insert_trg AFTER INSERT ON dams.dams FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_insert();
CREATE TRIGGER dams_delete_trg AFTER DELETE ON dams.dams FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_delete();
CREATE TRIGGER dams_update_trg AFTER UPDATE ON dams.dams FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_update();

CREATE TRIGGER dams_attribute_source_insert_trg AFTER INSERT ON dams.dams_attribute_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_insert();
CREATE TRIGGER dams_attribute_source_delete_trg AFTER DELETE ON dams.dams_attribute_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_delete();
CREATE TRIGGER dams_attribute_source_update_trg AFTER UPDATE ON dams.dams_attribute_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_update();

CREATE TRIGGER dams_feature_source_insert_trg AFTER INSERT ON dams.dams_feature_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabddatasourceid_insert();
CREATE TRIGGER dams_feature_source_delete_trg AFTER DELETE ON dams.dams_feature_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabddatasourceid_delete();
CREATE TRIGGER dams_feature_source_update_trg AFTER UPDATE ON dams.dams_feature_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabddatasourceid_update();


CREATE TRIGGER waterfalls_insert_trg AFTER INSERT ON waterfalls.waterfalls FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_insert();
CREATE TRIGGER waterfalls_delete_trg AFTER DELETE ON waterfalls.waterfalls FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_delete();
CREATE TRIGGER waterfalls_update_trg AFTER UPDATE ON waterfalls.waterfalls FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_update();

CREATE TRIGGER waterfalls_attribute_source_insert_trg AFTER INSERT ON waterfalls.waterfalls_attribute_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_insert();
CREATE TRIGGER waterfalls_attribute_source_delete_trg AFTER DELETE ON waterfalls.waterfalls_attribute_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_delete();
CREATE TRIGGER waterfalls_attribute_source_update_trg AFTER UPDATE ON waterfalls.waterfalls_attribute_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_update();

CREATE TRIGGER waterfalls_feature_source_insert_trg AFTER INSERT ON waterfalls.waterfalls_feature_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabddatasourceid_insert();
CREATE TRIGGER waterfalls_feature_source_delete_trg AFTER DELETE ON waterfalls.waterfalls_feature_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabddatasourceid_delete();
CREATE TRIGGER waterfalls_feature_source_update_trg AFTER UPDATE ON waterfalls.waterfalls_feature_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabddatasourceid_update();


CREATE TRIGGER fishways_insert_trg AFTER INSERT ON fishways.fishways FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_insert();
CREATE TRIGGER fishways_delete_trg AFTER DELETE ON fishways.fishways FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_delete();
CREATE TRIGGER fishways_update_trg AFTER UPDATE ON fishways.fishways FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_update();

CREATE TRIGGER fishways_attribute_source_insert_trg AFTER INSERT ON fishways.fishways_attribute_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_insert();
CREATE TRIGGER fishways_attribute_source_delete_trg AFTER DELETE ON fishways.fishways_attribute_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_delete();
CREATE TRIGGER fishways_attribute_source_update_trg AFTER UPDATE ON fishways.fishways_attribute_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabdid_update();

CREATE TRIGGER fishways_feature_source_insert_trg AFTER INSERT ON fishways.fishways_feature_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabddatasourceid_insert();
CREATE TRIGGER fishways_feature_source_delete_trg AFTER DELETE ON fishways.fishways_feature_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabddatasourceid_delete();
CREATE TRIGGER fishways_feature_source_update_trg AFTER UPDATE ON fishways.fishways_feature_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_cabddatasourceid_update();

CREATE TRIGGER fishways_species_mapping_insert_trg AFTER INSERT ON fishways.species_mapping FOR EACH ROW EXECUTE PROCEDURE cabd.audit_speciesmappingtable_insert();
CREATE TRIGGER fishways_species_mapping_delete_trg AFTER DELETE ON fishways.species_mapping FOR EACH ROW EXECUTE PROCEDURE cabd.audit_speciesmappingtable_delete();
CREATE TRIGGER fishways_species_mapping_update_trg AFTER UPDATE ON fishways.species_mapping FOR EACH ROW EXECUTE PROCEDURE cabd.audit_speciesmappingtable_update();


CREATE TRIGGER data_source_insert_trg AFTER INSERT ON cabd.data_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_datasourcetable_insert();
CREATE TRIGGER data_source_delete_trg AFTER DELETE ON cabd.data_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_datasourcetable_delete();
CREATE TRIGGER data_source_update_trg AFTER UPDATE ON cabd.data_source FOR EACH ROW EXECUTE PROCEDURE cabd.audit_datasourcetable_update();


CREATE TRIGGER contacts_insert_trg AFTER INSERT ON cabd.contacts FOR EACH ROW EXECUTE PROCEDURE cabd.audit_id_insert();
CREATE TRIGGER contacts_delete_trg AFTER DELETE ON cabd.contacts FOR EACH ROW EXECUTE PROCEDURE cabd.audit_id_delete();
CREATE TRIGGER contacts_update_trg AFTER UPDATE ON cabd.contacts FOR EACH ROW EXECUTE PROCEDURE cabd.audit_id_update();

CREATE TRIGGER fish_species_insert_trg AFTER INSERT ON cabd.fish_species FOR EACH ROW EXECUTE PROCEDURE cabd.audit_id_insert();
CREATE TRIGGER fish_species_delete_trg AFTER DELETE ON cabd.fish_species FOR EACH ROW EXECUTE PROCEDURE cabd.audit_id_delete();
CREATE TRIGGER fish_species_update_trg AFTER UPDATE ON cabd.fish_species FOR EACH ROW EXECUTE PROCEDURE cabd.audit_id_update();


grant all privileges on cabd.user_feature_updates to cabd;
grant all privileges on cabd.audit_log to cabd;
grant all privileges on cabd.audit_log_revision_seq to cabd;



              