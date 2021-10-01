update cabd.feature_types  set type = 'dams' where type = 'dams_medium_large';

CREATE OR REPLACE VIEW cabd.dams_view
AS SELECT d.cabd_id,
    'dams'::text AS feature_type,
    st_y(d.spoint) AS latitude,
    st_x(d.spoint) AS longitude,
    d.dam_name_en,
    d.dam_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.reservoir_name_en,
    d.reservoir_name_fr,
    d.nhn_workunit_id,
    nhn.sub_sub_drainage_area,
    d.province_territory_code,
    pt.name AS province_territory,
    d.owner,
    d.ownership_type_code,
    ow.name AS ownership_type,
    d.municipality,
    d.province_compliance_status,
    d.federal_compliance_status,
    d.operating_note,
    d.operating_status_code,
    os.name AS operating_status,
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
     LEFT JOIN dams.turbine_type_codes dt ON dsp.code = d.turbine_type_code
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN dams.downstream_passage_route_codes down ON down.code = d.down_passage_route_code
     LEFT JOIN dams.dam_complete_level_codes cl ON cl.code = d.complete_level_code
     LEFT JOIN dams.lake_control_codes lk ON lk.code = d.lake_control_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_workunit_id::text
     LEFT JOIN dams.passability_status_codes ps ON ps.code = d.passability_status_code;
     
     
CREATE OR REPLACE VIEW cabd.barriers_view
AS SELECT barriers.cabd_id,
    barriers.barrier_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name AS province_territory,
    barriers.nhn_workunit_id,
    nhn.sub_sub_drainage_area,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name AS passability_status,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS barrier_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_workunit_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.spoint AS snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS barrier_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_workunit_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_workunit_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;
     
     
     
CREATE OR REPLACE VIEW cabd.all_features_view
AS SELECT barriers.cabd_id,
    barriers.barrier_type AS feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name AS province_territory,
    barriers.nhn_workunit_id,
    nhn.sub_sub_drainage_area,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name AS passability_status,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS barrier_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_workunit_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.spoint AS snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS barrier_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_workunit_id,
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
            fishways.nhn_workunit_id,
            fishways.municipality,
            fishways.river_name_en,
            fishways.river_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::smallint,
            fishways.original_point
           FROM fishways.fishways) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_workunit_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;     