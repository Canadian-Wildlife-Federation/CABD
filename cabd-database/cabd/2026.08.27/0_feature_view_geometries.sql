set role cabd;

-- cabd.waterfalls_view_en source

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
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    w.include_in_act,
    case when w.snapped_point is null then w.original_point else w.snapped_point end AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = w.cabd_id;


-- cabd.waterfalls_view_fr source

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
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    w.include_in_act,
    case when w.snapped_point is null then w.original_point else w.snapped_point end AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = w.cabd_id;     



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
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.geometry AS geometry
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
            CASE
                WHEN dams.snapped_point IS NOT NULL THEN dams.snapped_point
                ELSE dams.original_point
            END AS geometry
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
            CASE
                WHEN waterfalls.snapped_point IS NOT NULL THEN waterfalls.snapped_point
                ELSE waterfalls.original_point
            END AS geometry
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
                CASE
                    WHEN c.snapped_point IS NOT NULL THEN c.snapped_point
                    ELSE c.original_point
                END AS geometry
           FROM stream_crossings.sites c
             LEFT JOIN stream_crossings.structures ts ON ts.site_id = c.cabd_id AND ts.primary_structure IS TRUE) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;

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
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.geometry AS geometry
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
            CASE
                WHEN dams.snapped_point IS NOT NULL THEN dams.snapped_point
                ELSE dams.original_point
            END AS geometry
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
            CASE
                WHEN waterfalls.snapped_point IS NOT NULL THEN waterfalls.snapped_point
                ELSE waterfalls.original_point
            END AS geometry
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
                CASE
                    WHEN c.snapped_point IS NOT NULL THEN c.snapped_point
                    ELSE c.original_point
                END AS geometry
           FROM stream_crossings.sites c
             LEFT JOIN stream_crossings.structures ts ON ts.site_id = c.cabd_id AND ts.primary_structure IS TRUE) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;     




     -- cabd.dams_view_en source

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
        CASE
            WHEN upd.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    d.addressed_status_code,
    ast.name_en AS addressed_status,
    d.assessment_type_code,
    aty.name_en AS assessment_type,
    d.barrier_assessment_status_code,
    bas.name_en AS barrier_assessment_status,
    d.barrier_removed_code,
    br.name_en AS barrier_removed,
    d.include_in_act,
    case when d.snapped_point is not null then d.snapped_point else d.original_point end AS geometry
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
     LEFT JOIN cabd.updates_pending upd ON upd.cabd_id = d.cabd_id
     LEFT JOIN cabd.barrier_removed_codes br ON br.code = d.barrier_removed_code
     LEFT JOIN cabd.barrier_assessment_status_codes bas ON bas.code = d.barrier_assessment_status_code
     LEFT JOIN cabd.assessment_type_codes aty ON aty.code = d.assessment_type_code
     LEFT JOIN cabd.addressed_status_codes ast ON ast.code = d.addressed_status_code;


-- cabd.dams_view_fr source

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
        CASE
            WHEN upd.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    d.addressed_status_code,
    ast.name_fr AS addressed_status,
    d.assessment_type_code,
    aty.name_fr AS assessment_type,
    d.barrier_assessment_status_code,
    bas.name_fr AS barrier_assessment_status,
    d.barrier_removed_code,
    br.name_fr AS barrier_removed,
    d.include_in_act,
    case when d.snapped_point is not null then d.snapped_point else d.original_point end AS geometry
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
     LEFT JOIN cabd.updates_pending upd ON upd.cabd_id = d.cabd_id
     LEFT JOIN cabd.barrier_removed_codes br ON br.code = d.barrier_removed_code
     LEFT JOIN cabd.barrier_assessment_status_codes bas ON bas.code = d.barrier_assessment_status_code
     LEFT JOIN cabd.assessment_type_codes aty ON aty.code = d.assessment_type_code
     LEFT JOIN cabd.addressed_status_codes ast ON ast.code = d.addressed_status_code;     




-- cabd.all_features_view_en source

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
    barriers.geometry AS geometry
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
            CASE
                WHEN dams.snapped_point IS NOT NULL THEN dams.snapped_point
                ELSE dams.original_point
            END AS geometry
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
            CASE
                WHEN waterfalls.snapped_point IS NOT NULL THEN waterfalls.snapped_point
                ELSE waterfalls.original_point
            END AS geometry
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
            fishways.original_point as geometry
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
                CASE
                    WHEN c.snapped_point IS NOT NULL THEN c.snapped_point
                    ELSE c.original_point
                END AS geometry
           FROM stream_crossings.sites c
             LEFT JOIN stream_crossings.structures ts ON ts.site_id = c.cabd_id AND ts.primary_structure IS TRUE) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;


-- cabd.all_features_view_fr source

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
    barriers.geometry AS geometry
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
            CASE
                WHEN dams.snapped_point IS NOT NULL THEN dams.snapped_point
                ELSE dams.original_point
            END AS geometry
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
            CASE
                WHEN waterfalls.snapped_point IS NOT NULL THEN waterfalls.snapped_point
                ELSE waterfalls.original_point
            END AS geometry
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
            fishways.original_point as geometry
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
                CASE
                    WHEN c.snapped_point IS NOT NULL THEN c.snapped_point
                    ELSE c.original_point
                END AS geometry
           FROM stream_crossings.sites c
             LEFT JOIN stream_crossings.structures ts ON ts.site_id = c.cabd_id AND ts.primary_structure IS TRUE) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;          