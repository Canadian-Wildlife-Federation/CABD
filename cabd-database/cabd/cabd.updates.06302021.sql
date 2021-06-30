-- remove the snapped_geometry column from the fishways tables
--decision made after meeting with Alex&Katherine 06302021


--update all features view and associated metadata
CREATE OR REPLACE VIEW cabd.all_features_view
AS SELECT barriers.cabd_id,
    barriers.barrier_type AS feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name AS province_territory,
    barriers.watershed_group_code,
    wg.name AS watershed_group_name,
    barriers.nhn_workunit_id,
    barriers.nearest_municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name as passability_status,
    barriers.snapped_point as geometry
   FROM ( SELECT dams_medium_large.cabd_id,
            'dams_medium_large'::text AS barrier_type,
            dams_medium_large.dam_name_en AS name_en,
            dams_medium_large.dam_name_fr AS name_fr,
            dams_medium_large.province_territory_code,
            dams_medium_large.watershed_group_code,
            dams_medium_large.nhn_workunit_id,
            dams_medium_large.nearest_municipality,
            dams_medium_large.waterbody_name_en,
            dams_medium_large.waterbody_name_fr,
            dams_medium_large.reservoir_name_en,
            dams_medium_large.reservoir_name_fr,
            dams_medium_large.passability_status_code,
            dams_medium_large.snapped_point
           FROM dams.dams_medium_large
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS barrier_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.watershed_group_code,
            waterfalls.nhn_workunit_id,
            waterfalls.nearest_municipality,
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
            NULL::varchar(512) AS name_en,
            NULL::varchar(512) AS name_fr,
            fishways.province_territory_code,
            fishways.watershed_group_code,
            fishways.nhn_workunit_id,
            fishways.nearest_municipality,
            fishways.river_name_en,
            fishways.river_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL,
            fishways.original_point
           FROM fishways.fishways
          ) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = barriers.watershed_group_code::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;
     
grant all privileges on cabd.all_features_view to cabd;



CREATE OR REPLACE VIEW cabd.fishways_view
AS SELECT d.cabd_id,
    'fishways'::text AS feature_type,
    st_y(d.original_point) AS latitude,
    st_x(d.original_point) AS longitude,
    d.dam_id,
    d.dam_name_en,
    d.dam_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.river_name_en,
    d.river_name_fr,
    d.watershed_group_code,
    wg.name AS watershed_group_name,
    d.nhn_workunit_id,
    d.province_territory_code,
    pt.name AS province_territory,
    d.nearest_municipality,	
    d.fishpass_type_code,
    tc.name as fishpass_type,
    d.monitoring_equipment,
    d.architect,
    d.contracted_by,
    d.constructed_by,
    d.plans_held_by,
    d.purpose,
    d.designed_on_biology,
    d.length_m,
    d.elevation_m,
    d.inclination_pct,
    d.depth_m,
    d.entrance_location_code,
    elc.name as entrance_location,
    d.entrance_position_code,	
    epc as entrance_position, 
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
    d.data_source_id,
    d.data_source,
    d.complete_level_code,
    cl.name as complete_level,
    sp1.species AS known_use,
    sp2.species AS known_notuse,
    d.original_point as geometry
FROM fishways.fishways d
     LEFT JOIN cabd.province_territory_codes pt ON pt.code = d.province_territory_code
     LEFT JOIN cabd.watershed_groups wg ON wg.code = d.watershed_group_code
     LEFT JOIN cabd.upstream_passage_type_codes tc ON tc.code = d.fishpass_type_code
     LEFT JOIN fishways.entrance_location_codes elc on elc.code = d.entrance_location_code
     LEFT JOIN fishways.entrance_position_codes epc on epc.code = d.entrance_position_code
     LEFT JOIN fishways.fishway_complete_level_codes cl on cl.code = d.complete_level_code  
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


grant all privileges on cabd.fishways_view to cabd;

alter table fishways.fishways drop column snapped_point;
