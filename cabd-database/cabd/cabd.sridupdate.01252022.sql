drop view cabd.dams_view;
drop view cabd.fishways_view;
drop view cabd.waterfalls_view;
drop view cabd.barriers_view;
drop view cabd.all_features_view;

ALTER TABLE dams.dams
ALTER COLUMN original_point TYPE geometry(POINT, 4617) 
USING ST_Transform(original_point, 4617) ;

ALTER TABLE dams.dams
ALTER COLUMN snapped_point TYPE geometry(POINT, 4617) 
USING ST_Transform(snapped_point, 4617) ;

ALTER TABLE waterfalls.waterfalls
ALTER COLUMN original_point TYPE geometry(POINT, 4617) 
USING ST_Transform(original_point, 4617) ;

ALTER TABLE waterfalls.waterfalls
ALTER COLUMN snapped_point TYPE geometry(POINT, 4617) 
USING ST_Transform(snapped_point, 4617) ;

ALTER TABLE fishways.fishways
ALTER COLUMN original_point TYPE geometry(POINT, 4617) 
USING ST_Transform(original_point, 4617) ;


ALTER TABLE cabd.province_territory_codes
ALTER COLUMN geometry TYPE geometry(MULTIPOLYGON, 4617) 
USING ST_Transform(geometry, 4617) ;

ALTER TABLE cabd.nhn_workunit
ALTER COLUMN polygon TYPE geometry(POLYGON, 4617) 
USING ST_Transform(polygon, 4617) ;



drop foreign table chyf_flowpath;

CREATE FOREIGN TABLE chyf_flowpath (
 region_id character varying(32),     
 type character varying,    
 rank character varying,        
 length double precision ,        
 name character varying,         
 nameid character varying ,        
 geometry geometry(LineString,4617) 
)
SERVER chyf_server
OPTIONS (schema_name 'chyf2', table_name 'eflowpath');  

grant select on chyf_flowpath to public;

CREATE OR REPLACE FUNCTION cabd.snap_to_network(src_schema varchar, src_table varchar, raw_geom varchar, snapped_geom varchar, max_distance_m double precision) RETURNS VOID AS $$
DECLARE
  pnt_rec RECORD;
  fp_rec RECORD;
BEGIN

	FOR pnt_rec IN EXECUTE format('SELECT cabd_id, %I as rawg FROM %I.%I WHERE %I is not null AND (use_analysis = true or use_analysis is null)', raw_geom, src_schema, src_table,raw_geom) 
	LOOP 
		--RAISE NOTICE '%s: %s', pnt_rec.cabd_id, pnt_rec.rawg;
		FOR fp_rec IN EXECUTE format ('SELECT fp.geometry as geometry, st_distance(%L::geometry::geography, fp.geometry::geography) AS distance FROM chyf_flowpath fp WHERE st_expand(%L::geometry, 0.01) && fp.geometry and st_distance(%L::geometry::geography, fp.geometry::geography) < %s ORDER BY distance ', pnt_rec.rawg, pnt_rec.rawg, pnt_rec.rawg, max_distance_m)
		LOOP
			EXECUTE format('UPDATE %I.%I SET %I = ST_LineInterpolatePoint(%L::geometry, ST_LineLocatePoint(%L::geometry, %L::geometry) ) WHERE cabd_id = %L', src_schema, src_table, snapped_geom,fp_rec.geometry, fp_rec.geometry, pnt_rec.rawg, pnt_rec.cabd_id);
			--RAISE NOTICE '%s', fp_rec.distance;	
			EXIT;
		
		END LOOP;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

-- fix bug with barriers view
update cabd.feature_type_metadata set field_name = 'feature_type' where view_name  = 'cabd.barriers_view' and field_name = 'barrier_type'

--resnap
select cabd.snap_to_network('dams', 'dams', 'original_point', 'snapped_point', 150);
select cabd.snap_to_network('waterfalls', 'waterfalls', 'original_point', 'snapped_point', 150);

--recreate view
CREATE OR REPLACE VIEW cabd.dams_view
AS SELECT d.cabd_id,
    'dams'::text AS feature_type,
    st_y(d.snapped_point) AS latitude,
    st_x(d.snapped_point) AS longitude,
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
    d.provincial_compliance_status,
    d.federal_compliance_status,
    d.operating_notes,
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
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code;
     
CREATE OR REPLACE VIEW cabd.barriers_view
AS SELECT barriers.cabd_id,
    barriers.feature_type,
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
            'dams'::text AS feature_type,
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
            dams.snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS feature_type,
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
            dams.snapped_point
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
            NULL::smallint AS int2,
            fishways.original_point
           FROM fishways.fishways) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_workunit_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;
     

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
    d.nhn_workunit_id,
    nhn.sub_sub_drainage_area,
    d.province_territory_code,
    pt.name AS province_territory,
    d.municipality,
    d.fishpass_type_code,
    tc.name AS fishpass_type,
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
    elc.name AS entrance_location,
    d.entrance_position_code,
    epc.name AS entrance_position,
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
    cl.name AS complete_level,
    sp1.species AS known_use,
    sp2.species AS known_notuse,
    '/featuresources/'::text || d.cabd_id AS data_source,
    d.original_point AS geometry
   FROM fishways.fishways d
     LEFT JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_workunit_id::text
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
          
          
CREATE OR REPLACE VIEW cabd.waterfalls_view
AS SELECT w.cabd_id,
    'waterfalls'::text AS feature_type,
    st_y(w.snapped_point) AS latitude,
    st_x(w.snapped_point) AS longitude,
    w.fall_name_en,
    w.fall_name_fr,
    w.waterbody_name_en,
    w.waterbody_name_fr,
    w.nhn_workunit_id,
    nhn.sub_sub_drainage_area,
    w.province_territory_code,
    pt.name AS province_territory,
    w.municipality,
    w.fall_height_m,
    w.last_modified,
    w.comments,
    w.complete_level_code,
    cl.name AS complete_level,
    w.passability_status_code,
    ps.name AS passability_status,
    w.snapped_point AS geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = w.nhn_workunit_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code;

     
GRANT ALL PRIVILEGES ON cabd.dams_view to cabd;
GRANT ALL PRIVILEGES ON cabd.barriers_view to cabd;
GRANT ALL PRIVILEGES ON cabd.fishways_view to cabd;
GRANT ALL PRIVILEGES ON cabd.waterfalls_view to cabd;
GRANT ALL PRIVILEGES ON cabd.all_features_view to cabd;
          