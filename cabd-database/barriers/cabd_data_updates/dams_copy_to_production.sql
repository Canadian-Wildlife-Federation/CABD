-- https://www.postgresql.org/docs/13/sql-begin.html
-- if you are using a GUI you need to configure the script editor to 
-- not run in autocommit mode 

BEGIN TRANSACTION;

INSERT INTO dams.dams(
    cabd_id, dam_name_en, dam_name_fr, waterbody_name_en,
    waterbody_name_fr, reservoir_name_en, reservoir_name_fr,
    province_territory_code, municipality, "owner",
    ownership_type_code, provincial_compliance_status,
    federal_compliance_status, operating_notes, operating_status_code,
    use_code, use_irrigation_code, use_electricity_code,
    use_supply_code, use_floodcontrol_code, use_recreation_code,
    use_navigation_code, use_fish_code, use_pollution_code,
    use_invasivespecies_code, use_other_code, lake_control_code,
    construction_year, removed_year, assess_schedule, expected_life,
    maintenance_last, maintenance_next, function_code, condition_code,
    construction_type_code, height_m, length_m, size_class_code,
    spillway_capacity, spillway_type_code, reservoir_present,
    reservoir_area_skm, reservoir_depth_m, storage_capacity_mcm,
    avg_rate_of_discharge_ls, degree_of_regulation_pc, provincial_flow_req,
    federal_flow_req, catchment_area_skm, hydro_peaking_system,
    generating_capacity_mwh, turbine_number, turbine_type_code,
    up_passage_type_code, down_passage_route_code, last_modified,
    "comments", complete_level_code, original_point, snapped_point,
    nhn_watershed_id, upstream_linear_km, passability_status_code,
    passability_status_note, use_analysis, facility_name_en, facility_name_fr
)
SELECT
    cabd_id, dam_name_en, dam_name_fr, waterbody_name_en,
    waterbody_name_fr, reservoir_name_en, reservoir_name_fr,
    province_territory_code, municipality, "owner",
    ownership_type_code, provincial_compliance_status,
    federal_compliance_status, operating_notes, operating_status_code,
    use_code, use_irrigation_code, use_electricity_code,
    use_supply_code, use_floodcontrol_code, use_recreation_code,
    use_navigation_code, use_fish_code, use_pollution_code,
    use_invasivespecies_code, use_other_code, lake_control_code,
    construction_year, removed_year, assess_schedule, expected_life,
    maintenance_last, maintenance_next, function_code, condition_code,
    construction_type_code, height_m, length_m, size_class_code,
    spillway_capacity, spillway_type_code, reservoir_present,
    reservoir_area_skm, reservoir_depth_m, storage_capacity_mcm,
    avg_rate_of_discharge_ls, degree_of_regulation_pc, provincial_flow_req,
    federal_flow_req, catchment_area_skm, hydro_peaking_system,
    generating_capacity_mwh, turbine_number, turbine_type_code,
    up_passage_type_code, down_passage_route_code, last_modified,
    "comments", complete_level_code, original_point, snapped_point,
    nhn_watershed_id, upstream_linear_km, passability_status_code,
    passability_status_note, use_analysis, facility_name_en, facility_name_fr
FROM featurecopy.dams;

INSERT INTO dams.dams_attribute_source(
    cabd_id, dam_name_en_ds, dam_name_fr_ds, waterbody_name_en_ds,
    waterbody_name_fr_ds, reservoir_name_en_ds, reservoir_name_fr_ds,
    owner_ds, ownership_type_code_ds, provincial_compliance_status_ds,
    federal_compliance_status_ds, operating_notes_ds, operating_status_code_ds,
    use_code_ds, use_irrigation_code_ds, use_electricity_code_ds,
    use_supply_code_ds, use_floodcontrol_code_ds,use_recreation_code_ds,
    use_navigation_code_ds, use_fish_code_ds, use_pollution_code_ds,
    use_invasivespecies_code_ds, use_other_code_ds, lake_control_code_ds,
    construction_year_ds, assess_schedule_ds, expected_life_ds,
    maintenance_last_ds, maintenance_next_ds, function_code_ds,
    condition_code_ds, construction_type_code_ds, height_m_ds, length_m_ds,
    size_class_code_ds, spillway_capacity_ds, spillway_type_code_ds,
    reservoir_present_ds, reservoir_area_skm_ds, reservoir_depth_m_ds,
    storage_capacity_mcm_ds, avg_rate_of_discharge_ls_ds,
    degree_of_regulation_pc_ds, provincial_flow_req_ds, federal_flow_req_ds,
    catchment_area_skm_ds, upstream_linear_km_ds, hydro_peaking_system_ds,
    generating_capacity_mwh_ds, turbine_number_ds, turbine_type_code_ds,
    up_passage_type_code_ds, down_passage_route_code_ds, passability_status_code_ds,
    passability_status_note_ds, comments_ds, complete_level_code_ds,
    original_point_ds, facility_name_en_ds, facility_name_fr_ds, removed_year_ds
)
SELECT
    cabd_id, dam_name_en_ds, dam_name_fr_ds, waterbody_name_en_ds,
    waterbody_name_fr_ds, reservoir_name_en_ds, reservoir_name_fr_ds,
    owner_ds, ownership_type_code_ds, provincial_compliance_status_ds,
    federal_compliance_status_ds, operating_notes_ds, operating_status_code_ds,
    use_code_ds, use_irrigation_code_ds, use_electricity_code_ds,
    use_supply_code_ds, use_floodcontrol_code_ds,use_recreation_code_ds,
    use_navigation_code_ds, use_fish_code_ds, use_pollution_code_ds,
    use_invasivespecies_code_ds, use_other_code_ds, lake_control_code_ds,
    construction_year_ds, assess_schedule_ds, expected_life_ds,
    maintenance_last_ds, maintenance_next_ds, function_code_ds,
    condition_code_ds, construction_type_code_ds, height_m_ds, length_m_ds,
    size_class_code_ds, spillway_capacity_ds, spillway_type_code_ds,
    reservoir_present_ds, reservoir_area_skm_ds, reservoir_depth_m_ds,
    storage_capacity_mcm_ds, avg_rate_of_discharge_ls_ds,
    degree_of_regulation_pc_ds, provincial_flow_req_ds, federal_flow_req_ds,
    catchment_area_skm_ds, upstream_linear_km_ds, hydro_peaking_system_ds,
    generating_capacity_mwh_ds, turbine_number_ds, turbine_type_code_ds,
    up_passage_type_code_ds, down_passage_route_code_ds, passability_status_code_ds,
    passability_status_note_ds, comments_ds, complete_level_code_ds,
    original_point_ds, facility_name_en_ds, facility_name_fr_ds, removed_year_ds
FROM featurecopy.dams_attribute_source;

INSERT INTO dams.dams_feature_source (cabd_id, datasource_id, datasource_feature_id)
    SELECT cabd_id, datasource_id, datasource_feature_id
FROM featurecopy.dams_feature_source
ON CONFLICT DO NOTHING;

--do whatever qa checks you want to do here?

SELECT COUNT(*) FROM dams.dams;
SELECT COUNT(*) FROM dams.dams_attribute_source;
SELECT COUNT(*) FROM dams.dams_feature_source;

--COMMIT;