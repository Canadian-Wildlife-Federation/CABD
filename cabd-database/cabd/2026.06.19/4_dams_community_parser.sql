-- add missing constraint
alter table dams.dams add CONSTRAINT dams_medium_large_fk_27 FOREIGN KEY (assessment_type_code) REFERENCES cabd.assessment_type_codes(code);
alter table dams.dams add CONSTRAINT dams_medium_large_fk_28 FOREIGN KEY (addressed_status_code) REFERENCES cabd.addressed_status_codes(code);

--share status type and renam to be specific to community holding 
alter type stream_crossings."status_type" set schema cabd;
alter type cabd."status_type" rename to "community_holding_status_type";

alter table dams.dams_community_staging drop CONSTRAINT status_value_ch;
alter table dams.dams_community_staging add CONSTRAINT status_value_ch CHECK (((status)::text = ANY (ARRAY[('NEW'::character varying)::text, ('PROCESSED'::character varying)::text])));

-- DROP TABLE dams.dams_community_holding;
CREATE TABLE dams.dams_community_holding (
	id uuid NOT NULL,
	cabd_id uuid NOT NULL,
	user_id uuid NOT NULL,
	uploaded_datetime timestamptz NOT NULL,
	latitude numeric NULL,
	longitude numeric NULL,
	dam_name_en varchar,
    addressed_status_code int2 null,
    assessment_type_code int2 null,
    size_class_code int4 null,
    up_passage_type_code int4 null,
    passability_status_code int4 null,    
	
    status cabd."community_holding_status_type" NULL,
	reviewer varchar NULL,

	CONSTRAINT dams_community_holding_pkey PRIMARY KEY (id),
    CONSTRAINT dams_community_holding_addr_type_code_fkey FOREIGN KEY (addressed_status_code) REFERENCES cabd.addressed_status_codes(code),
	CONSTRAINT dams_community_holding_asses_type_code_fkey FOREIGN KEY (assessment_type_code) REFERENCES cabd.assessment_type_codes(code),
	CONSTRAINT dams_community_holding_size_class_cd_fkey FOREIGN KEY (size_class_code) REFERENCES dams.size_codes(code),
	CONSTRAINT dams_community_holding_up_pass_type_cd_fkey FOREIGN KEY (up_passage_type_code) REFERENCES cabd.upstream_passage_type_codes(code),
	CONSTRAINT dams_community_holdding_pass_status_cd_fkey FOREIGN KEY (passability_status_code) REFERENCES cabd.passability_status_codes(code)
);
alter table dams.dams_community_holding owner to cabd;


ALTER TABLE dams.dams_attribute_source RENAME COLUMN dam_name_en_ds TO dam_name_en_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN dam_name_fr_ds TO dam_name_fr_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN waterbody_name_en_ds TO waterbody_name_en_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN waterbody_name_fr_ds TO waterbody_name_fr_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN reservoir_name_en_ds TO reservoir_name_en_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN reservoir_name_fr_ds TO reservoir_name_fr_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN owner_ds TO owner_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN ownership_type_code_ds TO ownership_type_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN provincial_compliance_status_ds TO provincial_compliance_status_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN federal_compliance_status_ds TO federal_compliance_status_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN operating_notes_ds TO operating_notes_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN operating_status_code_ds TO operating_status_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_code_ds TO use_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_irrigation_code_ds TO use_irrigation_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_electricity_code_ds TO use_electricity_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_supply_code_ds TO use_supply_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_floodcontrol_code_ds TO use_floodcontrol_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_recreation_code_ds TO use_recreation_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_navigation_code_ds TO use_navigation_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_fish_code_ds TO use_fish_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_pollution_code_ds TO use_pollution_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_invasivespecies_code_ds TO use_invasivespecies_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_other_code_ds TO use_other_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN lake_control_code_ds TO lake_control_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN construction_year_ds TO construction_year_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN assess_schedule_ds TO assess_schedule_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN expected_end_of_life_ds TO expected_end_of_life_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN maintenance_last_ds TO maintenance_last_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN maintenance_next_ds TO maintenance_next_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN function_code_ds TO function_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN condition_code_ds TO condition_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN structure_type_code_ds TO structure_type_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN height_m_ds TO height_m_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN length_m_ds TO length_m_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN size_class_code_ds TO size_class_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN spillway_capacity_ds TO spillway_capacity_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN spillway_type_code_ds TO spillway_type_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN reservoir_present_ds TO reservoir_present_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN reservoir_area_skm_ds TO reservoir_area_skm_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN reservoir_depth_m_ds TO reservoir_depth_m_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN storage_capacity_mcm_ds TO storage_capacity_mcm_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN avg_rate_of_discharge_ls_ds TO avg_rate_of_discharge_ls_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN degree_of_regulation_pc_ds TO degree_of_regulation_pc_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN provincial_flow_req_ds TO provincial_flow_req_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN federal_flow_req_ds TO federal_flow_req_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN catchment_area_skm_ds TO catchment_area_skm_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN upstream_linear_km_ds TO upstream_linear_km_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN hydro_peaking_system_ds TO hydro_peaking_system_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN generating_capacity_mwh_ds TO generating_capacity_mwh_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN turbine_number_ds TO turbine_number_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN turbine_type_code_ds TO turbine_type_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN up_passage_type_code_ds TO up_passage_type_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN down_passage_route_code_ds TO down_passage_route_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN passability_status_code_ds TO passability_status_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN passability_status_note_ds TO passability_status_note_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN comments_ds TO comments_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN complete_level_code_ds TO complete_level_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN original_point_ds TO original_point_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN facility_name_en_ds TO facility_name_en_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN facility_name_fr_ds TO facility_name_fr_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN removed_year_ds TO removed_year_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN construction_material_code_ds TO construction_material_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN use_conservation_code_ds TO use_conservation_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN assessment_type_code_ds TO assessment_type_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN addressed_status_code_ds TO addressed_status_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN barrier_assessment_status_code_ds TO barrier_assessment_status_code_dsid;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN barrier_removed_code_ds TO barrier_removed_code_dsid;

ALTER TABLE dams.dams_attribute_source ADD COLUMN dam_name_en_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN dam_name_fr_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN waterbody_name_en_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN waterbody_name_fr_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN reservoir_name_en_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN reservoir_name_fr_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN owner_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN ownership_type_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN provincial_compliance_status_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN federal_compliance_status_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN operating_notes_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN operating_status_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_irrigation_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_electricity_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_supply_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_floodcontrol_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_recreation_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_navigation_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_fish_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_pollution_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_invasivespecies_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_other_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN lake_control_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN construction_year_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN assess_schedule_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN expected_end_of_life_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN maintenance_last_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN maintenance_next_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN function_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN condition_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN structure_type_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN height_m_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN length_m_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN size_class_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN spillway_capacity_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN spillway_type_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN reservoir_present_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN reservoir_area_skm_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN reservoir_depth_m_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN storage_capacity_mcm_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN avg_rate_of_discharge_ls_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN degree_of_regulation_pc_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN provincial_flow_req_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN federal_flow_req_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN catchment_area_skm_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN upstream_linear_km_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN hydro_peaking_system_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN generating_capacity_mwh_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN turbine_number_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN turbine_type_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN up_passage_type_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN down_passage_route_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN passability_status_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN passability_status_note_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN comments_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN complete_level_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN original_point_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN facility_name_en_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN facility_name_fr_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN removed_year_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN construction_material_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_conservation_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN assessment_type_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN addressed_status_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN barrier_assessment_status_code_src varchar(2);
ALTER TABLE dams.dams_attribute_source ADD COLUMN barrier_removed_code_src varchar(2);

UPDATE dams.dams_attribute_source
SET
    dam_name_en_src = CASE WHEN dam_name_en_dsid IS NOT NULL THEN 'ds' ELSE dam_name_en_src END,
    dam_name_fr_src = CASE WHEN dam_name_fr_dsid IS NOT NULL THEN 'ds' ELSE dam_name_fr_src END,
    waterbody_name_en_src = CASE WHEN waterbody_name_en_dsid IS NOT NULL THEN 'ds' ELSE waterbody_name_en_src END,
    waterbody_name_fr_src = CASE WHEN waterbody_name_fr_dsid IS NOT NULL THEN 'ds' ELSE waterbody_name_fr_src END,
    reservoir_name_en_src = CASE WHEN reservoir_name_en_dsid IS NOT NULL THEN 'ds' ELSE reservoir_name_en_src END,
    reservoir_name_fr_src = CASE WHEN reservoir_name_fr_dsid IS NOT NULL THEN 'ds' ELSE reservoir_name_fr_src END,
    owner_src = CASE WHEN owner_dsid IS NOT NULL THEN 'ds' ELSE owner_src END,
    ownership_type_code_src = CASE WHEN ownership_type_code_dsid IS NOT NULL THEN 'ds' ELSE ownership_type_code_src END,
    provincial_compliance_status_src = CASE WHEN provincial_compliance_status_dsid IS NOT NULL THEN 'ds' ELSE provincial_compliance_status_src END,
    federal_compliance_status_src = CASE WHEN federal_compliance_status_dsid IS NOT NULL THEN 'ds' ELSE federal_compliance_status_src END,
    operating_notes_src = CASE WHEN operating_notes_dsid IS NOT NULL THEN 'ds' ELSE operating_notes_src END,
    operating_status_code_src = CASE WHEN operating_status_code_dsid IS NOT NULL THEN 'ds' ELSE operating_status_code_src END,
    use_code_src = CASE WHEN use_code_dsid IS NOT NULL THEN 'ds' ELSE use_code_src END,
    use_irrigation_code_src = CASE WHEN use_irrigation_code_dsid IS NOT NULL THEN 'ds' ELSE use_irrigation_code_src END,
    use_electricity_code_src = CASE WHEN use_electricity_code_dsid IS NOT NULL THEN 'ds' ELSE use_electricity_code_src END,
    use_supply_code_src = CASE WHEN use_supply_code_dsid IS NOT NULL THEN 'ds' ELSE use_supply_code_src END,
    use_floodcontrol_code_src = CASE WHEN use_floodcontrol_code_dsid IS NOT NULL THEN 'ds' ELSE use_floodcontrol_code_src END,
    use_recreation_code_src = CASE WHEN use_recreation_code_dsid IS NOT NULL THEN 'ds' ELSE use_recreation_code_src END,
    use_navigation_code_src = CASE WHEN use_navigation_code_dsid IS NOT NULL THEN 'ds' ELSE use_navigation_code_src END,
    use_fish_code_src = CASE WHEN use_fish_code_dsid IS NOT NULL THEN 'ds' ELSE use_fish_code_src END,
    use_pollution_code_src = CASE WHEN use_pollution_code_dsid IS NOT NULL THEN 'ds' ELSE use_pollution_code_src END,
    use_invasivespecies_code_src = CASE WHEN use_invasivespecies_code_dsid IS NOT NULL THEN 'ds' ELSE use_invasivespecies_code_src END,
    use_other_code_src = CASE WHEN use_other_code_dsid IS NOT NULL THEN 'ds' ELSE use_other_code_src END,
    lake_control_code_src = CASE WHEN lake_control_code_dsid IS NOT NULL THEN 'ds' ELSE lake_control_code_src END,
    construction_year_src = CASE WHEN construction_year_dsid IS NOT NULL THEN 'ds' ELSE construction_year_src END,
    assess_schedule_src = CASE WHEN assess_schedule_dsid IS NOT NULL THEN 'ds' ELSE assess_schedule_src END,
    expected_end_of_life_src = CASE WHEN expected_end_of_life_dsid IS NOT NULL THEN 'ds' ELSE expected_end_of_life_src END,
    maintenance_last_src = CASE WHEN maintenance_last_dsid IS NOT NULL THEN 'ds' ELSE maintenance_last_src END,
    maintenance_next_src = CASE WHEN maintenance_next_dsid IS NOT NULL THEN 'ds' ELSE maintenance_next_src END,
    function_code_src = CASE WHEN function_code_dsid IS NOT NULL THEN 'ds' ELSE function_code_src END,
    condition_code_src = CASE WHEN condition_code_dsid IS NOT NULL THEN 'ds' ELSE condition_code_src END,
    structure_type_code_src = CASE WHEN structure_type_code_dsid IS NOT NULL THEN 'ds' ELSE structure_type_code_src END,
    height_m_src = CASE WHEN height_m_dsid IS NOT NULL THEN 'ds' ELSE height_m_src END,
    length_m_src = CASE WHEN length_m_dsid IS NOT NULL THEN 'ds' ELSE length_m_src END,
    size_class_code_src = CASE WHEN size_class_code_dsid IS NOT NULL THEN 'ds' ELSE size_class_code_src END,
    spillway_capacity_src = CASE WHEN spillway_capacity_dsid IS NOT NULL THEN 'ds' ELSE spillway_capacity_src END,
    spillway_type_code_src = CASE WHEN spillway_type_code_dsid IS NOT NULL THEN 'ds' ELSE spillway_type_code_src END,
    reservoir_present_src = CASE WHEN reservoir_present_dsid IS NOT NULL THEN 'ds' ELSE reservoir_present_src END,
    reservoir_area_skm_src = CASE WHEN reservoir_area_skm_dsid IS NOT NULL THEN 'ds' ELSE reservoir_area_skm_src END,
    reservoir_depth_m_src = CASE WHEN reservoir_depth_m_dsid IS NOT NULL THEN 'ds' ELSE reservoir_depth_m_src END,
    storage_capacity_mcm_src = CASE WHEN storage_capacity_mcm_dsid IS NOT NULL THEN 'ds' ELSE storage_capacity_mcm_src END,
    avg_rate_of_discharge_ls_src = CASE WHEN avg_rate_of_discharge_ls_dsid IS NOT NULL THEN 'ds' ELSE avg_rate_of_discharge_ls_src END,
    degree_of_regulation_pc_src = CASE WHEN degree_of_regulation_pc_dsid IS NOT NULL THEN 'ds' ELSE degree_of_regulation_pc_src END,
    provincial_flow_req_src = CASE WHEN provincial_flow_req_dsid IS NOT NULL THEN 'ds' ELSE provincial_flow_req_src END,
    federal_flow_req_src = CASE WHEN federal_flow_req_dsid IS NOT NULL THEN 'ds' ELSE federal_flow_req_src END,
    catchment_area_skm_src = CASE WHEN catchment_area_skm_dsid IS NOT NULL THEN 'ds' ELSE catchment_area_skm_src END,
    upstream_linear_km_src = CASE WHEN upstream_linear_km_dsid IS NOT NULL THEN 'ds' ELSE upstream_linear_km_src END,
    hydro_peaking_system_src = CASE WHEN hydro_peaking_system_dsid IS NOT NULL THEN 'ds' ELSE hydro_peaking_system_src END,
    generating_capacity_mwh_src = CASE WHEN generating_capacity_mwh_dsid IS NOT NULL THEN 'ds' ELSE generating_capacity_mwh_src END,
    turbine_number_src = CASE WHEN turbine_number_dsid IS NOT NULL THEN 'ds' ELSE turbine_number_src END,
    turbine_type_code_src = CASE WHEN turbine_type_code_dsid IS NOT NULL THEN 'ds' ELSE turbine_type_code_src END,
    up_passage_type_code_src = CASE WHEN up_passage_type_code_dsid IS NOT NULL THEN 'ds' ELSE up_passage_type_code_src END,
    down_passage_route_code_src = CASE WHEN down_passage_route_code_dsid IS NOT NULL THEN 'ds' ELSE down_passage_route_code_src END,
    passability_status_code_src = CASE WHEN passability_status_code_dsid IS NOT NULL THEN 'ds' ELSE passability_status_code_src END,
    passability_status_note_src = CASE WHEN passability_status_note_dsid IS NOT NULL THEN 'ds' ELSE passability_status_note_src END,
    comments_src = CASE WHEN comments_dsid IS NOT NULL THEN 'ds' ELSE comments_src END,
    complete_level_code_src = CASE WHEN complete_level_code_dsid IS NOT NULL THEN 'ds' ELSE complete_level_code_src END,
    original_point_src = CASE WHEN original_point_dsid IS NOT NULL THEN 'ds' ELSE original_point_src END,
    facility_name_en_src = CASE WHEN facility_name_en_dsid IS NOT NULL THEN 'ds' ELSE facility_name_en_src END,
    facility_name_fr_src = CASE WHEN facility_name_fr_dsid IS NOT NULL THEN 'ds' ELSE facility_name_fr_src END,
    removed_year_src = CASE WHEN removed_year_dsid IS NOT NULL THEN 'ds' ELSE removed_year_src END,
    construction_material_code_src = CASE WHEN construction_material_code_dsid IS NOT NULL THEN 'ds' ELSE construction_material_code_src END,
    use_conservation_code_src = CASE WHEN use_conservation_code_dsid IS NOT NULL THEN 'ds' ELSE use_conservation_code_src END,
    assessment_type_code_src = CASE WHEN assessment_type_code_dsid IS NOT NULL THEN 'ds' ELSE assessment_type_code_src END,
    addressed_status_code_src = CASE WHEN addressed_status_code_dsid IS NOT NULL THEN 'ds' ELSE addressed_status_code_src END,
    barrier_assessment_status_code_src = CASE WHEN barrier_assessment_status_code_dsid IS NOT NULL THEN 'ds' ELSE barrier_assessment_status_code_src END,
    barrier_removed_code_src = CASE WHEN barrier_removed_code_dsid IS NOT NULL THEN 'ds' ELSE barrier_removed_code_src end;



ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_s_federal_compliance_status_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_s_provincial_compliance_status_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_so_avg_rate_of_discharge_ls_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_so_use_invasivespecies_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_sou_degree_of_regulation_pc_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_sou_down_passage_route_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_sou_generating_capacity_mwh_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_sou_passability_status_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_sou_passability_status_note_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_sour_construction_type_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_sourc_operating_status_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_sourc_use_floodcontrol_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_assess_schedule_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_catchment_area_skm_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_comments_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_complete_level_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_condition_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_construction_year_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_dam_name_en_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_dam_name_fr_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_expected_life_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_facility_name_en_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_facility_name_fr_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_federal_flow_req_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_function_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_height_m_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_hydro_peaking_system_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_lake_control_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_length_m_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_maintenance_last_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_maintenance_next_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_operating_notes_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_original_point_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_owner_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_ownership_type_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_provincial_flow_req_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_removed_year_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_reservoir_area_skm_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_reservoir_depth_m_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_reservoir_name_en_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_reservoir_name_fr_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_reservoir_present_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_size_class_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_spillway_capacity_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_spillway_type_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_storage_capacity_mcm_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_turbine_number_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_turbine_type_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_up_passage_type_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_upstream_linear_km_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_use_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_use_electricity_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_use_fish_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_use_irrigation_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_use_navigation_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_use_other_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_use_pollution_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_use_recreation_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_use_supply_code_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_waterbody_name_en_ds_fkey;
ALTER TABLE dams.dams_attribute_source DROP CONSTRAINT dams_medium_large_attribute_source_waterbody_name_fr_ds_fkey;

-- function to move data from dams community staging to dams community holding
-- DROP FUNCTION dams.dams_community_staging_insert_trigger();

CREATE OR REPLACE FUNCTION dams.dams_community_staging_insert_trg()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN

	if (NEW.status != 'NEW') then
		RETURN NEW;
	end if;

    insert into dams.dams_community_holding (
		status,
		id, 
		cabd_id, 
		user_id, 
		uploaded_datetime,
	    latitude,
	    longitude,
	    dam_name_en,
        addressed_status_code,
        assessment_type_code,
        size_class_code,
        up_passage_type_code,
        passability_status_code
	)values (
		'NEW',
		NEW.id, 
		NEW.cabd_id, 
		NEW.user_id, 
		NEW.uploaded_datetime,
		((NEW.data->'geometry'->>'coordinates')::jsonb ->> 1)::double precision,
		((NEW.data->'geometry'->>'coordinates')::jsonb ->> 0)::double precision,
		NEW.data->'properties'->>'dam_name',
        case when NEW.data->'properties'->>'has_fish_structure' ilike 'true' or NEW.data->'properties'->>'has_fish_structure' ilike 'Yes' then 3 else null end, --addressed_status_code
        1, --assessment_type_code
        cabd.lookup_community_attribute(NEW.data->'properties','dam_height'), --dam_height
        case when NEW.data->'properties'->>'has_fish_structure' ilike 'false' or NEW.data->'properties'->>'has_fish_structure' ilike 'No' then 8 else null end, --up_passage_type_code
        case when NEW.data->'properties'->>'has_fish_structure' ilike 'true' or NEW.data->'properties'->>'has_fish_structure' ilike 'Yes' then 2 else 1 end --passability_code
	);
	UPDATE dams.dams_community_staging SET status = 'PROCESSED' where id = NEW.id;
    RETURN NEW;
END;
$function$
;
alter function dams.dams_community_staging_insert_trg owner to cabd;
--associated trigger

create trigger dams_community_staging_trigger after
insert
    on
    dams.dams_community_staging for each row execute function dams.dams_community_staging_insert_trg();


-- migrate updates from community holding to core dams table

-- DROP FUNCTION dams.dams_community_holding_data_trg();
CREATE OR REPLACE FUNCTION dams.dams_community_holding_data_trg()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
  cabdid uuid;
  distance double precision;
  doupdate boolean;
  newpoint geometry;
  STREAM_SNAP_TOLERANCE integer := 500; --for snapping points to stream network chyf and nhn
  CABD_FEATURE_DISANCE_MATCH_TOLDERANCE integer:= 100; --maximum matching distance in meters

BEGIN

    --sites data to update
    if (NEW.status != 'REVIEWED') then
        return NEW;
    end if;

    -- only update the fields if the community data field is not null and if the cabd dams field is null or unknown.
    
    -- does a record exists
    select cabd_id into cabdid from dams.dams where cabd_id = NEW.cabd_id;

    newpoint := st_transform(st_setsrid(st_makepoint(NEW.longitude, NEW.latitude), 4326), 4617);

    if (cabdid is null) then
        -- look for distance match
        --matching "distance tolerance"	    
		SELECT dms.cabd_id, dms.dist into cabdid, distance
		FROM (select newpoint::geography as point) sat
		CROSS JOIN LATERAL (
		  SELECT b.cabd_id, b.original_point::geography <-> sat.point AS dist
		  FROM dams.dams b
		  ORDER BY dist
		  LIMIT 1
		) dms;

        if (distance is null or distance > CABD_FEATURE_DISANCE_MATCH_TOLDERANCE) then
            cabdid := null;
            distance := null;
        end if;

        if (cabdid is not null) THEN
            --want to update the cabd_id of this record
            update dams.dams_community_holding set cabd_id = cabdid where id = NEW.id;
        end if;
	end if;
	

    if (cabdid is not null) THEN

        raise notice 'cabd is not null %s', cabdid;

        if (NEW.dam_name_en is not null) then
            -- only update the fields if the community data field is not null and if the cabd dams field is null or unknown.
            select case when dam_name_en is null then true when dam_name_en = '' then true else false end into doupdate
            from dams.dams where cabd_id = cabdid;
            
            if (doupdate) then
                update dams.dams set dam_name_en = NEW.dam_name_en where cabd_id = cabdid;
                update dams.dams_attribute_source set dam_name_en_src = 'c', dam_name_en_dsid = NEW.id where cabd_id = cabdid;
            end if;
        end if;

        if (NEW.addressed_status_code is not null) then
            -- only update the fields if the community data field is not null and if the cabd dams field is null or unknown.
            select case when addressed_status_code is null then true when addressed_status_code = 99 then true else false end into doupdate
            from dams.dams where cabd_id = cabdid;
            
            if (doupdate) then
                update dams.dams set addressed_status_code = NEW.addressed_status_code where cabd_id = cabdid;
                update dams.dams_attribute_source set addressed_status_code_src = 'c', addressed_status_code_dsid = NEW.id where cabd_id = cabdid;
            end if;
        end if;

        if (NEW.assessment_type_code is not null) then
            -- only update the fields if the community data field is not null and if the cabd dams field is null or unknown.
            select case when assessment_type_code is null then true else false end into doupdate
            from dams.dams where cabd_id = cabdid;
            
            if (doupdate) then
                update dams.dams set assessment_type_code = NEW.assessment_type_code where cabd_id = cabdid;
                update dams.dams_attribute_source set assessment_type_code_src = 'c', assessment_type_code_dsid = NEW.id where cabd_id = cabdid;
            end if;
        end if;

        if (NEW.size_class_code is not null) then
            -- only update the fields if the community data field is not null and if the cabd dams field is null or unknown.
            select case when size_class_code is null then true when size_class_code = 99 then true else false end into doupdate
            from dams.dams where cabd_id = cabdid;
            
            if (doupdate) then
                update dams.dams set size_class_code = NEW.size_class_code where cabd_id = cabdid;
                update dams.dams_attribute_source set size_class_code_src = 'c', size_class_code_dsid = NEW.id where cabd_id = cabdid;
            end if;
        end if;

        if (NEW.up_passage_type_code is not null) then
            -- only update the fields if the community data field is not null and if the cabd dams field is null or unknown.
            select case when up_passage_type_code is null then true when up_passage_type_code = 99 then true else false end into doupdate
            from dams.dams where cabd_id = cabdid;
            
            if (doupdate) then
                update dams.dams set up_passage_type_code = NEW.up_passage_type_code where cabd_id = cabdid;
                update dams.dams_attribute_source set up_passage_type_code_src = 'c', up_passage_type_code_dsid = NEW.id where cabd_id = cabdid;
            end if;
        end if;

        if (NEW.passability_status_code is not null) then
            -- only update the fields if the community data field is not null and if the cabd dams field is null or unknown.
            select case when passability_status_code is null then true else false end into doupdate
            from dams.dams where cabd_id = cabdid;
            
            if (doupdate) then
                update dams.dams set passability_status_code = NEW.passability_status_code where cabd_id = cabdid;
                update dams.dams_attribute_source set passability_status_code_src = 'c', passability_status_code_dsid = NEW.id where cabd_id = cabdid;
            end if;
        end if;

        -- never update lat/lon/geometry fields from community data

        update dams.dams_community_holding set status = 'PROCESSED' where id = NEW.id;

    else

        --need to insert to dams
        insert into dams.dams(cabd_id, 
            dam_name_en, addressed_status_code, 
		    assessment_type_code, size_class_code, 
            up_passage_type_code, passability_status_code,
            original_point, snapped_point, snapped_ncc, 
		    province_territory_code, nhn_watershed_id
		    )
        values (NEW.cabd_id, 
            NEW.dam_name_en, NEW.addressed_status_code,
            NEW.assessment_type_code, NEW.size_class_code, 
            NEW.up_passage_type_code, NEW.passability_status_code,
            newpoint,
            cabd.snap_point_to_chyf_network( newpoint, STREAM_SNAP_TOLERANCE),
		    cabd.snap_point_to_nhn_network( newpoint, STREAM_SNAP_TOLERANCE),
            cabd.find_province_territory_code(newpoint),
            cabd.find_nhn_watershed_id(newpoint));
	

        --set attribute source
        insert into dams.dams_attribute_source(cabd_id, 
            dam_name_en_src, dam_name_en_dsid, 
            addressed_status_code_src, addressed_status_code_dsid, 
            assessment_type_code_src, assessment_type_code_dsid, 
            size_class_code_src, size_class_code_dsid,
            up_passage_type_code_src, up_passage_type_code_dsid, 
            passability_status_code_src, passability_status_code_dsid,
            original_point_src, original_point_dsid
        )VALUES(
            NEW.cabd_id, 
                case when NEW.dam_name_en is null then null else 'c' end,
                case when NEW.dam_name_en is null then null else NEW.id end,
                case when NEW.addressed_status_code is null then null else 'c' end,
                case when NEW.addressed_status_code is null then null else NEW.id end,
                case when NEW.assessment_type_code is null then null else 'c' end,
                case when NEW.assessment_type_code is null then null else NEW.id end,
                case when NEW.size_class_code is null then null else 'c' end,
                case when NEW.size_class_code is null then null else NEW.id end,
                case when NEW.up_passage_type_code is null then null else 'c' end,
                case when NEW.up_passage_type_code is null then null else NEW.id end,
                case when NEW.passability_status_code is null then null else 'c' end,
                case when NEW.passability_status_code is null then null else NEW.id end,
                'c', NEW.id --original point                
        );

	    update dams.dams_community_holding set status = 'PROCESSED' where id = NEW.id;

    END IF;
	RETURN NEW;
END;
$function$
;
alter function dams.dams_community_holding_data_trg owner to cabd;


create trigger dams_community_holding_data_trg after
insert
    or
update
    on
    dams.dams_community_holding for each row
    when ((new.status = 'REVIEWED'::cabd.community_holding_status_type)) execute function dams.dams_community_holding_data_trg();
    

--name cleanup
ALTER function stream_crossings.stream_crossing_community_staging_insert_trigger RENAME TO stream_crossing_community_staging_insert_trg;