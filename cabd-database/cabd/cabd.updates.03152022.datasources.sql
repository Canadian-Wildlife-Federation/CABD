--default name for the feature type in the data view

alter table cabd.feature_types add column default_featurename_field varchar(128);
update cabd.feature_types set default_featurename_field = 'name_id' where data_view = 'cabd.barriers_view';
update cabd.feature_types set default_featurename_field = 'dam_name_en' where data_view = 'cabd.dams_view';
update cabd.feature_types set default_featurename_field = 'fall_name_en' where data_view = 'cabd.waterfalls_view';
update cabd.feature_types set default_featurename_field = 'dam_name_en' where data_view = 'cabd.fishways_view';

alter table cabd.feature_types add column feature_source_table varchar(128);
update cabd.feature_types set feature_source_table = 'dams.dams_feature_source' where data_view = 'cabd.dams_view';
update cabd.feature_types set feature_source_table = 'waterfalls.waterfalls_feature_source' where data_view = 'cabd.waterfalls_view';
update cabd.feature_types set feature_source_table = 'fishways.fishways_feature_source' where data_view = 'cabd.fishways_view';



-----------------------------------------------------------------
--
--   WATERFALLS
--
-----------------------------------------------------------------
create table waterfalls.waterfalls_feature_source(
 cabd_id uuid not null,
 datasource_id uuid not null,
 datasource_feature_id varchar not null,
 primary key (cabd_id, datasource_id)
);

alter table waterfalls.waterfalls_feature_source 
add constraint waterfalls_feature_source_cabdid_fk 
foreign key (cabd_id) 
references waterfalls.waterfalls(cabd_id) on delete cascade;


alter table waterfalls.waterfalls_feature_source 
add constraint waterfalls_data_source_id_fk 
foreign key (datasource_id) 
references cabd.data_source(id) on delete restrict;


insert into waterfalls.waterfalls_feature_source (cabd_id, datasource_id, datasource_feature_id)
	select distinct * from (
	select cabd_id, fall_name_en_ds, fall_name_en_dsfid
	from waterfalls.waterfalls_attribute_source
	where fall_name_en_dsfid is not null
	union
	select cabd_id, fall_name_fr_ds, fall_name_fr_dsfid
	from waterfalls.waterfalls_attribute_source
	where fall_name_fr_dsfid is not null
	union
	select cabd_id, waterbody_name_en_ds, waterbody_name_en_dsfid
	from waterfalls.waterfalls_attribute_source
	where waterbody_name_en_dsfid is not null
	union
	select cabd_id, waterbody_name_fr_ds, waterbody_name_fr_dsfid
	from waterfalls.waterfalls_attribute_source
	where waterbody_name_fr_dsfid is not null
	union
	select cabd_id, municipality_ds, municipality_dsfid
	from waterfalls.waterfalls_attribute_source
	where municipality_dsfid is not null
	union
	select cabd_id, fall_name_en_ds, fall_name_en_dsfid
	from waterfalls.waterfalls_attribute_source
	where fall_name_en_dsfid is not null
	union
	select cabd_id, fall_height_m_ds, fall_height_m_dsfid
	from waterfalls.waterfalls_attribute_source
	where fall_height_m_dsfid is not null
	union
	select cabd_id, comments_ds, comments_dsfid
	from waterfalls.waterfalls_attribute_source
	where comments_dsfid is not null
	union
	select cabd_id, complete_level_code_ds, complete_level_code_dsfid
	from waterfalls.waterfalls_attribute_source
	where complete_level_code_dsfid is not null
	union
	select cabd_id, passability_status_code_ds, passability_status_code_dsfid
	from waterfalls.waterfalls_attribute_source
	where passability_status_code_dsfid is not null
	union
	select cabd_id, original_point_ds, original_point_dsfid
	from waterfalls.waterfalls_attribute_source
	where original_point_dsfid is not null
	) foo;
	
	
		
-----------------------------------------------------------------
--
--   FISHWAYS
--
-----------------------------------------------------------------
create table fishways.fishways_feature_source(
 cabd_id uuid not null,
 datasource_id uuid not null,
 datasource_feature_id varchar not null,
 primary key (cabd_id, datasource_id)
);

alter table fishways.fishways_feature_source 
add constraint fishways_feature_source_cabdid_fk 
foreign key (cabd_id) 
references fishways.fishways(cabd_id) on delete cascade;


alter table fishways.fishways_feature_source 
add constraint fishways_data_source_id_fk 
foreign key (datasource_id) 
references cabd.data_source(id) on delete restrict;

insert into fishways.fishways_feature_source (cabd_id, datasource_id, datasource_feature_id)
	select distinct * from (
	
SELECT cabd_id, dam_name_en_ds, dam_name_en_dsfid FROM fishways.fishways_attribute_source where dam_name_en_dsfid is not null
union
SELECT cabd_id, dam_name_fr_ds, dam_name_fr_dsfid FROM fishways.fishways_attribute_source where dam_name_fr_dsfid is not null
union
SELECT cabd_id, waterbody_name_en_ds, waterbody_name_en_dsfid FROM fishways.fishways_attribute_source where waterbody_name_en_dsfid is not null
union
SELECT cabd_id, waterbody_name_fr_ds, waterbody_name_fr_dsfid FROM fishways.fishways_attribute_source where waterbody_name_fr_dsfid is not null
union
SELECT cabd_id, river_name_en_ds, river_name_en_dsfid FROM fishways.fishways_attribute_source where river_name_en_dsfid is not null
union
SELECT cabd_id, river_name_fr_ds, river_name_fr_dsfid FROM fishways.fishways_attribute_source where river_name_fr_dsfid is not null
union
SELECT cabd_id, municipality_ds, municipality_dsfid FROM fishways.fishways_attribute_source where municipality_dsfid is not null
union
SELECT cabd_id, fishpass_type_code_ds, fishpass_type_code_dsfid FROM fishways.fishways_attribute_source where fishpass_type_code_dsfid is not null
union
SELECT cabd_id, monitoring_equipment_ds, monitoring_equipment_dsfid FROM fishways.fishways_attribute_source where monitoring_equipment_dsfid is not null
union
SELECT cabd_id, architect_ds, architect_dsfid FROM fishways.fishways_attribute_source where architect_dsfid is not null
union
SELECT cabd_id, contracted_by_ds, contracted_by_dsfid FROM fishways.fishways_attribute_source where contracted_by_dsfid is not null
union
SELECT cabd_id, constructed_by_ds, constructed_by_dsfid FROM fishways.fishways_attribute_source where constructed_by_dsfid is not null
union
SELECT cabd_id, plans_held_by_ds, plans_held_by_dsfid FROM fishways.fishways_attribute_source where plans_held_by_dsfid is not null
union
SELECT cabd_id, purpose_ds, purpose_dsfid FROM fishways.fishways_attribute_source where purpose_dsfid is not null
union
SELECT cabd_id, designed_on_biology_ds, designed_on_biology_dsfid FROM fishways.fishways_attribute_source where designed_on_biology_dsfid is not null
union
SELECT cabd_id, length_m_ds, length_m_dsfid FROM fishways.fishways_attribute_source where length_m_dsfid is not null
union
SELECT cabd_id, elevation_m_ds, elevation_m_dsfid FROM fishways.fishways_attribute_source where elevation_m_dsfid is not null
union
SELECT cabd_id, gradient_ds, gradient_dsfid FROM fishways.fishways_attribute_source where gradient_dsfid is not null
union
SELECT cabd_id, depth_m_ds, depth_m_dsfid FROM fishways.fishways_attribute_source where depth_m_dsfid is not null
union
SELECT cabd_id, entrance_location_code_ds, entrance_location_code_dsfid FROM fishways.fishways_attribute_source where entrance_location_code_dsfid is not null
union
SELECT cabd_id, entrance_position_code_ds, entrance_position_code_dsfid FROM fishways.fishways_attribute_source where entrance_position_code_dsfid is not null
union
SELECT cabd_id, modified_ds, modified_dsfid FROM fishways.fishways_attribute_source where modified_dsfid is not null
union
SELECT cabd_id, modification_year_ds, modification_year_dsfid FROM fishways.fishways_attribute_source where modification_year_dsfid is not null
union
SELECT cabd_id, modification_purpose_ds, modification_purpose_dsfid FROM fishways.fishways_attribute_source where modification_purpose_dsfid is not null
union
SELECT cabd_id, year_constructed_ds, year_constructed_dsfid FROM fishways.fishways_attribute_source where year_constructed_dsfid is not null
union
SELECT cabd_id, operated_by_ds, operated_by_dsfid FROM fishways.fishways_attribute_source where operated_by_dsfid is not null
union
SELECT cabd_id, operation_period_ds, operation_period_dsfid FROM fishways.fishways_attribute_source where operation_period_dsfid is not null
union
SELECT cabd_id, has_evaluating_studies_ds, has_evaluating_studies_dsfid FROM fishways.fishways_attribute_source where has_evaluating_studies_dsfid is not null
union
SELECT cabd_id, nature_of_evaluation_studies_ds, nature_of_evaluation_studies_dsfid FROM fishways.fishways_attribute_source where nature_of_evaluation_studies_dsfid is not null
union
SELECT cabd_id, engineering_notes_ds, engineering_notes_dsfid FROM fishways.fishways_attribute_source where engineering_notes_dsfid is not null
union
SELECT cabd_id, operating_notes_ds, operating_notes_dsfid FROM fishways.fishways_attribute_source where operating_notes_dsfid is not null
union
SELECT cabd_id, mean_fishway_velocity_ms_ds, mean_fishway_velocity_ms_dsfid FROM fishways.fishways_attribute_source where mean_fishway_velocity_ms_dsfid is not null
union
SELECT cabd_id, max_fishway_velocity_ms_ds, max_fishway_velocity_ms_dsfid FROM fishways.fishways_attribute_source where max_fishway_velocity_ms_dsfid is not null
union
SELECT cabd_id, estimate_of_attraction_pct_ds, estimate_of_attraction_pct_dsfid FROM fishways.fishways_attribute_source where estimate_of_attraction_pct_dsfid is not null
union
SELECT cabd_id, estimate_of_passage_success_pct_ds, estimate_of_passage_success_pct_dsfid FROM fishways.fishways_attribute_source where estimate_of_passage_success_pct_dsfid is not null
union
SELECT cabd_id, fishway_reference_id_ds, fishway_reference_id_dsfid FROM fishways.fishways_attribute_source where fishway_reference_id_dsfid is not null
union
SELECT cabd_id, complete_level_code_ds, complete_level_code_dsfid FROM fishways.fishways_attribute_source where complete_level_code_dsfid is not null
union
SELECT cabd_id, original_point_ds, original_point_dsfid FROM fishways.fishways_attribute_source where original_point_dsfid is not null
) foo;


-----------------------------------------------------------------
--
--   DAMS
--
-----------------------------------------------------------------
create table dams.dams_feature_source(
 cabd_id uuid not null,
 datasource_id uuid not null,
 datasource_feature_id varchar not null,
 primary key (cabd_id, datasource_id)
);

alter table dams.dams_feature_source 
add constraint dams_feature_source_cabdid_fk 
foreign key (cabd_id) 
references dams.dams(cabd_id) on delete cascade;


alter table dams.dams_feature_source 
add constraint dams_data_source_id_fk 
foreign key (datasource_id) 
references cabd.data_source(id) on delete restrict;

insert into dams.dams_feature_source (cabd_id, datasource_id, datasource_feature_id)
	select distinct * from (
SELECT cabd_id, dam_name_en_ds, dam_name_en_dsfid FROM dams.dams_attribute_source WHERE dam_name_en_dsfid IS NOT NULL UNION
SELECT cabd_id, dam_name_fr_ds, dam_name_fr_dsfid FROM dams.dams_attribute_source WHERE dam_name_fr_dsfid IS NOT NULL UNION
SELECT cabd_id, waterbody_name_en_ds, waterbody_name_en_dsfid FROM dams.dams_attribute_source WHERE waterbody_name_en_dsfid IS NOT NULL UNION
SELECT cabd_id, waterbody_name_fr_ds, waterbody_name_fr_dsfid FROM dams.dams_attribute_source WHERE waterbody_name_fr_dsfid IS NOT NULL UNION
SELECT cabd_id, reservoir_name_en_ds, reservoir_name_en_dsfid FROM dams.dams_attribute_source WHERE reservoir_name_en_dsfid IS NOT NULL UNION
SELECT cabd_id, reservoir_name_fr_ds, reservoir_name_fr_dsfid FROM dams.dams_attribute_source WHERE reservoir_name_fr_dsfid IS NOT NULL UNION
SELECT cabd_id, municipality_ds, municipality_dsfid FROM dams.dams_attribute_source WHERE municipality_dsfid IS NOT NULL UNION
SELECT cabd_id, owner_ds, owner_dsfid FROM dams.dams_attribute_source WHERE owner_dsfid IS NOT NULL UNION
SELECT cabd_id, ownership_type_code_ds, ownership_type_code_dsfid FROM dams.dams_attribute_source WHERE ownership_type_code_dsfid IS NOT NULL UNION
SELECT cabd_id, province_compliance_status_ds, province_compliance_status_dsfid FROM dams.dams_attribute_source WHERE province_compliance_status_dsfid IS NOT NULL UNION
SELECT cabd_id, federal_compliance_status_ds, federal_compliance_status_dsfid FROM dams.dams_attribute_source WHERE federal_compliance_status_dsfid IS NOT NULL UNION
SELECT cabd_id, operating_note_ds, operating_note_dsfid FROM dams.dams_attribute_source WHERE operating_note_dsfid IS NOT NULL UNION
SELECT cabd_id, operating_status_code_ds, operating_status_code_dsfid FROM dams.dams_attribute_source WHERE operating_status_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_code_ds, use_code_dsfid FROM dams.dams_attribute_source WHERE use_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_irrigation_code_ds, use_irrigation_code_dsfid FROM dams.dams_attribute_source WHERE use_irrigation_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_electricity_code_ds, use_electricity_code_dsfid FROM dams.dams_attribute_source WHERE use_electricity_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_supply_code_ds, use_supply_code_dsfid FROM dams.dams_attribute_source WHERE use_supply_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_floodcontrol_code_ds, use_floodcontrol_code_dsfid FROM dams.dams_attribute_source WHERE use_floodcontrol_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_recreation_code_ds, use_recreation_code_dsfid FROM dams.dams_attribute_source WHERE use_recreation_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_navigation_code_ds, use_navigation_code_dsfid FROM dams.dams_attribute_source WHERE use_navigation_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_fish_code_ds, use_fish_code_dsfid FROM dams.dams_attribute_source WHERE use_fish_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_pollution_code_ds, use_pollution_code_dsfid FROM dams.dams_attribute_source WHERE use_pollution_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_invasivespecies_code_ds, use_invasivespecies_code_dsfid FROM dams.dams_attribute_source WHERE use_invasivespecies_code_dsfid IS NOT NULL UNION
SELECT cabd_id, use_other_code_ds, use_other_code_dsfid FROM dams.dams_attribute_source WHERE use_other_code_dsfid IS NOT NULL UNION
SELECT cabd_id, lake_control_code_ds, lake_control_code_dsfid FROM dams.dams_attribute_source WHERE lake_control_code_dsfid IS NOT NULL UNION
SELECT cabd_id, construction_year_ds, construction_year_dsfid FROM dams.dams_attribute_source WHERE construction_year_dsfid IS NOT NULL UNION
SELECT cabd_id, assess_schedule_ds, assess_schedule_dsfid FROM dams.dams_attribute_source WHERE assess_schedule_dsfid IS NOT NULL UNION
SELECT cabd_id, expected_life_ds, expected_life_dsfid FROM dams.dams_attribute_source WHERE expected_life_dsfid IS NOT NULL UNION
SELECT cabd_id, maintenance_last_ds, maintenance_last_dsfid FROM dams.dams_attribute_source WHERE maintenance_last_dsfid IS NOT NULL UNION
SELECT cabd_id, maintenance_next_ds, maintenance_next_dsfid FROM dams.dams_attribute_source WHERE maintenance_next_dsfid IS NOT NULL UNION
SELECT cabd_id, function_code_ds, function_code_dsfid FROM dams.dams_attribute_source WHERE function_code_dsfid IS NOT NULL UNION
SELECT cabd_id, condition_code_ds, condition_code_dsfid FROM dams.dams_attribute_source WHERE condition_code_dsfid IS NOT NULL UNION
SELECT cabd_id, construction_type_code_ds, construction_type_code_dsfid FROM dams.dams_attribute_source WHERE construction_type_code_dsfid IS NOT NULL UNION
SELECT cabd_id, height_m_ds, height_m_dsfid FROM dams.dams_attribute_source WHERE height_m_dsfid IS NOT NULL UNION
SELECT cabd_id, length_m_ds, length_m_dsfid FROM dams.dams_attribute_source WHERE length_m_dsfid IS NOT NULL UNION
SELECT cabd_id, size_class_code_ds, size_class_code_dsfid FROM dams.dams_attribute_source WHERE size_class_code_dsfid IS NOT NULL UNION
SELECT cabd_id, spillway_capacity_ds, spillway_capacity_dsfid FROM dams.dams_attribute_source WHERE spillway_capacity_dsfid IS NOT NULL UNION
SELECT cabd_id, spillway_type_code_ds, spillway_type_code_dsfid FROM dams.dams_attribute_source WHERE spillway_type_code_dsfid IS NOT NULL UNION
SELECT cabd_id, reservoir_present_ds, reservoir_present_dsfid FROM dams.dams_attribute_source WHERE reservoir_present_dsfid IS NOT NULL UNION
SELECT cabd_id, reservoir_area_skm_ds, reservoir_area_skm_dsfid FROM dams.dams_attribute_source WHERE reservoir_area_skm_dsfid IS NOT NULL UNION
SELECT cabd_id, reservoir_depth_m_ds, reservoir_depth_m_dsfid FROM dams.dams_attribute_source WHERE reservoir_depth_m_dsfid IS NOT NULL UNION
SELECT cabd_id, storage_capacity_mcm_ds, storage_capacity_mcm_dsfid FROM dams.dams_attribute_source WHERE storage_capacity_mcm_dsfid IS NOT NULL UNION
SELECT cabd_id, avg_rate_of_discharge_ls_ds, avg_rate_of_discharge_ls_dsfid FROM dams.dams_attribute_source WHERE avg_rate_of_discharge_ls_dsfid IS NOT NULL UNION
SELECT cabd_id, degree_of_regulation_pc_ds, degree_of_regulation_pc_dsfid FROM dams.dams_attribute_source WHERE degree_of_regulation_pc_dsfid IS NOT NULL UNION
SELECT cabd_id, provincial_flow_req_ds, provincial_flow_req_dsfid FROM dams.dams_attribute_source WHERE provincial_flow_req_dsfid IS NOT NULL UNION
SELECT cabd_id, federal_flow_req_ds, federal_flow_req_dsfid FROM dams.dams_attribute_source WHERE federal_flow_req_dsfid IS NOT NULL UNION
SELECT cabd_id, catchment_area_skm_ds, catchment_area_skm_dsfid FROM dams.dams_attribute_source WHERE catchment_area_skm_dsfid IS NOT NULL UNION
SELECT cabd_id, upstream_linear_km_ds, upstream_linear_km_dsfid FROM dams.dams_attribute_source WHERE upstream_linear_km_dsfid IS NOT NULL UNION
SELECT cabd_id, hydro_peaking_system_ds, hydro_peaking_system_dsfid FROM dams.dams_attribute_source WHERE hydro_peaking_system_dsfid IS NOT NULL UNION
SELECT cabd_id, generating_capacity_mwh_ds, generating_capacity_mwh_dsfid FROM dams.dams_attribute_source WHERE generating_capacity_mwh_dsfid IS NOT NULL UNION
SELECT cabd_id, turbine_number_ds, turbine_number_dsfid FROM dams.dams_attribute_source WHERE turbine_number_dsfid IS NOT NULL UNION
SELECT cabd_id, turbine_type_code_ds, turbine_type_code_dsfid FROM dams.dams_attribute_source WHERE turbine_type_code_dsfid IS NOT NULL UNION
SELECT cabd_id, up_passage_type_code_ds, up_passage_type_code_dsfid FROM dams.dams_attribute_source WHERE up_passage_type_code_dsfid IS NOT NULL UNION
SELECT cabd_id, down_passage_route_code_ds, down_passage_route_code_dsfid FROM dams.dams_attribute_source WHERE down_passage_route_code_dsfid IS NOT NULL UNION
SELECT cabd_id, passability_status_code_ds, passability_status_code_dsfid FROM dams.dams_attribute_source WHERE passability_status_code_dsfid IS NOT NULL UNION
SELECT cabd_id, passability_status_note_ds, passability_status_note_dsfid FROM dams.dams_attribute_source WHERE passability_status_note_dsfid IS NOT NULL UNION
SELECT cabd_id, comments_ds, comments_dsfid FROM dams.dams_attribute_source WHERE comments_dsfid IS NOT NULL UNION
SELECT cabd_id, complete_level_code_ds, complete_level_code_dsfid FROM dams.dams_attribute_source WHERE complete_level_code_dsfid IS NOT NULL UNION
SELECT cabd_id, original_point_ds, original_point_dsfid FROM dams.dams_attribute_source WHERE original_point_dsfid IS NOT NULL   
) foo;



----------------------------------------------------------------------------------------------------
--no longer use the dsfid fields - these are replaced by the data sources
--table for each feature type. We won't remove them initially but
--should remove them eventually 
----------------------------------------------------------------------------------------------------
--dams
--alter table dams.dams_attribute_source drop column dam_name_en_dsfid;
--alter table dams.dams_attribute_source drop column dam_name_fr_dsfid;
--alter table dams.dams_attribute_source drop column waterbody_name_en_dsfid;
--alter table dams.dams_attribute_source drop column waterbody_name_fr_dsfid;
--alter table dams.dams_attribute_source drop column reservoir_name_en_dsfid;
--alter table dams.dams_attribute_source drop column reservoir_name_fr_dsfid;
--alter table dams.dams_attribute_source drop column owner_dsfid;
--alter table dams.dams_attribute_source drop column ownership_type_code_dsfid;
--alter table dams.dams_attribute_source drop column provincial_compliance_status_dsfid;
--alter table dams.dams_attribute_source drop column federal_compliance_status_dsfid;
--alter table dams.dams_attribute_source drop column operating_notes_dsfid;
--alter table dams.dams_attribute_source drop column operating_status_code_dsfid;
--alter table dams.dams_attribute_source drop column use_code_dsfid;
--alter table dams.dams_attribute_source drop column use_irrigation_code_dsfid;
--alter table dams.dams_attribute_source drop column use_electricity_code_dsfid;
--alter table dams.dams_attribute_source drop column use_supply_code_dsfid;
--alter table dams.dams_attribute_source drop column use_floodcontrol_code_dsfid;
--alter table dams.dams_attribute_source drop column use_recreation_code_dsfid;
--alter table dams.dams_attribute_source drop column use_navigation_code_dsfid;
--alter table dams.dams_attribute_source drop column use_fish_code_dsfid;
--alter table dams.dams_attribute_source drop column use_pollution_code_dsfid;
--alter table dams.dams_attribute_source drop column use_invasivespecies_code_dsfid;
--alter table dams.dams_attribute_source drop column use_other_code_dsfid;
--alter table dams.dams_attribute_source drop column lake_control_code_dsfid;
--alter table dams.dams_attribute_source drop column construction_year_dsfid;
--alter table dams.dams_attribute_source drop column assess_schedule_dsfid;
--alter table dams.dams_attribute_source drop column expected_life_dsfid;
--alter table dams.dams_attribute_source drop column maintenance_last_dsfid;
--alter table dams.dams_attribute_source drop column maintenance_next_dsfid;
--alter table dams.dams_attribute_source drop column function_code_dsfid;
--alter table dams.dams_attribute_source drop column condition_code_dsfid;
--alter table dams.dams_attribute_source drop column construction_type_code_dsfid;
--alter table dams.dams_attribute_source drop column height_m_dsfid;
--alter table dams.dams_attribute_source drop column length_m_dsfid;
--alter table dams.dams_attribute_source drop column size_class_code_dsfid;
--alter table dams.dams_attribute_source drop column spillway_capacity_dsfid;
--alter table dams.dams_attribute_source drop column spillway_type_code_dsfid;
--alter table dams.dams_attribute_source drop column reservoir_present_dsfid;
--alter table dams.dams_attribute_source drop column reservoir_area_skm_dsfid;
--alter table dams.dams_attribute_source drop column reservoir_depth_m_dsfid;
--alter table dams.dams_attribute_source drop column storage_capacity_mcm_dsfid;
--alter table dams.dams_attribute_source drop column avg_rate_of_discharge_ls_dsfid;
--alter table dams.dams_attribute_source drop column degree_of_regulation_pc_dsfid;
--alter table dams.dams_attribute_source drop column provincial_flow_req_dsfid;
--alter table dams.dams_attribute_source drop column federal_flow_req_dsfid;
--alter table dams.dams_attribute_source drop column catchment_area_skm_dsfid;
--alter table dams.dams_attribute_source drop column upstream_linear_km_dsfid;
--alter table dams.dams_attribute_source drop column hydro_peaking_system_dsfid;
--alter table dams.dams_attribute_source drop column generating_capacity_mwh_dsfid;
--alter table dams.dams_attribute_source drop column turbine_number_dsfid;
--alter table dams.dams_attribute_source drop column turbine_type_code_dsfid;
--alter table dams.dams_attribute_source drop column up_passage_type_code_dsfid;
--alter table dams.dams_attribute_source drop column down_passage_route_code_dsfid;
--alter table dams.dams_attribute_source drop column passability_status_code_dsfid;
--alter table dams.dams_attribute_source drop column passability_status_note_dsfid;
--alter table dams.dams_attribute_source drop column comments_dsfid;
--alter table dams.dams_attribute_source drop column complete_level_code_dsfid;
--alter table dams.dams_attribute_source drop column original_point_dsfid;
--alter table dams.dams_attribute_source drop column facility_name_en_dsfid;
--alter table dams.dams_attribute_source drop column facility_name_fr_dsfid;
--alter table dams.dams_attribute_source drop column removed_year_dsfid;


--waterfalls
--alter table waterfalls.waterfalls_attribute_source drop column fall_name_en_dsfid,
--alter table waterfalls.waterfalls_attribute_source drop column fall_name_fr_dsfid,
--alter table waterfalls.waterfalls_attribute_source drop column waterbody_name_en_dsfid,
--alter table waterfalls.waterfalls_attribute_source drop column waterbody_name_fr_dsfid,
--alter table waterfalls.waterfalls_attribute_source drop column fall_height_m_dsfid,
--alter table waterfalls.waterfalls_attribute_source drop column comments_dsfid,
--alter table waterfalls.waterfalls_attribute_source drop column complete_level_code_dsfid,
--alter table waterfalls.waterfalls_attribute_source drop column passability_status_code_dsfid,
--alter table waterfalls.waterfalls_attribute_source drop column original_point_dsfid;

--fishways
--alter table fishways.fishways_attribute_source drop column structure_name_en_dsfid;
--alter table fishways.fishways_attribute_source drop column structure_name_fr_dsfid;
--alter table fishways.fishways_attribute_source drop column waterbody_name_en_dsfid;
--alter table fishways.fishways_attribute_source drop column waterbody_name_fr_dsfid;
--alter table fishways.fishways_attribute_source drop column river_name_en_dsfid;
--alter table fishways.fishways_attribute_source drop column river_name_fr_dsfid;
--alter table fishways.fishways_attribute_source drop column fishpass_type_code_dsfid;
--alter table fishways.fishways_attribute_source drop column monitoring_equipment_dsfid;
--alter table fishways.fishways_attribute_source drop column architect_dsfid;
--alter table fishways.fishways_attribute_source drop column contracted_by_dsfid;
--alter table fishways.fishways_attribute_source drop column constructed_by_dsfid;
--alter table fishways.fishways_attribute_source drop column plans_held_by_dsfid;
--alter table fishways.fishways_attribute_source drop column purpose_dsfid;
--alter table fishways.fishways_attribute_source drop column designed_on_biology_dsfid;
--alter table fishways.fishways_attribute_source drop column length_m_dsfid;
--alter table fishways.fishways_attribute_source drop column elevation_m_dsfid;
--alter table fishways.fishways_attribute_source drop column gradient_dsfid;
--alter table fishways.fishways_attribute_source drop column depth_m_dsfid;
--alter table fishways.fishways_attribute_source drop column entrance_location_code_dsfid;
--alter table fishways.fishways_attribute_source drop column entrance_position_code_dsfid;
--alter table fishways.fishways_attribute_source drop column modified_dsfid;
--alter table fishways.fishways_attribute_source drop column modification_year_dsfid;
--alter table fishways.fishways_attribute_source drop column modification_purpose_dsfid;
--alter table fishways.fishways_attribute_source drop column year_constructed_dsfid;
--alter table fishways.fishways_attribute_source drop column operated_by_dsfid;
--alter table fishways.fishways_attribute_source drop column operation_period_dsfid;
--alter table fishways.fishways_attribute_source drop column has_evaluating_studies_dsfid;
--alter table fishways.fishways_attribute_source drop column nature_of_evaluation_studies_dsfid;
--alter table fishways.fishways_attribute_source drop column engineering_notes_dsfid;
--alter table fishways.fishways_attribute_source drop column operating_notes_dsfid;
--alter table fishways.fishways_attribute_source drop column mean_fishway_velocity_ms_dsfid;
--alter table fishways.fishways_attribute_source drop column max_fishway_velocity_ms_dsfid;
--alter table fishways.fishways_attribute_source drop column estimate_of_attraction_pct_dsfid;
--alter table fishways.fishways_attribute_source drop column estimate_of_passage_success_pct_dsfid;
--alter table fishways.fishways_attribute_source drop column fishway_reference_id_dsfid;
--alter table fishways.fishways_attribute_source drop column complete_level_code_dsfid;
--alter table fishways.fishways_attribute_source drop column original_point_dsfid;
