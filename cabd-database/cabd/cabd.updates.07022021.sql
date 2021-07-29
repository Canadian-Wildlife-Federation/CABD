-- this scripts add the data source attribute tables
-- for each of the feature types (dams, waterfalls, fishways) currently
-- in the database, and populates the data with the data source
-- information in the feature types, then removes the data source
-- field from that table and corresponding view

--------------- DATA SOURCE TABLE ------------------------
-- make the necessary data source attribute tracking tables
CREATE TABLE cabd.data_source(
  id uuid not null primary key,
  name varchar,
  version_date date,
  version_number varchar,
  source varchar,
  comments varchar
);


insert into cabd.data_source (id, name, version_date) 
select uuid_generate_v4(), a.data_source, '2021-03-31'
from (
select distinct data_source from dams.dams_medium_large where data_source is not null
union 
select distinct data_source from waterfalls.waterfalls where data_source is not null
union
select distinct data_source from fishways.fishways where data_source is not null
) a;

--------------- DAMS ------------------------
-- create dams attribute source table
CREATE TABLE dams.dams_medium_large_attribute_source (
	cabd_id uuid NOT NULL primary key references dams.dams_medium_large,
	dam_name_en_ds uuid REFERENCES cabd.data_source(id),
	dam_name_en_dsfid varchar,
	dam_name_fr_ds uuid REFERENCES cabd.data_source(id),
	dam_name_fr_dsfid varchar,
	waterbody_name_en_ds uuid REFERENCES cabd.data_source(id),
	waterbody_name_en_dsfid varchar,
	waterbody_name_fr_ds uuid REFERENCES cabd.data_source(id),
	waterbody_name_fr_dsfid varchar,
	reservoir_name_en_ds uuid REFERENCES cabd.data_source(id),
	reservoir_name_en_dsfid varchar,
	reservoir_name_fr_ds uuid REFERENCES cabd.data_source(id),
	reservoir_name_fr_dsfid varchar,
	watershed_group_code_ds uuid REFERENCES cabd.data_source(id),
	watershed_group_code_dsfid varchar,
	nhn_workunit_id_ds uuid REFERENCES cabd.data_source(id),
	nhn_workunit_id_dsfid varchar,
	province_territory_code_ds uuid REFERENCES cabd.data_source(id),
	province_territory_code_dsfid varchar,
	municipality_ds uuid REFERENCES cabd.data_source(id),
	municipality_dsfid varchar,
	owner_ds uuid REFERENCES cabd.data_source(id),
	owner_dsfid varchar,
	ownership_type_code_ds uuid REFERENCES cabd.data_source(id),
	ownership_type_code_dsfid varchar,
	province_compliance_status_ds uuid REFERENCES cabd.data_source(id),
	province_compliance_status_dsfid varchar,
	federal_compliance_status_ds uuid REFERENCES cabd.data_source(id),
	federal_compliance_status_dsfid varchar,
	operating_note_ds uuid REFERENCES cabd.data_source(id),
	operating_note_dsfid varchar, 
	operating_status_code_ds uuid REFERENCES cabd.data_source(id),
	operating_status_code_dsfid varchar,
	use_code_ds uuid REFERENCES cabd.data_source(id),
	use_code_dsfid varchar,
	use_irrigation_code_ds uuid REFERENCES cabd.data_source(id),
	use_irrigation_code_dsfid varchar,
	use_electricity_code_ds uuid REFERENCES cabd.data_source(id),
	use_electricity_code_dsfid varchar,
	use_supply_code_ds uuid REFERENCES cabd.data_source(id),
	use_supply_code_dsfid varchar,
	use_floodcontrol_code_ds uuid REFERENCES cabd.data_source(id),
	use_floodcontrol_code_dsfid varchar,
	use_recreation_code_ds uuid REFERENCES cabd.data_source(id),
	use_recreation_code_dsfid varchar,
	use_navigation_code_ds uuid REFERENCES cabd.data_source(id),
	use_navigation_code_dsfid varchar,
	use_fish_code_ds uuid REFERENCES cabd.data_source(id),
	use_fish_code_dsfid varchar,
	use_pollution_code_ds uuid REFERENCES cabd.data_source(id),
	use_pollution_code_dsfid varchar,
	use_invasivespecies_code_ds uuid REFERENCES cabd.data_source(id),
	use_invasivespecies_code_dsfid varchar,
	use_other_code_ds uuid REFERENCES cabd.data_source(id),
	use_other_code_dsfid varchar,
	lake_control_code_ds uuid REFERENCES cabd.data_source(id),
	lake_control_code_dsfid varchar,
	construction_year_ds uuid REFERENCES cabd.data_source(id),
	construction_year_dsfid varchar,
	assess_schedule_ds uuid REFERENCES cabd.data_source(id),
	assess_schedule_dsfid varchar,
	expected_life_ds uuid REFERENCES cabd.data_source(id),
	expected_life_dsfid varchar,
	maintenance_last_ds uuid REFERENCES cabd.data_source(id),
	maintenance_last_dsfid varchar,
	maintenance_next_ds uuid REFERENCES cabd.data_source(id),
	maintenance_next_dsfid varchar,
	function_code_ds uuid REFERENCES cabd.data_source(id),
	function_code_dsfid varchar,
	condition_code_ds uuid REFERENCES cabd.data_source(id),
	condition_code_dsfid varchar,
	construction_type_code_ds uuid REFERENCES cabd.data_source(id),
	construction_type_code_dsfid varchar,
	height_m_ds uuid REFERENCES cabd.data_source(id),
	height_m_dsfid varchar,
	length_m_ds uuid REFERENCES cabd.data_source(id),
	length_m_dsfid varchar,
	size_class_code_ds uuid REFERENCES cabd.data_source(id),
	size_class_code_dsfid varchar,
	spillway_capacity_ds uuid REFERENCES cabd.data_source(id),
	spillway_capacity_dsfid varchar,
	spillway_type_code_ds uuid REFERENCES cabd.data_source(id),
	spillway_type_code_dsfid varchar,
	reservoir_present_ds uuid REFERENCES cabd.data_source(id),
	reservoir_present_dsfid varchar,
	reservoir_area_skm_ds uuid REFERENCES cabd.data_source(id),
	reservoir_area_skm_dsfid varchar,
	reservoir_depth_m_ds uuid REFERENCES cabd.data_source(id),
	reservoir_depth_m_dsfid varchar,
	storage_capacity_mcm_ds uuid REFERENCES cabd.data_source(id),
	storage_capacity_mcm_dsfid varchar,
	avg_rate_of_discharge_ls_ds uuid REFERENCES cabd.data_source(id),
	avg_rate_of_discharge_ls_dsfid varchar,
	degree_of_regulation_pc_ds uuid REFERENCES cabd.data_source(id),
	degree_of_regulation_pc_dsfid varchar,
	provincial_flow_req_ds uuid REFERENCES cabd.data_source(id),
	provincial_flow_req_dsfid varchar,
	federal_flow_req_ds uuid REFERENCES cabd.data_source(id),
	federal_flow_req_dsfid varchar,
	catchment_area_skm_ds uuid REFERENCES cabd.data_source(id),
	catchment_area_skm_dsfid varchar,
	upstream_linear_km_ds uuid REFERENCES cabd.data_source(id),
	upstream_linear_km_dsfid varchar,
	hydro_peaking_system_ds uuid REFERENCES cabd.data_source(id),
	hydro_peaking_system_dsfid varchar,
	generating_capacity_mwh_ds uuid REFERENCES cabd.data_source(id),
	generating_capacity_mwh_dsfid varchar,
	turbine_number_ds uuid REFERENCES cabd.data_source(id),
	turbine_number_dsfid varchar,
	turbine_type_code_ds uuid REFERENCES cabd.data_source(id),
	turbine_type_code_dsfid varchar,
	up_passage_type_code_ds uuid REFERENCES cabd.data_source(id),
	up_passage_type_code_dsfid varchar,
	down_passage_route_code_ds uuid REFERENCES cabd.data_source(id),
	down_passage_route_code_dsfid varchar,
	passability_status_code_ds uuid REFERENCES cabd.data_source(id),
	passability_status_code_dsfid varchar,
	passability_status_note_ds uuid REFERENCES cabd.data_source(id),
	passability_status_note_dsfid varchar,
	last_modified_ds uuid REFERENCES cabd.data_source(id),
	last_modified_dsfid varchar,
	comments_ds uuid REFERENCES cabd.data_source(id),
	comments_dsfid varchar,
	complete_level_code_ds uuid REFERENCES cabd.data_source(id),
	complete_level_code_dsfid varchar,
	original_point_ds uuid REFERENCES cabd.data_source(id),
	original_point_dsfid varchar
);

--update dams attribute source table based on existing data in the dams table
insert into dams.dams_medium_large_attribute_source
(cabd_id, dam_name_en_ds, dam_name_en_dsfid, dam_name_fr_ds, dam_name_fr_dsfid, 
waterbody_name_en_ds, waterbody_name_en_dsfid, waterbody_name_fr_ds, waterbody_name_fr_dsfid, 
reservoir_name_en_ds, reservoir_name_en_dsfid, reservoir_name_fr_ds, reservoir_name_fr_dsfid, 
watershed_group_code_ds, watershed_group_code_dsfid, nhn_workunit_id_ds, nhn_workunit_id_dsfid, 
province_territory_code_ds, province_territory_code_dsfid, municipality_ds, 
municipality_dsfid, owner_ds, owner_dsfid, ownership_type_code_ds, 
ownership_type_code_dsfid, province_compliance_status_ds, province_compliance_status_dsfid, 
federal_compliance_status_ds, federal_compliance_status_dsfid, operating_note_ds, 
operating_note_dsfid, operating_status_code_ds, operating_status_code_dsfid, use_code_ds, 
use_code_dsfid, use_irrigation_code_ds, use_irrigation_code_dsfid, use_electricity_code_ds, 
use_electricity_code_dsfid, use_supply_code_ds, use_supply_code_dsfid, use_floodcontrol_code_ds, 
use_floodcontrol_code_dsfid, use_recreation_code_ds, use_recreation_code_dsfid, 
use_navigation_code_ds, use_navigation_code_dsfid, use_fish_code_ds, use_fish_code_dsfid, 
use_pollution_code_ds, use_pollution_code_dsfid, use_invasivespecies_code_ds, 
use_invasivespecies_code_dsfid, use_other_code_ds, use_other_code_dsfid, lake_control_code_ds, 
lake_control_code_dsfid, construction_year_ds, construction_year_dsfid, assess_schedule_ds, 
assess_schedule_dsfid, expected_life_ds, expected_life_dsfid, maintenance_last_ds, 
maintenance_last_dsfid, maintenance_next_ds, maintenance_next_dsfid, function_code_ds, 
function_code_dsfid, condition_code_ds, condition_code_dsfid, construction_type_code_ds, 
construction_type_code_dsfid, height_m_ds, height_m_dsfid, length_m_ds, length_m_dsfid,
size_class_code_ds, size_class_code_dsfid, spillway_capacity_ds, spillway_capacity_dsfid, 
spillway_type_code_ds, spillway_type_code_dsfid, reservoir_present_ds, reservoir_present_dsfid, 
reservoir_area_skm_ds, reservoir_area_skm_dsfid, reservoir_depth_m_ds, reservoir_depth_m_dsfid, 
storage_capacity_mcm_ds, storage_capacity_mcm_dsfid, avg_rate_of_discharge_ls_ds, 
avg_rate_of_discharge_ls_dsfid, degree_of_regulation_pc_ds, degree_of_regulation_pc_dsfid, 
provincial_flow_req_ds, provincial_flow_req_dsfid, federal_flow_req_ds, federal_flow_req_dsfid, 
catchment_area_skm_ds, catchment_area_skm_dsfid, upstream_linear_km_ds, upstream_linear_km_dsfid, 
hydro_peaking_system_ds, hydro_peaking_system_dsfid, generating_capacity_mwh_ds, 
generating_capacity_mwh_dsfid, turbine_number_ds, turbine_number_dsfid, turbine_type_code_ds, 
turbine_type_code_dsfid, up_passage_type_code_ds, up_passage_type_code_dsfid, 
down_passage_route_code_ds, down_passage_route_code_dsfid, passability_status_code_ds, 
passability_status_code_dsfid, passability_status_note_ds, passability_status_note_dsfid, 
last_modified_ds, last_modified_dsfid, comments_ds, comments_dsfid, 
complete_level_code_ds, complete_level_code_dsfid, original_point_ds, original_point_dsfid)

select a.cabd_id,
case when a.dam_name_en is not null then b.id else null end,
case when a.dam_name_en is not null then a.data_source_id else null end,
case when a.dam_name_fr is not null then b.id else null end,
case when a.dam_name_fr is not null then a.data_source_id else null end,
case when a.waterbody_name_en is not null then b.id else null end,
case when a.waterbody_name_en is not null then a.data_source_id else null end,
case when a.waterbody_name_fr is not null then b.id else null end,
case when a.waterbody_name_fr is not null then a.data_source_id else null end,
case when a.reservoir_name_en is not null then b.id else null end,
case when a.reservoir_name_en is not null then a.data_source_id else null end,
case when a.reservoir_name_fr is not null then b.id else null end,
case when a.reservoir_name_fr is not null then a.data_source_id else null end,
case when a.watershed_group_code is not null then b.id else null end,
case when a.watershed_group_code is not null then a.data_source_id else null end,
case when a.nhn_workunit_id is not null then b.id else null end,
case when a.nhn_workunit_id is not null then a.data_source_id else null end,
case when a.province_territory_code is not null then b.id else null end,
case when a.province_territory_code is not null then a.data_source_id else null end,
case when a.municipality is not null then b.id else null end,
case when a.municipality is not null then a.data_source_id else null end,
case when a."owner" is not null then b.id else null end,
case when a."owner" is not null then a.data_source_id else null end,
case when a.ownership_type_code is not null then b.id else null end,
case when a.ownership_type_code is not null then a.data_source_id else null end,
case when a.province_compliance_status is not null then b.id else null end,
case when a.province_compliance_status is not null then a.data_source_id else null end,
case when a.federal_compliance_status is not null then b.id else null end,
case when a.federal_compliance_status is not null then a.data_source_id else null end,
case when a.operating_note is not null then b.id else null end,
case when a.operating_note is not null then a.data_source_id else null end,
case when a.operating_status_code is not null then b.id else null end,
case when a.operating_status_code is not null then a.data_source_id else null end,
case when a.use_code is not null then b.id else null end,
case when a.use_code is not null then a.data_source_id else null end,
case when a.use_irrigation_code is not null then b.id else null end,
case when a.use_irrigation_code is not null then a.data_source_id else null end,
case when a.use_electricity_code is not null then b.id else null end,
case when a.use_electricity_code is not null then a.data_source_id else null end,
case when a.use_supply_code is not null then b.id else null end,
case when a.use_supply_code is not null then a.data_source_id else null end,
case when a.use_floodcontrol_code is not null then b.id else null end,
case when a.use_floodcontrol_code is not null then a.data_source_id else null end,
case when a.use_recreation_code is not null then b.id else null end,
case when a.use_recreation_code is not null then a.data_source_id else null end,
case when a.use_navigation_code is not null then b.id else null end,
case when a.use_navigation_code is not null then a.data_source_id else null end,
case when a.use_fish_code is not null then b.id else null end,
case when a.use_fish_code is not null then a.data_source_id else null end,
case when a.use_pollution_code is not null then b.id else null end,
case when a.use_pollution_code is not null then a.data_source_id else null end,
case when a.use_invasivespecies_code is not null then b.id else null end,
case when a.use_invasivespecies_code is not null then a.data_source_id else null end,
case when a.use_other_code is not null then b.id else null end,
case when a.use_other_code is not null then a.data_source_id else null end,
case when a.lake_control_code is not null then b.id else null end,
case when a.lake_control_code is not null then a.data_source_id else null end,
case when a.construction_year is not null then b.id else null end,
case when a.construction_year is not null then a.data_source_id else null end,
case when a.assess_schedule is not null then b.id else null end,
case when a.assess_schedule is not null then a.data_source_id else null end,
case when a.expected_life is not null then b.id else null end,
case when a.expected_life is not null then a.data_source_id else null end,
case when a.maintenance_last is not null then b.id else null end,
case when a.maintenance_last is not null then a.data_source_id else null end,
case when a.maintenance_next is not null then b.id else null end,
case when a.maintenance_next is not null then a.data_source_id else null end,
case when a.function_code is not null then b.id else null end,
case when a.function_code is not null then a.data_source_id else null end,
case when a.condition_code is not null then b.id else null end,
case when a.condition_code is not null then a.data_source_id else null end,
case when a.construction_type_code is not null then b.id else null end,
case when a.construction_type_code is not null then a.data_source_id else null end,
case when a.height_m is not null then b.id else null end,
case when a.height_m is not null then a.data_source_id else null end,
case when a.length_m is not null then b.id else null end,
case when a.length_m is not null then a.data_source_id else null end,
case when a.size_class_code is not null then b.id else null end,
case when a.size_class_code is not null then a.data_source_id else null end,
case when a.spillway_capacity is not null then b.id else null end,
case when a.spillway_capacity is not null then a.data_source_id else null end,
case when a.spillway_type_code is not null then b.id else null end,
case when a.spillway_type_code is not null then a.data_source_id else null end,
case when a.reservoir_present is not null then b.id else null end,
case when a.reservoir_present is not null then a.data_source_id else null end,
case when a.reservoir_area_skm is not null then b.id else null end,
case when a.reservoir_area_skm is not null then a.data_source_id else null end,
case when a.reservoir_depth_m is not null then b.id else null end,
case when a.reservoir_depth_m is not null then a.data_source_id else null end,
case when a.storage_capacity_mcm is not null then b.id else null end,
case when a.storage_capacity_mcm is not null then a.data_source_id else null end,
case when a.avg_rate_of_discharge_ls is not null then b.id else null end,
case when a.avg_rate_of_discharge_ls is not null then a.data_source_id else null end,
case when a.degree_of_regulation_pc is not null then b.id else null end,
case when a.degree_of_regulation_pc is not null then a.data_source_id else null end,
case when a.provincial_flow_req is not null then b.id else null end,
case when a.provincial_flow_req is not null then a.data_source_id else null end,
case when a.federal_flow_req is not null then b.id else null end,
case when a.federal_flow_req is not null then a.data_source_id else null end,
case when a.catchment_area_skm is not null then b.id else null end,
case when a.catchment_area_skm is not null then a.data_source_id else null end,
case when a.upstream_linear_km is not null then b.id else null end,
case when a.upstream_linear_km is not null then a.data_source_id else null end,
case when a.hydro_peaking_system is not null then b.id else null end,
case when a.hydro_peaking_system is not null then a.data_source_id else null end,
case when a.generating_capacity_mwh is not null then b.id else null end,
case when a.generating_capacity_mwh is not null then a.data_source_id else null end,
case when a.turbine_number is not null then b.id else null end,
case when a.turbine_number is not null then a.data_source_id else null end,
case when a.turbine_type_code is not null then b.id else null end,
case when a.turbine_type_code is not null then a.data_source_id else null end,
case when a.up_passage_type_code is not null then b.id else null end,
case when a.up_passage_type_code is not null then a.data_source_id else null end,
case when a.down_passage_route_code is not null then b.id else null end,
case when a.down_passage_route_code is not null then a.data_source_id else null end,
case when a.passability_status_code is not null then b.id else null end,
case when a.passability_status_code is not null then a.data_source_id else null end,
case when a.passability_status_note is not null then b.id else null end,
case when a.passability_status_note is not null then a.data_source_id else null end,
case when a.last_modified is not null then b.id else null end,
case when a.last_modified is not null then a.data_source_id else null end,
case when a."comments" is not null then b.id else null end,
case when a."comments" is not null then a.data_source_id else null end,
case when a.complete_level_code is not null then b.id else null end,
case when a.complete_level_code is not null then a.data_source_id else null end,
case when a.original_point is not null then b.id else null end,
case when a.original_point is not null then a.data_source_id else null end
from
dams.dams_medium_large a left join cabd.data_source b on a.data_source = b.name
where a.data_source is not null;

--drop data source and data source id column from original table
delete from cabd.feature_type_metadata where view_name = 'cabd.dams_medium_large_view'
and (field_name = 'data_source_id' or field_name = 'data_source');

update cabd.feature_type_metadata set vw_simple_order = vw_simple_order - 2 
where view_name = 'cabd.dams_medium_large_view' and vw_simple_order > 19 and vw_simple_order is not null;

update cabd.feature_type_metadata set vw_all_order = vw_all_order - 2 
where view_name = 'cabd.dams_medium_large_view' and vw_all_order > 33 and vw_all_order is not null;

--drop columns from view
DROP VIEW cabd.dams_medium_large_view;

CREATE OR REPLACE VIEW cabd.dams_medium_large_view
AS SELECT d.cabd_id,
    'dams_medium_large'::text AS feature_type,
    st_y(d.snapped_point) as latitude,
    st_x(d.snapped_point) as longitude,
    d.dam_name_en,
    d.dam_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.reservoir_name_en,
    d.reservoir_name_fr,
    d.watershed_group_code,
    wg.name AS watershed_group_name,
    d.nhn_workunit_id,
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
    ps.name as passability_status,
    d.passability_status_note,
    d.complete_level_code,
    cl.name AS complete_level,
    d.snapped_point AS geometry
   FROM dams.dams_medium_large d
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
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = d.watershed_group_code::text
     LEFT JOIN dams.passability_status_codes ps ON ps.code = d.passability_status_code;

GRANT ALL PRIVILEGES ON cabd.dams_medium_large_view to cabd;

ALTER TABLE dams.dams_medium_large drop column data_source;
ALTER TABLE dams.dams_medium_large drop column data_source_id;

---------- WATERFALLS ---------------
CREATE TABLE waterfalls.waterfalls_attribute_source (
	cabd_id uuid NOT NULL primary key references waterfalls.waterfalls,
	fall_name_en_ds uuid REFERENCES cabd.data_source(id),
	fall_name_en_dsfid varchar,
	fall_name_fr_ds uuid REFERENCES cabd.data_source(id),
	fall_name_fr_dsfid varchar,
	waterbody_name_en_ds uuid REFERENCES cabd.data_source(id),
	waterbody_name_en_dsfid varchar,
	waterbody_name_fr_ds uuid REFERENCES cabd.data_source(id),
	waterbody_name_fr_dsfid varchar,
	watershed_group_code_ds uuid REFERENCES cabd.data_source(id),
	watershed_group_code_dsfid varchar,
	nhn_workunit_id_ds uuid REFERENCES cabd.data_source(id),
	nhn_workunit_id_dsfid varchar,
	province_territory_code_ds uuid REFERENCES cabd.data_source(id),
	province_territory_code_dsfid varchar,
	municipality_ds uuid REFERENCES cabd.data_source(id),
	municipality_dsfid varchar,
	fall_height_m_ds uuid REFERENCES cabd.data_source(id),
	fall_height_m_dsfid varchar,
	last_modified_ds uuid REFERENCES cabd.data_source(id),
	last_modified_dsfid varchar,
	comments_ds uuid REFERENCES cabd.data_source(id),
	comments_dsfid varchar,
	complete_level_code_ds uuid REFERENCES cabd.data_source(id),
	complete_level_code_dsfid varchar,
	passability_status_code_ds uuid REFERENCES cabd.data_source(id),
	passability_status_code_dsfis varchar,
	original_point_ds uuid REFERENCES cabd.data_source(id),
	original_point_dsfid varchar
);

insert into waterfalls.waterfalls_attribute_source
(cabd_id, fall_name_en_ds, fall_name_en_dsfid, fall_name_fr_ds, fall_name_fr_dsfid, waterbody_name_en_ds,
waterbody_name_en_dsfid, waterbody_name_fr_ds, waterbody_name_fr_dsfid, watershed_group_code_ds,
watershed_group_code_dsfid, nhn_workunit_id_ds, nhn_workunit_id_dsfid, province_territory_code_ds,
province_territory_code_dsfid, municipality_ds, municipality_dsfid, fall_height_m_ds,
fall_height_m_dsfid, last_modified_ds, last_modified_dsfid, comments_ds,
comments_dsfid, complete_level_code_ds, complete_level_code_dsfid, passability_status_code_ds,
passability_status_code_dsfis, original_point_ds, original_point_dsfid)

select a.cabd_id,
case when a.fall_name_en is not null then b.id else null end,
case when a.fall_name_en is not null then a.data_source_id else null end,
case when a.fall_name_fr is not null then b.id else null end,
case when a.fall_name_fr is not null then a.data_source_id else null end,
case when a.waterbody_name_en is not null then b.id else null end,
case when a.waterbody_name_en is not null then a.data_source_id else null end,
case when a.waterbody_name_fr is not null then b.id else null end,
case when a.waterbody_name_fr is not null then a.data_source_id else null end,
case when a.watershed_group_code is not null then b.id else null end,
case when a.watershed_group_code is not null then a.data_source_id else null end,
case when a.nhn_workunit_id is not null then b.id else null end,
case when a.nhn_workunit_id is not null then a.data_source_id else null end,
case when a.province_territory_code is not null then b.id else null end,
case when a.province_territory_code is not null then a.data_source_id else null end,
case when a.municipality is not null then b.id else null end,
case when a.municipality is not null then a.data_source_id else null end,
case when a.fall_height_m is not null then b.id else null end,
case when a.fall_height_m is not null then a.data_source_id else null end,
case when a.last_modified is not null then b.id else null end,
case when a.last_modified is not null then a.data_source_id else null end,
case when a."comments" is not null then b.id else null end,
case when a."comments" is not null then a.data_source_id else null end,
case when a.complete_level_code is not null then b.id else null end,
case when a.complete_level_code is not null then a.data_source_id else null end,
case when a.passability_status_code is not null then b.id else null end,
case when a.passability_status_code is not null then a.data_source_id else null end,
case when a.original_point is not null then b.id else null end,
case when a.original_point is not null then a.data_source_id else null end
from
waterfalls.waterfalls a left join cabd.data_source b on a.data_source = b.name
where a.data_source is not null;

--drop data source and data source id column from original table
delete from cabd.feature_type_metadata where view_name = 'cabd.waterfalls_view'
and (field_name = 'data_source_id' or field_name = 'data_source');

update cabd.feature_type_metadata set vw_simple_order = vw_simple_order - 2 
where view_name = 'cabd.waterfalls_view' and vw_simple_order > 9 and vw_simple_order is not null;

update cabd.feature_type_metadata set vw_all_order = vw_all_order - 2 
where view_name = 'cabd.waterfalls_view' and vw_all_order > 14 and vw_all_order is not null;

--drop columns from view
DROP VIEW cabd.waterfalls_view; 
CREATE OR REPLACE VIEW cabd.waterfalls_view
AS SELECT w.cabd_id,
    'waterfalls'::text AS feature_type,
    st_y(w.snapped_point) as latitude,
    st_x(w.snapped_point) as longitude,
    w.fall_name_en,
    w.fall_name_fr,
    w.waterbody_name_en,
    w.waterbody_name_fr,
    w.watershed_group_code,
    wg.name AS watershed_group_name,
    w.nhn_workunit_id,
    w.province_territory_code,
    pt.name AS province_territory,
    w.municipality,
    w.fall_height_m,
    w.last_modified,
    w.comments,
    w.complete_level_code,
    cl.name AS complete_level,
    w.passability_status_code,
    ps.name as passability_status,
    w.snapped_point as geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = w.watershed_group_code::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code;

     
grant all privileges on cabd.waterfalls_view to cabd;

ALTER TABLE waterfalls.waterfalls drop column data_source;
ALTER TABLE waterfalls.waterfalls drop column data_source_id;

------------------ FISHWAYS -------------------------------

CREATE TABLE fishways.fishways_attribute_source (
	cabd_id uuid NOT NULL primary key references fishways.fishways,
	dam_name_en_ds uuid REFERENCES cabd.data_source(id),
	dam_name_en_dsfid varchar,
	dam_name_fr_ds uuid REFERENCES cabd.data_source(id),
	dam_name_fr_dsfid varchar,
	waterbody_name_en_ds uuid REFERENCES cabd.data_source(id),
	waterbody_name_en_dsfid varchar,
	waterbody_name_fr_ds uuid REFERENCES cabd.data_source(id),
	waterbody_name_fr_dsfid varchar,
	river_name_en_ds uuid REFERENCES cabd.data_source(id),
	river_name_en_dsfid varchar,
	river_name_fr_ds uuid REFERENCES cabd.data_source(id),
	river_name_fr_dsfid varchar,
	watershed_group_code_ds uuid REFERENCES cabd.data_source(id),
	watershed_group_code_dsfid varchar,
	nhn_workunit_id_ds uuid REFERENCES cabd.data_source(id),
	nhn_workunit_id_dsfid varchar,
	province_territory_code_ds uuid REFERENCES cabd.data_source(id),
	province_territory_code_dsfid varchar,
	municipality_ds uuid REFERENCES cabd.data_source(id),
	municipality_dsfid varchar,
	fishpass_type_code_ds uuid REFERENCES cabd.data_source(id),
	fishpass_type_code_dsfid varchar,
	monitoring_equipment_ds uuid REFERENCES cabd.data_source(id),
	monitoring_equipment_dsfid varchar,
	architect_ds uuid REFERENCES cabd.data_source(id),
	architect_dsfid varchar,
	contracted_by_ds uuid REFERENCES cabd.data_source(id),
	contracted_by_dsfid varchar,
	constructed_by_ds uuid REFERENCES cabd.data_source(id),
	constructed_by_dsfid varchar,
	plans_held_by_ds uuid REFERENCES cabd.data_source(id),
	plans_held_by_dsfid varchar,
	purpose_ds uuid REFERENCES cabd.data_source(id),
	purpose_dsfid varchar,
	designed_on_biology_ds uuid REFERENCES cabd.data_source(id),
	designed_on_biology_dsfid varchar,
	length_m_ds uuid REFERENCES cabd.data_source(id),
	length_m_dsfid varchar,
	elevation_m_ds uuid REFERENCES cabd.data_source(id),
	elevation_m_dsfid varchar,
	inclination_pct_ds uuid REFERENCES cabd.data_source(id),
	inclination_pct_dsfid varchar,
	depth_m_ds uuid REFERENCES cabd.data_source(id),
	depth_m_dsfid varchar,
	entrance_location_code_ds uuid REFERENCES cabd.data_source(id),
	entrance_location_code_dsfid varchar,
	entrance_position_code_ds uuid REFERENCES cabd.data_source(id),
	entrance_position_code_dsfid varchar,
	modified_ds uuid REFERENCES cabd.data_source(id),
	modified_dsfid varchar,
	modification_year_ds uuid REFERENCES cabd.data_source(id),
	modification_year_dsfid varchar,
	modification_purpose_ds uuid REFERENCES cabd.data_source(id),
	modification_purpose_dsfid varchar,
	year_constructed_ds uuid REFERENCES cabd.data_source(id),
	year_constructed_dsfid varchar,
	operated_by_ds uuid REFERENCES cabd.data_source(id),
	operated_by_dsfid varchar,
	operation_period_ds uuid REFERENCES cabd.data_source(id),
	operation_period_dsfid varchar,
	has_evaluating_studies_ds uuid REFERENCES cabd.data_source(id),
	has_evaluating_studies_dsfid varchar,
	nature_of_evaluation_studies_ds uuid REFERENCES cabd.data_source(id),
	nature_of_evaluation_studies_dsfid varchar,
	engineering_notes_ds uuid REFERENCES cabd.data_source(id),
	engineering_notes_dsfid varchar,
	operating_notes_ds uuid REFERENCES cabd.data_source(id),
	operating_notes_dsfid varchar,
	mean_fishway_velocity_ms_ds uuid REFERENCES cabd.data_source(id),
	mean_fishway_velocity_ms_dsfid varchar,
	max_fishway_velocity_ms_ds uuid REFERENCES cabd.data_source(id),
	max_fishway_velocity_ms_dsfid varchar,
	estimate_of_attraction_pct_ds uuid REFERENCES cabd.data_source(id),
	estimate_of_attraction_pct_dsfid varchar,
	estimate_of_passage_success_pct_ds uuid REFERENCES cabd.data_source(id),
	estimate_of_passage_success_pct_dsfid varchar,
	fishway_reference_id_ds uuid REFERENCES cabd.data_source(id),
	fishway_reference_id_dsfid varchar,
	complete_level_code_ds uuid REFERENCES cabd.data_source(id),
	complete_level_code_dsfid varchar,
	original_point_ds uuid REFERENCES cabd.data_source(id),
	original_point_dsfid varchar
);


insert into fishways.fishways_attribute_source
(cabd_id, dam_name_en_ds, dam_name_en_dsfid, dam_name_fr_ds, dam_name_fr_dsfid, 
waterbody_name_en_ds, waterbody_name_en_dsfid, waterbody_name_fr_ds, waterbody_name_fr_dsfid, 
river_name_en_ds, river_name_en_dsfid, river_name_fr_ds, river_name_fr_dsfid, 
watershed_group_code_ds, watershed_group_code_dsfid, nhn_workunit_id_ds, nhn_workunit_id_dsfid, 
province_territory_code_ds, province_territory_code_dsfid, municipality_ds, 
municipality_dsfid, fishpass_type_code_ds, fishpass_type_code_dsfid, 
monitoring_equipment_ds, monitoring_equipment_dsfid, architect_ds, architect_dsfid, 
contracted_by_ds, contracted_by_dsfid, constructed_by_ds, constructed_by_dsfid, 
plans_held_by_ds, plans_held_by_dsfid, purpose_ds, purpose_dsfid, designed_on_biology_ds, 
designed_on_biology_dsfid, length_m_ds, length_m_dsfid, elevation_m_ds, 
elevation_m_dsfid, inclination_pct_ds, inclination_pct_dsfid, depth_m_ds, depth_m_dsfid, 
entrance_location_code_ds, entrance_location_code_dsfid, entrance_position_code_ds, 
entrance_position_code_dsfid, modified_ds, modified_dsfid, modification_year_ds, 
modification_year_dsfid, modification_purpose_ds, modification_purpose_dsfid, year_constructed_ds, 
year_constructed_dsfid, operated_by_ds, operated_by_dsfid, operation_period_ds, 
operation_period_dsfid, has_evaluating_studies_ds, has_evaluating_studies_dsfid, 
nature_of_evaluation_studies_ds, nature_of_evaluation_studies_dsfid, engineering_notes_ds, 
engineering_notes_dsfid, operating_notes_ds, operating_notes_dsfid, mean_fishway_velocity_ms_ds, 
mean_fishway_velocity_ms_dsfid, max_fishway_velocity_ms_ds, max_fishway_velocity_ms_dsfid, 
estimate_of_attraction_pct_ds, estimate_of_attraction_pct_dsfid, estimate_of_passage_success_pct_ds,
 estimate_of_passage_success_pct_dsfid, fishway_reference_id_ds, fishway_reference_id_dsfid,
  complete_level_code_ds, complete_level_code_dsfid, original_point_ds, original_point_dsfid)

select a.cabd_id,
case when a.dam_name_en is not null then b.id else null end,
case when a.dam_name_en is not null then a.data_source_id else null end,
case when a.dam_name_fr is not null then b.id else null end,
case when a.dam_name_fr is not null then a.data_source_id else null end,
case when a.waterbody_name_en is not null then b.id else null end,
case when a.waterbody_name_en is not null then a.data_source_id else null end,
case when a.waterbody_name_fr is not null then b.id else null end,
case when a.waterbody_name_fr is not null then a.data_source_id else null end,
case when a.river_name_en is not null then b.id else null end,
case when a.river_name_en is not null then a.data_source_id else null end,
case when a.river_name_fr is not null then b.id else null end,
case when a.river_name_fr is not null then a.data_source_id else null end,
case when a.watershed_group_code is not null then b.id else null end,
case when a.watershed_group_code is not null then a.data_source_id else null end,
case when a.nhn_workunit_id is not null then b.id else null end,
case when a.nhn_workunit_id is not null then a.data_source_id else null end,
case when a.province_territory_code is not null then b.id else null end,
case when a.province_territory_code is not null then a.data_source_id else null end,
case when a.municipality is not null then b.id else null end,
case when a.municipality is not null then a.data_source_id else null end,
case when a.fishpass_type_code is not null then b.id else null end,
case when a.fishpass_type_code is not null then a.data_source_id else null end,
case when a.monitoring_equipment is not null then b.id else null end,
case when a.monitoring_equipment is not null then a.data_source_id else null end,
case when a.architect is not null then b.id else null end,
case when a.architect is not null then a.data_source_id else null end,
case when a.contracted_by is not null then b.id else null end,
case when a.contracted_by is not null then a.data_source_id else null end,
case when a.constructed_by is not null then b.id else null end,
case when a.constructed_by is not null then a.data_source_id else null end,
case when a.plans_held_by is not null then b.id else null end,
case when a.plans_held_by is not null then a.data_source_id else null end,
case when a.purpose is not null then b.id else null end,
case when a.purpose is not null then a.data_source_id else null end,
case when a.designed_on_biology is not null then b.id else null end,
case when a.designed_on_biology is not null then a.data_source_id else null end,
case when a.length_m is not null then b.id else null end,
case when a.length_m is not null then a.data_source_id else null end,
case when a.elevation_m is not null then b.id else null end,
case when a.elevation_m is not null then a.data_source_id else null end,
case when a.inclination_pct is not null then b.id else null end,
case when a.inclination_pct is not null then a.data_source_id else null end,
case when a.depth_m is not null then b.id else null end,
case when a.depth_m is not null then a.data_source_id else null end,
case when a.entrance_location_code is not null then b.id else null end,
case when a.entrance_location_code is not null then a.data_source_id else null end,
case when a.entrance_position_code is not null then b.id else null end,
case when a.entrance_position_code is not null then a.data_source_id else null end,
case when a.modified is not null then b.id else null end,
case when a.modified is not null then a.data_source_id else null end,
case when a.modification_year is not null then b.id else null end,
case when a.modification_year is not null then a.data_source_id else null end,
case when a.modification_purpose is not null then b.id else null end,
case when a.modification_purpose is not null then a.data_source_id else null end,
case when a.year_constructed is not null then b.id else null end,
case when a.year_constructed is not null then a.data_source_id else null end,
case when a.operated_by is not null then b.id else null end,
case when a.operated_by is not null then a.data_source_id else null end,
case when a.operation_period is not null then b.id else null end,
case when a.operation_period is not null then a.data_source_id else null end,
case when a.has_evaluating_studies is not null then b.id else null end,
case when a.has_evaluating_studies is not null then a.data_source_id else null end,
case when a.nature_of_evaluation_studies is not null then b.id else null end,
case when a.nature_of_evaluation_studies is not null then a.data_source_id else null end,
case when a.engineering_notes is not null then b.id else null end,
case when a.engineering_notes is not null then a.data_source_id else null end,
case when a.operating_notes is not null then b.id else null end,
case when a.operating_notes is not null then a.data_source_id else null end,
case when a.mean_fishway_velocity_ms is not null then b.id else null end,
case when a.mean_fishway_velocity_ms is not null then a.data_source_id else null end,
case when a.max_fishway_velocity_ms is not null then b.id else null end,
case when a.max_fishway_velocity_ms is not null then a.data_source_id else null end,
case when a.estimate_of_attraction_pct is not null then b.id else null end,
case when a.estimate_of_attraction_pct is not null then a.data_source_id else null end,
case when a.estimate_of_passage_success_pct is not null then b.id else null end,
case when a.estimate_of_passage_success_pct is not null then a.data_source_id else null end,
case when a.fishway_reference_id is not null then b.id else null end,
case when a.fishway_reference_id is not null then a.data_source_id else null end,
case when a.complete_level_code is not null then b.id else null end,
case when a.complete_level_code is not null then a.data_source_id else null end,
case when a.original_point is not null then b.id else null end,
case when a.original_point is not null then a.data_source_id else null end
from
fishways.fishways a left join cabd.data_source b on a.data_source = b.name
where a.data_source is not null;


--drop data source and data source id column from original table
delete from cabd.feature_type_metadata where view_name = 'cabd.fishways_view'
and (field_name = 'data_source_id' or field_name = 'data_source');

update cabd.feature_type_metadata set vw_simple_order = vw_simple_order - 2 
where view_name = 'cabd.fishways_view' and vw_simple_order > 26 and vw_simple_order is not null;

update cabd.feature_type_metadata set vw_all_order = vw_all_order - 2 
where view_name = 'cabd.fishways_view' and vw_all_order > 47 and vw_all_order is not null;

--drop columns from view
drop view cabd.fishways_view;

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
    d.municipality,	
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

ALTER TABLE fishways.fishways drop column data_source;
ALTER TABLE fishways.fishways drop column data_source_id;



--fix some issues with metadata field orders that I noticed when reviewing results
update cabd.feature_type_metadata set vw_simple_order = 2 where view_name = 'cabd.barriers_view' and field_name = 'waterbody_name_en';
update cabd.feature_type_metadata set vw_simple_order = 3 where view_name = 'cabd.barriers_view' and field_name = 'reservoir_name_en';
update cabd.feature_type_metadata set vw_simple_order = 4 where view_name = 'cabd.barriers_view' and field_name = 'province_territory';
update cabd.feature_type_metadata set vw_simple_order = 5 where view_name = 'cabd.barriers_view' and field_name = 'passability_status';
update cabd.feature_type_metadata set vw_all_order = vw_all_order+1 where view_name = 'cabd.dams_medium_large_view' and vw_all_order > 24;
update cabd.feature_type_metadata set vw_all_order = 25 where view_name = 'cabd.dams_medium_large_view' and field_name = 'function_name';
update cabd.feature_type_metadata set vw_all_order = vw_all_order+1 where view_name = 'cabd.dams_medium_large_view' and vw_all_order > 76;
update cabd.feature_type_metadata set vw_all_order = 77 where view_name = 'cabd.dams_medium_large_view' and field_name = 'provincial_flow_req';
