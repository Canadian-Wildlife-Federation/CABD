
alter table cabd.feature_type_metadata add column value_options_reference varchar(2048);

update cabd.feature_type_metadata set value_options_reference = 'cabd.province_territory_codes;name;name;' where field_name = 'province_territory';
update cabd.feature_type_metadata set value_options_reference = 'cabd.province_territory_codes;code;name;' where field_name = 'province_territory_code';

update cabd.feature_type_metadata set value_options_reference = 'cabd.passability_status_codes;name;name;description' where field_name = 'passability_status';
update cabd.feature_type_metadata set value_options_reference = 'cabd.passability_status_codes;code;name;description' where field_name = 'passability_status_code';


update cabd.feature_type_metadata set value_options_reference = 'cabd.barrier_ownership_type_codes;name;name;description' where field_name = 'ownership_type';
update cabd.feature_type_metadata set value_options_reference = 'cabd.barrier_ownership_type_codes;code;name;description' where field_name = 'ownership_type_code';

update cabd.feature_type_metadata set value_options_reference = 'cabd.upstream_passage_type_codes;name;name;description' where field_name = 'up_passage_type';
update cabd.feature_type_metadata set value_options_reference = 'cabd.upstream_passage_type_codes;code;name;description' where field_name = 'up_passage_type_code';

update cabd.feature_type_metadata set value_options_reference = 'cabd.nhn_workunit;id;sub_sub_drainage_area;' where field_name = 'nhn_workunit_id';

-- dam specific
update cabd.feature_type_metadata set value_options_reference = 'dams.condition_codes;name;name;description' where field_name = 'dam_condition'and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.condition_codes;code;name;description' where field_name = 'condition_code'and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.construction_type_codes;name;name;description' where field_name = 'construction_type'and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.construction_type_codes;code;name;description' where field_name = 'construction_type_code'and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.dam_complete_level_codes;name;name;description' where field_name = 'complete_level' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.dam_complete_level_codes;code;name;description' where field_name = 'complete_level_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.dam_use_codes;name;name;description' where field_name = 'use_code' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.dam_use_codes;code;name;description' where field_name = 'use_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_irrigation' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_irrigation_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_electricity' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_electricity_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_supply' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_supply_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_floodcontrol' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_floodcontrol_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_recreation' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_recreation_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_navigation' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_navigation_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_fish' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_fish_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_pollution' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_pollution_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_invasivespecies' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_invasivespecies_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;name;name;description' where field_name = 'use_other' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.use_codes;code;name;description' where field_name = 'use_other_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.downstream_passage_route_codes;name;name;description' where field_name = 'down_passage_route' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.downstream_passage_route_codes;code;name;description' where field_name = 'down_passage_route_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.function_codes;name;name;description' where field_name = 'function_name' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.function_codes;code;name;description' where field_name = 'function_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.lake_control_codes;name;name;description' where field_name = 'lake_control' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.lake_control_codes;code;name;description' where field_name = 'lake_control_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.operating_status_codes;name;name;description' where field_name = 'operating_status' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.operating_status_codes;code;name;description' where field_name = 'operating_status_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.size_codes;name;name;description' where field_name = 'size_class' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.size_codes;code;name;description' where field_name = 'size_class_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.spillway_type_codes;name;name;description' where field_name = 'spillway_type' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.spillway_type_codes;code;name;description' where field_name = 'spillway_type_code' and view_name = 'cabd.dams_view';

update cabd.feature_type_metadata set value_options_reference = 'dams.turbine_type_codes;name;name;description' where field_name = 'turbine_type' and view_name = 'cabd.dams_view';
update cabd.feature_type_metadata set value_options_reference = 'dams.turbine_type_codes;code;name;description' where field_name = 'turbine_type_code' and view_name = 'cabd.dams_view';


--fishways
update cabd.feature_type_metadata set value_options_reference = 'fishways.entrance_location_codes;name;name;description' where field_name = 'entrance_location' and view_name = 'cabd.fishways_view';
update cabd.feature_type_metadata set value_options_reference = 'fishways.entrance_location_codes;code;name;description' where field_name = 'entrance_location_code' and view_name = 'cabd.fishways_view';

update cabd.feature_type_metadata set value_options_reference = 'fishways.entrance_position_codes;name;name;description' where field_name = 'entrance_position' and view_name = 'cabd.fishways_view';
update cabd.feature_type_metadata set value_options_reference = 'fishways.entrance_position_codes;code;name;description' where field_name = 'entrance_position_code' and view_name = 'cabd.fishways_view';

update cabd.feature_type_metadata set value_options_reference = 'fishways.fishway_complete_level_codes;name;name;description' where field_name = 'complete_level' and view_name = 'cabd.fishways_view';
update cabd.feature_type_metadata set value_options_reference = 'fishways.fishway_complete_level_codes;code;name;description' where field_name = 'complete_level_code' and view_name = 'cabd.fishways_view';


--waterfalls
update cabd.feature_type_metadata set value_options_reference = 'waterfalls.waterfall_complete_level_codes;name;name;description' where field_name = 'entrance_location' and view_name = 'cabd.waterfalls_view';
update cabd.feature_type_metadata set value_options_reference = 'waterfalls.waterfall_complete_level_codes;code;name;description' where field_name = 'entrance_location_code' and view_name = 'cabd.waterfalls_view';
