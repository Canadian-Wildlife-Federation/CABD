-- drops and creates sites and structures table
-- source: tbl_structure_community_data_holding.xlsx
--
--notes:
-- downstream_direction_image and upstream_direction_image were duplicated
-- upstream_physical_blockages_code & downstream_physical_blockages_code are int arrays, postgresql does not support foreign keys on arrays
drop table if exists stream_crossings.stream_crossings_community_holding;
create table stream_crossings.stream_crossings_community_holding(
    id uuid primary key not null,
    cabd_id uuid not null,
    user_id uuid not null,
    uploaded_datetime timestamptz not null,
    feature_type_code integer references cabd.feature_type_codes(code),
    to_feature_type_code integer references cabd.feature_type_codes(code),
    latitude decimal,
    longitude decimal,
    site_accessible_code integer references cabd.response_codes(code),
    no_access_reason_code integer references cabd.no_access_reason_codes(code),
    crossing_type_code integer references stream_crossings.crossing_type_codes(code),
    --duplicated below
    --downstream_direction_image varchar, 
    road_type_code integer references cabd.road_type_codes(code),
    transportation_route_image varchar,
    obs_constriction_code integer references stream_crossings.obs_constriction_codes(code),
    water_flowing_upstream_code integer references cabd.flowing_codes(code),
    structure_outlet_image varchar,
    --duplicated below
    --upstream_direction_image varchar,
    structure_inlet_image varchar,
    upstream_physical_blockages_code integer[], --references cabd.blockage_type_codes(code),
    upstream_blockage_image varchar,
    upstream_blockage_height_code integer references stream_crossings.outlet_drop_codes(code),
    downstream_physical_blockages_code integer[], --references cabd.blockage_type_codes(code),
    downstream_blockage_height_code integer references stream_crossings.outlet_drop_codes(code),
    downstream_blockage_image varchar,
    water_flowing_under_code integer references cabd.flowing_codes(code),
    outlet_drop_code integer references stream_crossings.outlet_drop_codes(code),
    multiple_closed_bottom_code integer references cabd.response_codes(code),
    cbs_constriction_code integer references stream_crossings.cbs_constriction_codes(code),
    structure_count integer,
    water_flowing_through_code integer references cabd.flowing_codes(code),
    ford_type_code integer references stream_crossings.ford_type_codes(code),
    water_flowing_over_code integer references cabd.response_codes(code),
    site_image varchar,
    structure_signs_code integer references cabd.response_codes(code),
    stream_at_site_code boolean,
    water_existed_code integer references cabd.response_codes(code),
    trail_end_code integer references cabd.response_codes(code),
    access_method_code integer references cabd.access_method_codes(code),
    close_by_code integer references cabd.response_codes(code),
    dam_name varchar,
    partial_dam_removal_code integer references cabd.response_codes(code),
    downstream_direction_image varchar,
    downstream_side_image varchar,
    water_passing_code integer references dams.side_channel_bypass_codes(code),
    dam_size_code integer references dams.size_codes(code),
    has_fish_structure boolean,
    fishway_image varchar,
    upstream_direction_image varchar,
    upstream_side_image varchar,
    notes varchar,
    status integer references cabd.status_codes(code),
    reviewer varchar
);
