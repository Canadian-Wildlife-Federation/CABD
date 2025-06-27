-- this file contains some updates required to map the community data to the 
-- holding table for stream crossings.


-- I noticed some photos were not being parsed and it looks like
-- it is a result of this not being setup correctly in the metadata, so
-- this updates the metadata
--TODO: is this modelled_crossings or stream crossings?
--this is not tested
update cabd.feature_types 
set community_data_photo_fields = array[
	'transportation_route_image',
	'structure_outlet_image',
	'structure_inlet_image',
	'upstream_blockage_image',
	'downstream_blockage_image',
	'site_image',
	'downstream_direction_image',
	'downstream_side_image',
	'upstream_direction_image',
	'upstream_side_image',
	'fishway_image'
]
where type = 'modelled_crossings';

-- reset the status types for this table
alter table stream_crossings.stream_crossings_community_staging drop constraint status_value_ch;
alter table stream_crossings.stream_crossings_community_staging add constraint status_value_ch CHECK ( ((status)::text = ANY (ARRAY[('NEW'::character varying)::text, ('PROCESSED'::character varying)::text])));



-- mobile data attribute mapping
-- this maps all the values, but for a few of the cabd attributes
-- the mapping table is not used, because of the mapping rules are complex
-- see the trigger for details
drop table if exists cabd.community_attribute_mapping;
create table cabd.community_attribute_mapping(
	attribute_name varchar,
	attribute_value varchar,
	code_value integer,
	primary key (attribute_name, attribute_value)
);

insert into cabd.community_attribute_mapping (attribute_name, attribute_value, code_value) values
('feature_type', 'stream_crossings', 1),
('feature_type', 'modelled_crossings', 1),
('feature_type', 'dams', 2),
('to_feature_type', 'stream_crossings', 1),
('to_feature_type', 'modelled_crossings', 1),
('to_feature_type', 'dams', 2),
('to_feature_type', 'no_structure', 3),
('to_feature_type', 'no_access', 4),
('site_accessible', 'yes', 1),
('site_accessible', 'no', 2),
('no_access_reason', 'safety concerns', 1),
('no_access_reason', 'fencing', 2),
('no_access_reason', 'private property', 3),
('no_access_reason', 'difficult yerrain', 4),
('no_access_reason', 'impassable road', 5),
('no_access_reason', 'other', 6),
('inaccessible_reason', 'safety', 1),
('inaccessible_reason', 'fencing', 2),
('inaccessible_reason', 'privateproperty', 3),
('inaccessible_reason', 'difficultterrain', 4),
('inaccessible_reason', 'impassableroad', 5),
('inaccessible_reason', 'other', 6),
('structure_type', 'open-bottom', 1),
('structure_type', 'closed-bottom', 2),
('structure_type', 'ford-like', 4),
('selected_type', 'obs', 1),
('selected_type', 'cbs', 2),
('selected_type', 'ford', 4),
-- our mapping sheet had a typo
-- this should be transport_type, not transport_type_code
('transport_type', 'multilane', 1),
('transport_type', 'multi-lane (>2 lanes)', 1),
('transport_type', '1-2-lane', 2),
('transport_type', '1 and 2 lane paved', 2),
('transport_type', 'unpaved', 3),
('transport_type', 'driveway', 4),
('transport_type', 'trail', 5),
('transport_type', 'railroad', 6),
('obs', 'no - the structure is wider than the stream', 1),
('obs', 'structure_wider', 1),
('obs', 'no - the structure is the same width as the stream', 2),
('obs', 'structure same', 2),
('obs', 'yes', 3),
('obs', 'structure narrows stream', 3),
('obs', 'unsure', 4),
('water_flowing_upstream', 'no - dry', 1),
('water_flowing_upstream', 'no_dry', 1),
('water_flowing_upstream', 'yes - standing', 2),
('water_flowing_upstream', 'yes_standing', 2),
('water_flowing_upstream', 'yes - moving', 3),
('water_flowing_upstream', 'yes_moving', 3),
('upstream_blockages_debris ', 'true', 2),
('upstream_blockages_deformation', 'true', 3),
('upstream_blockages_human', 'true', 5),
('upstream_blockages_natural', 'true', 6),
('upstream_blockages_fencing', 'true', 7),
('upstream_blockages_dam', 'true', 9),
('upstream_blockages_other ', 'true', 10),
('upstream_blockage_height', '> 30cm', 1), 
('upstream_blockage_height', '>30', 1),
('upstream_blockage_height', '< 30cm', 2),
('upstream_blockage_height', '<30', 2),
('downstream_blockages_debris', 'true', 2),
('downstream_blockages_deformation', 'true', 3),
('downstream_blockages_human ', 'true', 5), 
('downstream_blockages_natural ', 'true', 6),
('downstream_blockages_fencing ', 'true', 7),
('downstream_blockages_dam', 'true', 9),
('downstream_blockages_other', 'true', 10),
('downstream_blockage_height', '> 30cm', 1),
('downstream_blockage_height', '>30', 1),
('downstream_blockage_height', '< 30cm', 2),
('downstream_blockage_height', '<30', 2),
('under_flow_type', 'no - dry', 1),
--('water_flowing_through', 'no_dry', 1),
('under_flow_type', 'yes - standing', 2),
--('water_flowing_through', 'yes_standing', 2),
('under_flow_type', 'yes - moving', 3),
--('water_flowing_through', 'yes_moving', 3),
('outlet_drop', '> 30cm', 1),
('outlet_drop', '>30', 1),
('outlet_drop', '< 30cm', 2),
('outlet_drop', '<30', 2),
('multiple_closed_bottom', 'yes', 1),
('multiple_closed_bottom', 'no', 2),
('multiple_closed_bottom', 'unsure', 3),
('width_comparison', 'larger', 1),
('width_comparison', 'structure larger', 1),
('width_comparison', 'equal', 2),
('width_comparison', 'structure equal', 2),
('width_comparison', 'smaller - less than half', 3),
('width_comparison', 'smaller structure less than half', 3),
('width_comparison', 'smaller - greater than half', 4),
('width_comparison', 'smaller structure greater than half', 4),
('width_comparison', 'unsure', 5),
('water_flowing_through', 'no_dry', 1),
('water_flowing_through', 'yes_standing', 2),
('water_flowing_through', 'yes_moving', 3),
('water_flowing_inside', 'no - dry', 1),
('water_flowing_inside', 'yes - standing', 2),
('water_flowing_inside', 'yes - moving', 3),
('ford_type', 'stream-bed', 1),
('ford_type', 'natural stream bed', 1),
('ford_type', 'engineered', 2),
('ford_type', 'engineered ford', 2),
('ford_type', 'unsure', 3),
('water_flowing_over', 'no - dry', 1),
('water_flowing_over', 'no_dry', 1),
('water_flowing_over', 'yes - standing', 2),
('water_flowing_over', 'yes_standing', 2),
('water_flowing_over', 'yes - moving', 3),
('water_flowing_over', 'yes_moving', 3),
('structure_signs', 'yes', 1),
('structure_signs', 'no', 2),
('structure_signs', 'unsure', 3),
('structure_existed', 'yes', 1),
('structure_existed', 'no', 2),
('structure_existed', 'unsure', 3),
('stream_at_site', 'true', 1),
('stream_at_site', 'false', 0),
('water_at_site', 'true', 1),
('water_at_site', 'false', 0),
('water_existed', 'yes', 1),
('water_existed', 'no', 2),
('water_existed', 'unsure', 3),
('trail_end', 'yes', 1),
('trail_end', 'no', 2),
('trail_end', 'unsure', 3),
('access_method', 'adjacent road/trail', 1),
('access_method', 'road/trail', 1),
('access_method', 'on foot', 2),
('access_method', 'foot', 2),
('access_method', 'boat/canoe', 3),
('access_method', 'wading', 4),
('close_by', 'yes', 1),
('close_by', 'no', 2),
('close_by', 'unsure', 3),
('partial_dam_removal', 'yes', 1),
('partial_dam_removal', 'no', 2),
('partial_dam_removal', 'unsure', 3),
('side_channel_bypass', 'through', 1),
('side_channel_bypass', 'around', 2), 
('side_channel_bypass', 'over', 3),
('side_channel_bypass', 'dry - no water passing', 4),
('dam_height', '<5m', 1),
('dam_height', '5-15m', 2),
('dam_height', '>15m', 3),
('has_fish_structure', 'true', 1),
('has_fish_structure', 'false', 0);

--
-- this function will lookup the code value for 
-- attributes in the json. One or more attributes can be supplied, it will 
-- look for the first one first, if not found it will look for the next one until
-- one is found.  Returns null if no mapping value found
--
CREATE OR REPLACE FUNCTION cabd.lookup_community_attribute(p_data jsonb, VARIADIC p_attribute_names VARCHAR[])
RETURNS INTEGER AS $$
DECLARE
    v_code_value INTEGER;
    v_attribute varchar;
BEGIN
    v_code_value := null;

    FOREACH v_attribute IN ARRAY p_attribute_names LOOP
	
	    SELECT code_value INTO v_code_value
    	FROM cabd.community_attribute_mapping
	    WHERE attribute_name = v_attribute
	      AND attribute_value ilike p_data->>v_attribute;

		if (v_code_value is not null) then
			return v_code_value;
		end if;
	end loop;
	return null;
END;
$$ LANGUAGE plpgsql;

--
--trigger to parse the stream crossing community data into the holding table
--when a "new record" is added to the community data table.
--
CREATE OR REPLACE FUNCTION stream_crossing_community_staging_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN

	if (NEW.status != 'NEW') then
		RETURN NEW;
	end if;

    insert into stream_crossings.stream_crossings_community_holding (
		status,
		id, 
		cabd_id, 
		user_id, 
		uploaded_datetime,
		feature_type_code,
		to_feature_type_code,
		latitude,
		longitude,
		site_accessible_code,
		no_access_reason_code,
		crossing_type_code,
		road_type_code,
		transportation_route_image,
		obs_constriction_code,
		water_flowing_upstream_code,
		structure_outlet_image,
		structure_inlet_image,
		upstream_physical_blockages_code,
		upstream_blockage_image,
		upstream_blockage_height_code,
		downstream_physical_blockages_code,
		downstream_blockage_height_code,
		downstream_blockage_image,
		water_flowing_under_code,
		outlet_drop_code,
		multiple_closed_bottom_code,
		cbs_constriction_code,
		structure_count,
		water_flowing_through_code,
		ford_type_code,
		water_flowing_over_code,
		site_image,
		structure_signs_code,
		stream_at_site_code,
		water_existed_code,
		trail_end_code,
		access_method_code,
		close_by_code,
		dam_name,
		partial_dam_removal_code,
		downstream_direction_image,
		downstream_side_image,
		water_passing_code,
		dam_size_code,
		has_fish_structure,
		fishway_image,
		upstream_direction_image,
		upstream_side_image,
		notes
	)values (
		'NEW',
		NEW.id, 
		NEW.cabd_id, 
		NEW.user_id, 
		NEW.uploaded_datetime,
		cabd.lookup_community_attribute(NEW.data->'properties', 'feature_type'),
		case when cabd.lookup_community_attribute(NEW.data->'properties','to_feature_type') is not null then cabd.lookup_community_attribute(NEW.data->'properties','to_feature_type') when NEW.data->'properties'->>'site_accessible' ilike 'no' then 4 else null end,
		((NEW.data->'geometry'->>'coordinates')::jsonb ->> 1)::double precision,
		((NEW.data->'geometry'->>'coordinates')::jsonb ->> 0)::double precision,
		case when NEW.data->'properties'->>'site_accessible' ilike 'yes' then 1 when NEW.data->'properties'->>'site_accessible' ilike 'no' then 2  when NEW.data->'properties'->>'to_feature_type' ilike 'no_access' then 2 else null end,
		cabd.lookup_community_attribute(NEW.data->'properties','inaccessible_reason', 'no_access_reason'),
		cabd.lookup_community_attribute(NEW.data->'properties','selected_type', 'structure_type'),
		cabd.lookup_community_attribute(NEW.data->'properties','transport_type'),
		NEW.data->'properties'->>'transportation_route_image',
		cabd.lookup_community_attribute(NEW.data->'properties','obs'),
		cabd.lookup_community_attribute(NEW.data->'properties','water_flowing_upstream'),
		NEW.data->'properties'->>'structure_outlet_image',
		NEW.data->'properties'->>'structure_inlet_image',
		
		case when NEW.data->'properties'->>'upstream_physical_blockage' ilike 'false' then array[1] 
			when NEW.data->'properties'->>'upstream_physical_blockage' ilike 'true' then
		      ('{' || substring(
                         case when NEW.data->'properties'->>'upstream_blockages_debris' ilike 'true' then ',2' else '' end || 
                         case when NEW.data->'properties'->>'upstream_blockages_deformation' ilike 'true' then ',3' else '' end || 
                         case when NEW.data->'properties'->>'upstream_blockages_human' ilike 'true' then ',5' else '' end || 
                         case when NEW.data->'properties'->>'upstream_blockages_natural' ilike 'true' then ',6' else '' end || 
                         case when NEW.data->'properties'->>'upstream_blockages_fencing' ilike 'true' then ',7' else '' end || 
                         case when NEW.data->'properties'->>'upstream_blockages_dam' ilike 'true' then ',9' else '' end ||  
                         case when NEW.data->'properties'->>'upstream_blockages_other' ilike 'true' then ',10' else '' end, 2) || '}')::int[] 
		else null end,
		
		NEW.data->'properties'->>'upstream_blockage_image',
		cabd.lookup_community_attribute(NEW.data->'properties','upstream_blockage_height'),
		
		case when NEW.data->'properties'->>'downstream_physical_blockage' ilike 'false' then array[1] 
             when NEW.data->'properties'->>'downstream_physical_blockage' ilike 'true' then 
                ('{' || substring(
                         case when NEW.data->'properties'->>'downstream_blockages_debris' ilike 'true' then ',2' else '' end || 
                         case when NEW.data->'properties'->>'downstream_blockages_deformation' ilike 'true' then ',3' else '' end ||
                         case when NEW.data->'properties'->>'downstream_blockages_human' ilike 'true' then ',5' else '' end ||
                         case when NEW.data->'properties'->>'downstream_blockages_natural' ilike 'true' then ',6' else '' end || 
                         case when NEW.data->'properties'->>'downstream_blockages_fencing' ilike 'true' then ',7' else '' end || 
                         case when NEW.data->'properties'->>'downstream_blockages_dam' ilike 'true' then ',9' else '' end || 
                         case when NEW.data->'properties'->>'downstream_blockages_other' ilike 'true' then ',10' else '' end, 2) || '}')::int[] 
        else null end,
		cabd.lookup_community_attribute(NEW.data->'properties','downstream_blockage_height'),
		NEW.data->'properties'->>'downstream_blockage_image',
		
		case when NEW.data->'properties'->>'selected_type' ilike 'obs' or NEW.data->'properties'->>'structure_type' ilike 'open-bottom' then cabd.lookup_community_attribute(NEW.data->'properties','water_flowing_through', 'under_flow_type') else null end,
		
		case when NEW.data->'properties'->>'selected_type' ilike 'cbs' or NEW.data->'properties'->>'structure_type' ilike 'closed-bottom' then cabd.lookup_community_attribute(NEW.data->'properties','outlet_drop') else null end,
		case when NEW.data->'properties'->>'selected_type' ilike 'cbs' or NEW.data->'properties'->>'structure_type' ilike 'closed-bottom' then cabd.lookup_community_attribute(NEW.data->'properties','multiple_closed_bottom') else null end,
		case when NEW.data->'properties'->>'selected_type' ilike 'cbs' or NEW.data->'properties'->>'structure_type' ilike 'closed-bottom' then cabd.lookup_community_attribute(NEW.data->'properties','width_comparison') else null end,
		case when NEW.data->'properties'->>'selected_type' ilike 'cbs' or NEW.data->'properties'->>'structure_type' ilike 'closed-bottom' then (NEW.data->'properties'->>'structure_count')::integer else null end,
		case when NEW.data->'properties'->>'selected_type' ilike 'cbs' or NEW.data->'properties'->>'structure_type' ilike 'closed-bottom' then cabd.lookup_community_attribute(NEW.data->'properties','water_flowing_through', 'water_flowing_inside') else null end,
		
		case when NEW.data->'properties'->>'selected_type' ilike 'ford' or NEW.data->'properties'->>'structure_type' ilike 'ford-like' then cabd.lookup_community_attribute(NEW.data->'properties','ford_type') else null end,
		case when NEW.data->'properties'->>'selected_type' ilike 'ford' or NEW.data->'properties'->>'structure_type' ilike 'ford-like' then cabd.lookup_community_attribute(NEW.data->'properties','water_flowing_over') else null end,
		NEW.data->'properties'->>'site_image',
		cabd.lookup_community_attribute(NEW.data->'properties','structure_signs', 'structure_existed'),
		cabd.lookup_community_attribute(NEW.data->'properties','stream_at_site', 'water_at_site')::boolean,
		cabd.lookup_community_attribute(NEW.data->'properties','water_existed'),
		cabd.lookup_community_attribute(NEW.data->'properties','trail_end'),
		cabd.lookup_community_attribute(NEW.data->'properties','access_method'),
		cabd.lookup_community_attribute(NEW.data->'properties','close_by'),
		NEW.data->'properties'->>'dam_name',
		cabd.lookup_community_attribute(NEW.data->'properties','partial_dam_removal'),
		NEW.data->'properties'->>'downstream_direction_image',
		NEW.data->'properties'->>'downstream_side_image',
		cabd.lookup_community_attribute(NEW.data->'properties','side_channel_bypass'),
		cabd.lookup_community_attribute(NEW.data->'properties','dam_height'),
		cabd.lookup_community_attribute(NEW.data->'properties','has_fish_structure')::boolean,
		NEW.data->'properties'->>'fishway_image',
		NEW.data->'properties'->>'upstream_direction_image',
		NEW.data->'properties'->>'upstream_side_image',
		NEW.data->'properties'->>'notes'
	);
	UPDATE stream_crossings.stream_crossings_community_staging SET status = 'PROCESSED' where id = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER stream_crossing_community_staging_trigger
AFTER INSERT ON stream_crossings.stream_crossings_community_staging
FOR EACH ROW
EXECUTE FUNCTION stream_crossing_community_staging_insert_trigger();
