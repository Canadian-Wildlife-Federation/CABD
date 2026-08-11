-- DROP FUNCTION stream_crossings.stream_crossing_community_staging_insert_trg();

CREATE OR REPLACE FUNCTION stream_crossings.stream_crossing_community_staging_insert_trg()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN

	if (NEW.status != 'NEW') then
		RETURN NEW;
	end if;

    insert into cabd.community_holding (
        cabd_feature_type,
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
        'stream_crossings',
		'NEW',
		NEW.id, 
		NEW.cabd_id, 
		NEW.user_id, 
		NEW.uploaded_datetime,
		cabd.lookup_community_attribute(NEW.data->'properties', 'feature_type'),
		case 
          when cabd.lookup_community_attribute(NEW.data->'properties','to_feature_type') is not null then cabd.lookup_community_attribute(NEW.data->'properties','to_feature_type') 
          when NEW.data->'properties'->>'site_accessible' ilike 'no' then 4 
          else cabd.lookup_community_attribute(NEW.data->'properties', 'feature_type') end,
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
$function$
;
