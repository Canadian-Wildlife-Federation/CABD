drop trigger stream_crossings_community_holding_data_trg on stream_crossings.stream_crossings_community_holding;
alter table stream_crossings.stream_crossings_community_holding set schema cabd;
alter table cabd.stream_crossings_community_holding rename to community_holding;

alter table cabd.community_holding add column cabd_feature_type varchar references cabd.feature_types(type);

update cabd.community_holding set cabd_feature_type = 'stream_crossings';

alter table cabd.community_holding alter column cabd_feature_type set not null;

alter table cabd.community_holding add column addressed_status_code int2 generated always as 
(case when cabd_feature_type = 'dams' and has_fish_structure then 3 else null end) STORED;

alter table cabd.community_holding add column assessment_type_code int2 generated always as 
(case when cabd_feature_type = 'dams' then 1 else null end) STORED;

alter table cabd.community_holding add column up_passage_type_code int2 generated always as 
(case when cabd_feature_type = 'dams' and not has_fish_structure then 8 else null end) STORED;

alter table cabd.community_holding add column passability_status_code int2; 

CREATE OR REPLACE FUNCTION cabd.calc_community_holding_passability_status_code()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.cabd_feature_type = 'dams' THEN
        NEW.passability_status_code = CASE WHEN NEW.has_fish_structure THEN 2 ELSE 1 END;
    ELSIF NEW.cabd_feature_type = 'stream_crossings' THEN
        NEW.passability_status_code = stream_crossings.community_passability_status(NEW);
    ELSE
        NEW.passability_status_code = NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER community_holding_passability_status_code_trg
BEFORE INSERT OR UPDATE ON cabd.community_holding
FOR EACH ROW
EXECUTE FUNCTION cabd.calc_community_holding_passability_status_code();

insert into cabd.community_attribute_mapping (attribute_name, attribute_value, code_value) values
('has_fish_structure', 'Yes', 1),
('has_fish_structure', 'No', 0);

drop view dams.assessment_rapid_en ;
drop view dams.assessment_rapid_fr ;

drop table dams.dams_community_holding;

drop trigger dams_community_staging_trigger on dams.dams_community_staging;
-- update dams staging -> holding trigger
DROP FUNCTION dams.dams_community_staging_insert_trg();
-- holding -> dams
DROP FUNCTION dams.dams_community_holding_data_trg();

CREATE OR REPLACE FUNCTION dams.dams_community_staging_insert_trg()
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
	    latitude,
	    longitude,
	    dam_name,
        dam_size_code,
        has_fish_structure       
	)values (
        'dams',
		'NEW',
		NEW.id, 
		NEW.cabd_id, 
		NEW.user_id, 
		NEW.uploaded_datetime,
		((NEW.data->'geometry'->>'coordinates')::jsonb ->> 1)::double precision,
		((NEW.data->'geometry'->>'coordinates')::jsonb ->> 0)::double precision,
		NEW.data->'properties'->>'dam_name',
        cabd.lookup_community_attribute(NEW.data->'properties','dam_height'), 
        case when cabd.lookup_community_attribute(NEW.data->'properties','has_fish_structure') = 1 then true else false end
	);
	UPDATE dams.dams_community_staging SET status = 'PROCESSED' where id = NEW.id;
    RETURN NEW;
END;
$function$
;
alter function dams.dams_community_staging_insert_trg owner to cabd;

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
    if (NEW.cabd_feature_type != 'dams') then
        RETURN NEW;
    END If;
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
            update cabd.community_holding set cabd_id = cabdid where id = NEW.id;
        end if;
	end if;
	

    if (cabdid is not null) THEN

        raise notice 'cabd is not null %s', cabdid;

        if (NEW.dam_name is not null) then
            -- only update the fields if the community data field is not null and if the cabd dams field is null or unknown.
            select case when dam_name_en is null then true when dam_name_en = '' then true else false end into doupdate
            from dams.dams where cabd_id = cabdid;
            
            if (doupdate) then
                update dams.dams set dam_name_en = NEW.dam_name where cabd_id = cabdid;
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

        if (NEW.dam_size_code is not null) then
            -- only update the fields if the community data field is not null and if the cabd dams field is null or unknown.
            select case when size_class_code is null then true when size_class_code = 99 then true else false end into doupdate
            from dams.dams where cabd_id = cabdid;
            
            if (doupdate) then
                update dams.dams set size_class_code = NEW.dam_size_code where cabd_id = cabdid;
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

        update cabd.community_holding set status = 'PROCESSED' where id = NEW.id;

    else
        -- Verify the cabd doesn't exists for another feature
        PERFORM 1
        FROM cabd.all_features_view_en
        WHERE cabd_id = NEW.cabd_id
            AND feature_type != 'dams';

        IF FOUND THEN
            RAISE EXCEPTION 'This cabd_id already exists for a different feature type. Need a new cabd id or remove the existing feature';
        END IF;

        --need to insert to dams
        insert into dams.dams(cabd_id, 
            dam_name_en, addressed_status_code, 
		    assessment_type_code, size_class_code, 
            up_passage_type_code, passability_status_code,
            original_point, snapped_point, snapped_ncc, 
		    province_territory_code, nhn_watershed_id
		    )
        values (NEW.cabd_id, 
            NEW.dam_name, NEW.addressed_status_code,
            NEW.assessment_type_code, NEW.dam_size_code, 
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
                case when NEW.dam_name is null then null else 'c' end,
                case when NEW.dam_name is null then null else NEW.id end,
                case when NEW.addressed_status_code is null then null else 'c' end,
                case when NEW.addressed_status_code is null then null else NEW.id end,
                case when NEW.assessment_type_code is null then null else 'c' end,
                case when NEW.assessment_type_code is null then null else NEW.id end,
                case when NEW.dam_size_code is null then null else 'c' end,
                case when NEW.dam_size_code is null then null else NEW.id end,
                case when NEW.up_passage_type_code is null then null else 'c' end,
                case when NEW.up_passage_type_code is null then null else NEW.id end,
                case when NEW.passability_status_code is null then null else 'c' end,
                case when NEW.passability_status_code is null then null else NEW.id end,
                'c', NEW.id --original point                
        );

	    update cabd.community_holding set status = 'PROCESSED' where id = NEW.id;

    END IF;
	RETURN NEW;
END;
$function$
;
alter function dams.dams_community_holding_data_trg owner to cabd;





--update stream crossings staging function
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
$function$
;
alter FUNCTION stream_crossings.stream_crossing_community_staging_insert_trg owner to cabd;


-- DROP FUNCTION stream_crossings.stream_crossings_community_holding_data_trg();

CREATE OR REPLACE FUNCTION stream_crossings.stream_crossings_community_holding_data_trg()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
  cabdid uuid;
  structureid uuid;
  distance double precision;
  doupdate boolean;
  _new_passability_status int;
  newpoint geometry;
  STREAM_SNAP_TOLERANCE integer := 500; --for snapping points to stream network chyf and nhn
  CABD_FEATURE_DISANCE_MATCH_TOLDERANCE integer:= 100; --maximum matching distance in meters

BEGIN

    if (NEW.cabd_feature_type != 'stream_crossings') then
        RETURN NEW;
    END If;
    --sites data to update
    if (NEW.status != 'REVIEWED') then
        return NEW;
    end if;

    if (NEW.feature_type_code is null or NEW.feature_type_code != 1 or NEW.to_feature_type_code is null or NEW.to_feature_type_code not in (1, 3, 4)) then
        --feature_type_code 1 = stream_crossing
        --to_feature_type_code 1 = stream_crossing, 2 = nostructure, 3 = noaccess
        return NEW;
    end if;
    
    -- does a record exists
    select cabd_id into cabdid from stream_crossings.sites where cabd_id = NEW.cabd_id;

    newpoint := st_transform(st_setsrid(st_makepoint(NEW.longitude, NEW.latitude), 4326), 4617);

    if (cabdid is null) then
        -- look for distance match
        --matching "distance tolerance"
	    
		SELECT structs.cabd_id, structs.dist into cabdid, distance
		FROM (select newpoint::geography as point) sat
		CROSS JOIN LATERAL (
		  SELECT b.cabd_id, b.original_point::geography <-> sat.point AS dist
		  FROM stream_crossings.sites b
		  ORDER BY dist
		  LIMIT 1
		) structs;

        if (distance is null or distance > CABD_FEATURE_DISANCE_MATCH_TOLDERANCE) then
            cabdid := null;
            distance := null;
        end if;

        if (cabdid is not null) THEN
            --want to update the cabd_id of this record so we can find all assessments associated with a given site
            update cabd.community_holding set cabd_id = cabdid where id = NEW.id;
        end if;
	end if;
	

    if (cabdid is not null) THEN

        raise notice 'cabd is not null %s', cabdid;

        --record exists we need to update appropriate attributes
        select structure_id into structureid from stream_crossings.structures where site_id = cabdid and primary_structure;
    
        if (NEW.road_type_code is not null) then
            select case 
				when road_type_code_src is null then true
				when (road_type_code_src in ('m', 's') or (road_type_code_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join cabd.community_holding b on b.id = a.road_type_code_dsid
            where a.cabd_id = cabdid;

			
            if (doupdate) then
                update stream_crossings.sites set road_type_code = NEW.road_type_code where cabd_id = cabdid;
                update stream_crossings.sites_attribute_source set road_type_code_src = 'c', road_type_code_dsid = NEW.id where cabd_id = cabdid;
            end if;

        end if;

        if (NEW.crossing_type_code is not null) then
            select case 
				when crossing_type_code_src is null then true
				when (crossing_type_code_src in ('m', 's') or (crossing_type_code_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join cabd.community_holding b on b.id = a.crossing_type_code_dsid
            where a.cabd_id = cabdid; 
			
            if (doupdate) then
                update stream_crossings.sites set crossing_type_code = NEW.crossing_type_code where cabd_id = cabdid;
                update stream_crossings.sites_attribute_source set crossing_type_code_src = 'c', crossing_type_code_dsid = NEW.id where cabd_id = cabdid;
            end if;

        end if;
		
        if (NEW.structure_count is not null) then
            select case 
				when num_structures_src is null then true
				when (num_structures_src in ('m', 's') or (num_structures_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join cabd.community_holding b on b.id = a.num_structures_dsid
            where a.cabd_id = cabdid;
			
            if (doupdate) then
                update stream_crossings.sites set num_structures = NEW.structure_count where cabd_id = cabdid;
                update stream_crossings.sites_attribute_source set num_structures_src = 'c', num_structures_dsid = NEW.id where cabd_id = cabdid;
            end if;

        end if;

        if (NEW.structure_inlet_image is not null) then
            select case 
				when photo_id_inlet_src is null then true
				when (photo_id_inlet_src in ('m', 's') or (photo_id_inlet_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join cabd.community_holding b on b.id = a.photo_id_inlet_dsid
            where a.cabd_id = cabdid;
			
            if (doupdate) then
                update stream_crossings.sites set photo_id_inlet = NEW.structure_inlet_image where cabd_id = cabdid;
                update stream_crossings.sites_attribute_source set photo_id_inlet_src = 'c', photo_id_inlet_dsid = NEW.id where cabd_id = cabdid;
            end if;

        end if;

        if (NEW.structure_outlet_image is not null) then
            select case 
				when photo_id_outlet_src is null then true
				when (photo_id_outlet_src in ('m', 's') or (photo_id_outlet_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join cabd.community_holding b on b.id = a.photo_id_outlet_dsid
            where a.cabd_id = cabdid;
			
            if (doupdate) then
                update stream_crossings.sites set photo_id_outlet = NEW.structure_outlet_image where cabd_id = cabdid;
                update stream_crossings.sites_attribute_source set photo_id_outlet_src = 'c', photo_id_outlet_dsid = NEW.id where cabd_id = cabdid;
            end if;

        end if;

        if (NEW.upstream_direction_image is not null) then
            select case 
				when photo_id_upstream_src is null then true
				when (photo_id_upstream_src in ('m', 's') or (photo_id_upstream_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join cabd.community_holding b on b.id = a.photo_id_upstream_dsid
            where a.cabd_id = cabdid;
			
            if (doupdate) then
                update stream_crossings.sites set photo_id_upstream = NEW.upstream_direction_image where cabd_id = cabdid;
                update stream_crossings.sites_attribute_source set photo_id_upstream_src = 'c', photo_id_upstream_dsid = NEW.id where cabd_id = cabdid;
            end if;

        end if;

        if (NEW.downstream_direction_image is not null) then
            select case 
				when photo_id_downstream_src is null then true
				when (photo_id_downstream_src in ('m', 's') or (photo_id_downstream_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join cabd.community_holding b on b.id = a.photo_id_downstream_dsid
            where a.cabd_id = cabdid;
			
            if (doupdate) then
                update stream_crossings.sites set photo_id_downstream = NEW.downstream_direction_image where cabd_id = cabdid;
                update stream_crossings.sites_attribute_source set photo_id_downstream_src = 'c', photo_id_downstream_dsid = NEW.id where cabd_id = cabdid;
            end if;

        end if;

        if (NEW.notes is not null) then
            select case 
				when crossing_comments_src is null then true
				when (crossing_comments_src in ('m', 's') or (crossing_comments_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join cabd.community_holding b on b.id = a.crossing_comments_dsid
            where a.cabd_id = cabdid;
			
            if (doupdate) then
                update stream_crossings.sites set crossing_comments = NEW.notes where cabd_id = cabdid;
                update stream_crossings.sites_attribute_source set crossing_comments_src = 'c', crossing_comments_dsid = NEW.id where cabd_id = cabdid;
            end if;

        end if;

        --geomery; expect community data to always have geometries
        select case 
			when original_point_src is null then true
			when (original_point_src in ('m', 's') or (original_point_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
			else false end  into doupdate
		from stream_crossings.sites_attribute_source a 
			left join cabd.community_holding b on b.id = a.original_point_dsid
        where a.cabd_id = cabdid;
			
        if (doupdate) then
            update stream_crossings.sites set 
                original_point = newpoint,
                province_territory_code = cabd.find_province_territory_code(newpoint),
                nhn_watershed_id = cabd.find_nhn_watershed_id(newpoint),
                snapped_point = cabd.snap_point_to_chyf_network(newpoint, STREAM_SNAP_TOLERANCE),
				snapped_ncc = cabd.snap_point_to_nhn_network(newpoint, STREAM_SNAP_TOLERANCE)
            where cabd_id = cabdid;

            -- update chu fields
            with matches as (	
                select a.cabd_id, b.id, b.chu_12_id, b.chu_10_id, b.chu_8_id, b.chu_6_id, b.chu_4_id, b.chu_2_id
                from stream_crossings.sites a, cabd.chu b
                where st_intersects(st_transform(a.original_point, 3979), b.geom) and a.cabd_id = cabdid
            )
            update stream_crossings.sites set 
                chu_12_id = a.chu_12_id,
                chu_10_id = a.chu_10_id,
                chu_8_id = a.chu_8_id,
                chu_6_id = a.chu_6_id,
                chu_4_id = a.chu_4_id,
                chu_2_id = a.chu_2_id
            from matches a where a.cabd_id = sites.cabd_id;

            update stream_crossings.sites_attribute_source set original_point_src = 'c', original_point_dsid = NEW.id where cabd_id = cabdid;
        end if;

        --structures
        if (NEW.upstream_physical_blockages_code is not null or NEW.downstream_physical_blockages_code is not null) then
            select case 
				when physical_blockages_code_src is null then true
				when (physical_blockages_code_src in ('m', 's') or (physical_blockages_code_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.structures_attribute_source a 
				left join cabd.community_holding b on b.id = a.physical_blockages_code_dsid                
            where a.structure_id in (select structure_id from stream_crossings.structures where site_id = cabdid);
			
            if (doupdate) then
                update stream_crossings.structures set physical_blockages_code = case when NEW.upstream_physical_blockages_code is null and NEW.downstream_physical_blockages_code is null then null when NEW.upstream_physical_blockages_code is null and NEW.downstream_physical_blockages_code is not null then NEW.downstream_physical_blockages_code when NEW.upstream_physical_blockages_code is not null and NEW.downstream_physical_blockages_code is null then NEW.upstream_physical_blockages_code else ARRAY(SELECT DISTINCT UNNEST(NEW.upstream_physical_blockages_code || NEW.downstream_physical_blockages_code)) end 
                where site_id = cabdid;
                update stream_crossings.structures_attribute_source 
                    set physical_blockages_code_src = 'c', physical_blockages_code_dsid = NEW.id 
                    where structure_id in (select structure_id from stream_crossings.structures where site_id = cabdid);
            end if;

        end if;
	    
        -- update passability status from community data
        -- only if computed value is not "not passible" and the existing
        -- value is coming from modelled crossings, satelitte review, or community data and this is newer than previous community update
        _new_passability_status := stream_crossings.community_passability_status(NEW);

        if (_new_passability_status != 4) then
            select case 
				when passability_status_code_src is null then true
				when (passability_status_code_src in ('m', 's') or (passability_status_code_src = 'c' and b.uploaded_datetime < NEW.uploaded_datetime)) then true 
				else false end  into doupdate
			from stream_crossings.structures_attribute_source a 
				left join cabd.community_holding b on b.id = a.passability_status_code_dsid
            where a.structure_id in (select structure_id from stream_crossings.structures where site_id = cabdid);
            if (doupdate) then
                update stream_crossings.structures set passability_status_code = _new_passability_status  
                where site_id = cabdid;
                update stream_crossings.structures_attribute_source 
                    set passability_status_code_src = 'c', passability_status_code_dsid = NEW.id 
                    where structure_id in (select structure_id from stream_crossings.structures where site_id = cabdid);
            end if;
        end if;

        update cabd.community_holding set status = 'PROCESSED' where id = NEW.id;

    else
        -- Verify the cabd doesn't exists for another feature
        PERFORM 1
        FROM cabd.all_features_view_en
        WHERE cabd_id = NEW.cabd_id
            AND feature_type != 'sites';

        IF FOUND THEN
            RAISE EXCEPTION 'This cabd_id already exists for a different feature type. Need a new cabd id or remove the existing feature';
        END IF;

        --need to insert to sites
        insert into stream_crossings.sites(cabd_id, 
            road_type_code, crossing_type_code, 
		    num_structures, photo_id_inlet, photo_id_outlet, photo_id_upstream,
		    photo_id_downstream, crossing_comments, original_point, 
		    snapped_point,
		    snapped_ncc, 
		    province_territory_code, nhn_watershed_id,
		    site_type_code, assessment_type_code, addressed_status_code)
        values (NEW.cabd_id, 
            NEW.road_type_code, NEW.crossing_type_code,
            NEW.structure_count, NEW.structure_inlet_image, NEW.structure_outlet_image, NEW.upstream_direction_image,
            NEW.downstream_direction_image, NEW.notes, newpoint,
            cabd.snap_point_to_chyf_network( newpoint, STREAM_SNAP_TOLERANCE),
		    cabd.snap_point_to_nhn_network( newpoint, STREAM_SNAP_TOLERANCE),
            cabd.find_province_territory_code(newpoint),
            cabd.find_nhn_watershed_id(newpoint),            		
		    99, 1, 99);
	

        --set attribute source
        insert into stream_crossings.sites_attribute_source(cabd_id, 
            road_type_code_src, road_type_code_dsid, 
            crossing_type_code_src, crossing_type_code_dsid, 
            num_structures_src, num_structures_dsid, 
            photo_id_inlet_src, photo_id_inlet_dsid,
            photo_id_outlet_src, photo_id_outlet_dsid, 
            photo_id_upstream_src, photo_id_upstream_dsid,
            photo_id_downstream_src, photo_id_downstream_dsid,
            crossing_comments_src, crossing_comments_dsid,
            original_point_src, original_point_dsid,		
            site_type_code_src, site_type_code_dsid,
            assessment_type_code_src, assessment_type_code_dsid,
            addressed_status_code_src, addressed_status_code_dsid
        )VALUES(
            NEW.cabd_id, 
                case when NEW.road_type_code is null then null else 'c' end,
                case when NEW.road_type_code is null then null else NEW.id end,
                case when NEW.crossing_type_code is null then null else 'c' end,
                case when NEW.crossing_type_code is null then null else NEW.id end,
                case when NEW.structure_count is null then null else 'c' end,
                case when NEW.structure_count is null then null else NEW.id end,
                case when NEW.structure_inlet_image is null then null else 'c' end,
                case when NEW.structure_inlet_image is null then null else NEW.id end,
                case when NEW.structure_outlet_image is null then null else 'c' end,
                case when NEW.structure_outlet_image is null then null else NEW.id end,
                case when NEW.upstream_direction_image is null then null else 'c' end,
                case when NEW.upstream_direction_image is null then null else NEW.id end,
                case when NEW.downstream_direction_image is null then null else 'c' end,
                case when NEW.downstream_direction_image is null then null else NEW.id end,
                case when NEW.notes is null then null else 'c' end,
                case when NEW.notes is null then null else NEW.id end,
                'c', NEW.id, --original point
                'c', NEW.id, --site_type_code
                'c', NEW.id, --assessment_type_code
                'c', NEW.id --addressed status code
        );

        -- insert into structures
        _new_passability_status := stream_crossings.community_passability_status(NEW);

        structureid := gen_random_uuid();
        insert into stream_crossings.structures(structure_id, site_id, physical_blockages_code,	primary_structure, structure_number, passability_status_code)
	    values (structureid, NEW.cabd_id,  
            case when NEW.upstream_physical_blockages_code is null and NEW.downstream_physical_blockages_code is null then null when NEW.upstream_physical_blockages_code is null and NEW.downstream_physical_blockages_code is not null then NEW.downstream_physical_blockages_code when NEW.upstream_physical_blockages_code is not null and NEW.downstream_physical_blockages_code is null then NEW.upstream_physical_blockages_code else ARRAY(SELECT DISTINCT UNNEST(NEW.upstream_physical_blockages_code || NEW.downstream_physical_blockages_code)) end,
            true, 1, stream_crossings.community_passability_status(NEW));
        
	    insert into stream_crossings.structures_attribute_source(structure_id, 
		    physical_blockages_code_src, physical_blockages_code_dsid, 
		    primary_structure_src, primary_structure_dsid, 
		    structure_number_src, structure_number_dsid, 
		    passability_status_code_src, passability_status_code_dsid
        )values(
	        structureid, 
			case when NEW.upstream_physical_blockages_code is not null or NEW.downstream_physical_blockages_code is not null then 'c' else null end,
            case when NEW.upstream_physical_blockages_code is not null or NEW.downstream_physical_blockages_code is not null then NEW.id else null end,
			'c', NEW.id, --primary_structure
			'c', NEW.id, --structure_number
			'c', NEW.id --passability_status_code
        );

        -- populate chu fields
        with matches as (	
            select a.cabd_id, b.id, b.chu_12_id, b.chu_10_id, b.chu_8_id, b.chu_6_id, b.chu_4_id, b.chu_2_id
            from stream_crossings.sites a, cabd.chu b
            where st_intersects(st_transform(a.original_point, 3979), b.geom) and a.cabd_id = NEW.cabd_id
        )
        update stream_crossings.sites set 
            chu_12_id = a.chu_12_id,
            chu_10_id = a.chu_10_id,
            chu_8_id = a.chu_8_id,
            chu_6_id = a.chu_6_id,
            chu_4_id = a.chu_4_id,
            chu_2_id = a.chu_2_id
        from matches a where a.cabd_id = sites.cabd_id;

	    update cabd.community_holding set status = 'PROCESSED' where id = NEW.id;

    END IF;
	RETURN NEW;
END;
$function$
;
alter FUNCTION stream_crossings.stream_crossings_community_holding_data_trg() owner to cabd;




create trigger dams_community_staging_trg after
insert
    on
    dams.dams_community_staging for each row execute function dams.dams_community_staging_insert_trg();

create trigger dams_community_holding_data_trg after
insert
    or
update
    on
    cabd.community_holding for each row
    when ((new.cabd_feature_type = 'dams') AND (new.status = 'REVIEWED'::cabd.community_holding_status_type)) execute function dams.dams_community_holding_data_trg();

drop trigger stream_crossing_community_staging_trigger on stream_crossings.stream_crossings_community_staging;
create trigger stream_crossings_community_staging_trg after
insert
    on
    stream_crossings.stream_crossings_community_staging for each row execute function stream_crossings.stream_crossing_community_staging_insert_trg();

create trigger stream_crossings_community_holding_data_trg after
insert
    or
update
    on
    cabd.community_holding for each row
    when ((new.cabd_feature_type = 'stream_crossings') AND (new.status = 'REVIEWED'::cabd.community_holding_status_type)) execute function stream_crossings.stream_crossings_community_holding_data_trg();

CREATE OR REPLACE VIEW dams.assessment_rapid_en
AS SELECT a.id AS assessment_id,
    'rapid_dam'::text AS cabd_assessment_type,
    a.cabd_id,
    a.uploaded_datetime AS assessment_date,
    a.latitude,
    a.longitude,
    a.dam_name as dam_name_en,    
    a.addressed_status_code,
    adrc.name_en AS addressed_status,
    a.assessment_type_code,
    astc.name_en AS assessment_type,
    a.dam_size_code,
    dsc.name_en AS size_class,
    a.up_passage_type_code,
    pc.name_en as up_passage_type,
    a.passability_status_code,
    psc.name_en as passability_status
   FROM cabd.community_holding a
     LEFT JOIN cabd.addressed_status_codes adrc ON adrc.code = a.addressed_status_code
     LEFT JOIN cabd.assessment_type_codes astc ON astc.code = a.assessment_type_code
     LEFT JOIN dams.size_codes dsc ON dsc.code = a.dam_size_code
     left join cabd.upstream_passage_type_codes pc on pc.code = a.up_passage_type_code
     left join cabd.passability_status_codes psc on psc.code = a.passability_status_code
  WHERE a.cabd_feature_type = 'dams' and a.status = 'PROCESSED'::cabd.community_holding_status_type;



CREATE OR REPLACE VIEW dams.assessment_rapid_fr
AS SELECT a.id AS assessment_id,
    'rapid_dam'::text AS cabd_assessment_type,
    a.cabd_id,
    a.uploaded_datetime AS assessment_date,
    a.latitude,
    a.longitude,
    a.dam_name as dam_name_en,    
    a.addressed_status_code,
    adrc.name_fr AS addressed_status,
    a.assessment_type_code,
    astc.name_fr AS assessment_type,
    a.dam_size_code,
    dsc.name_fr AS size_class,
    a.up_passage_type_code,
    pc.name_fr as up_passage_type,
    a.passability_status_code,
    psc.name_en as passability_status
   FROM cabd.community_holding a
     LEFT JOIN cabd.addressed_status_codes adrc ON adrc.code = a.addressed_status_code
     LEFT JOIN cabd.assessment_type_codes astc ON astc.code = a.assessment_type_code
     LEFT JOIN dams.size_codes dsc ON dsc.code = a.dam_size_code
     left join cabd.upstream_passage_type_codes pc on pc.code = a.up_passage_type_code
     left join cabd.passability_status_codes psc on psc.code = a.passability_status_code
  WHERE a.cabd_feature_type = 'dams' and a.status = 'PROCESSED'::cabd.community_holding_status_type;

alter view dams.assessment_rapid_en owner to cabd;
alter view dams.assessment_rapid_fr owner to cabd;

-- stream_crossings.assessment_rapid_en source



--name cleanup 





-- stream_crossings.assessment_rapid_en source

CREATE OR REPLACE VIEW stream_crossings.assessment_rapid_en
AS SELECT a.id AS assessment_id,
    'rapid'::text AS assessment_type,
    a.cabd_id,
    a.uploaded_datetime AS assessment_date,
    a.feature_type_code,
    ftc.name_en AS feature_type,
    a.to_feature_type_code,
    ftc2.name_en AS to_feature_type,
    a.latitude,
    a.longitude,
    a.site_accessible_code,
    sa.name_en AS site_accessible,
    a.no_access_reason_code,
    nar.name_en AS no_access_reason,
    a.crossing_type_code,
    ct.name_en AS crossing_type,
    a.road_type_code,
    rt.name_en AS road_type,
    a.transportation_route_image,
    a.obs_constriction_code,
    oc.name_en AS obs_constriction,
    a.water_flowing_upstream_code,
    wfup.name_en AS water_flowing_upstream,
    a.structure_outlet_image,
    a.structure_inlet_image,
    a.upstream_physical_blockages_code,
    uppb.name_en AS upstream_physical_blockages,
    a.upstream_blockage_image,
    a.upstream_blockage_height_code,
    ubh.name_en AS upstream_blockage_height,
    a.downstream_physical_blockages_code,
    downph.name_en AS downstream_physical_blockages,
    a.downstream_blockage_height_code,
    dbh.name_en AS downstream_blockage_height,
    a.downstream_blockage_image,
    a.water_flowing_under_code,
    wfu.name_en AS water_flowing_under,
    a.outlet_drop_code,
    od.name_en AS outlet_drop,
    a.multiple_closed_bottom_code,
    mcb.name_en AS multiple_closed_bottom,
    a.cbs_constriction_code,
    cc.name_en AS cbs_constriction,
    a.structure_count,
    a.water_flowing_through_code,
    wft.name_en AS water_flowing_through,
    a.ford_type_code,
    ft.name_en AS ford_type,
    a.water_flowing_over_code,
    wfo.name_en AS water_flowing_over,
    a.site_image,
    a.structure_signs_code,
    ss.name_en AS structure_signs,
    a.stream_at_site_code,
    a.water_existed_code,
    we.name_en AS water_existed,
    a.trail_end_code,
    te.name_en AS trail_end,
    a.access_method_code,
    am.name_en AS access_method,
    a.close_by_code,
    cb.name_en AS close_by,
    a.dam_name,
    a.partial_dam_removal_code,
    pdr.name_en AS partial_dam_removal,
    a.downstream_direction_image,
    a.downstream_side_image,
    a.water_passing_code,
    wp.name_en AS water_passing,
    a.dam_size_code,
    ds.name_en AS dam_size,
    a.has_fish_structure,
    a.fishway_image,
    a.upstream_direction_image,
    a.upstream_side_image,
    a.notes
   FROM cabd.community_holding a
     LEFT JOIN cabd.feature_type_codes ftc ON ftc.code = a.feature_type_code
     LEFT JOIN cabd.feature_type_codes ftc2 ON ftc2.code = a.to_feature_type_code
     LEFT JOIN cabd.response_codes sa ON sa.code = a.site_accessible_code
     LEFT JOIN cabd.no_access_reason_codes nar ON nar.code = a.no_access_reason_code
     LEFT JOIN stream_crossings.outlet_drop_codes dbh ON dbh.code = a.downstream_blockage_height_code
     LEFT JOIN stream_crossings.outlet_drop_codes ubh ON ubh.code = a.upstream_blockage_height_code
     LEFT JOIN cabd.response_codes mcb ON mcb.code = a.multiple_closed_bottom_code
     LEFT JOIN cabd.flowing_codes wfup ON wfup.code = a.water_flowing_upstream_code
     LEFT JOIN cabd.flowing_codes wft ON wft.code = a.water_flowing_through_code
     LEFT JOIN cabd.response_codes pdr ON pdr.code = a.partial_dam_removal_code
     LEFT JOIN cabd.flowing_codes wfu ON wfu.code = a.water_flowing_under_code
     LEFT JOIN cabd.access_method_codes am ON am.code = a.access_method_code
     LEFT JOIN stream_crossings.cbs_constriction_codes cc ON cc.code = a.cbs_constriction_code
     LEFT JOIN cabd.response_codes cb ON cb.code = a.close_by_code
     LEFT JOIN stream_crossings.crossing_type_codes ct ON ct.code = a.crossing_type_code
     LEFT JOIN dams.size_codes ds ON ds.code = a.dam_size_code
     LEFT JOIN stream_crossings.ford_type_codes ft ON ft.code = a.ford_type_code
     LEFT JOIN stream_crossings.obs_constriction_codes oc ON oc.code = a.obs_constriction_code
     LEFT JOIN stream_crossings.outlet_drop_codes od ON od.code = a.outlet_drop_code
     LEFT JOIN cabd.road_type_codes rt ON rt.code = a.road_type_code
     LEFT JOIN cabd.response_codes ss ON ss.code = a.structure_signs_code
     LEFT JOIN cabd.response_codes te ON te.code = a.trail_end_code
     LEFT JOIN cabd.response_codes we ON we.code = a.water_existed_code
     LEFT JOIN cabd.response_codes wfo ON wfo.code = a.water_flowing_over_code
     LEFT JOIN dams.side_channel_bypass_codes wp ON wp.code = a.water_passing_code
     LEFT JOIN ( SELECT community_holding.id,
            array_agg(cc_1.name_en) AS name_en
           FROM cabd.community_holding
             JOIN LATERAL unnest(community_holding.upstream_physical_blockages_code) code_ids(code_ids) ON true
             LEFT JOIN cabd.blockage_type_codes cc_1 ON cc_1.code = code_ids.code_ids
          GROUP BY community_holding.id) uppb ON uppb.id = a.id
     LEFT JOIN ( SELECT community_holding.id,
            array_agg(cc_1.name_en) AS name_en
           FROM cabd.community_holding
             JOIN LATERAL unnest(community_holding.downstream_physical_blockages_code) code_ids(code_ids) ON true
             LEFT JOIN cabd.blockage_type_codes cc_1 ON cc_1.code = code_ids.code_ids
          GROUP BY community_holding.id) downph ON downph.id = a.id
  WHERE a.cabd_feature_type = 'stream_crossings' and a.status = 'PROCESSED'::cabd.community_holding_status_type;

  -- stream_crossings.assessment_rapid_fr source

CREATE OR REPLACE VIEW stream_crossings.assessment_rapid_fr
AS SELECT a.id AS assessment_id,
    'rapid'::text AS assessment_type,
    a.cabd_id,
    a.uploaded_datetime AS assessment_date,
    a.feature_type_code,
    ftc.name_fr AS feature_type,
    a.to_feature_type_code,
    ftc2.name_fr AS to_feature_type,
    a.latitude,
    a.longitude,
    a.site_accessible_code,
    sa.name_fr AS site_accessible,
    a.no_access_reason_code,
    nar.name_fr AS no_access_reason,
    a.crossing_type_code,
    ct.name_fr AS crossing_type,
    a.road_type_code,
    rt.name_fr AS road_type,
    a.transportation_route_image,
    a.obs_constriction_code,
    oc.name_fr AS obs_constriction,
    a.water_flowing_upstream_code,
    wfup.name_fr AS water_flowing_upstream,
    a.structure_outlet_image,
    a.structure_inlet_image,
    a.upstream_physical_blockages_code,
    uppb.name_fr AS upstream_physical_blockages,
    a.upstream_blockage_image,
    a.upstream_blockage_height_code,
    ubh.name_fr AS upstream_blockage_height,
    a.downstream_physical_blockages_code,
    downph.name_fr AS downstream_physical_blockages,
    a.downstream_blockage_height_code,
    dbh.name_fr AS downstream_blockage_height,
    a.downstream_blockage_image,
    a.water_flowing_under_code,
    wfu.name_fr AS water_flowing_under,
    a.outlet_drop_code,
    od.name_fr AS outlet_drop,
    a.multiple_closed_bottom_code,
    mcb.name_fr AS multiple_closed_bottom,
    a.cbs_constriction_code,
    cc.name_fr AS cbs_constriction,
    a.structure_count,
    a.water_flowing_through_code,
    wft.name_fr AS water_flowing_through,
    a.ford_type_code,
    ft.name_fr AS ford_type,
    a.water_flowing_over_code,
    wfo.name_fr AS water_flowing_over,
    a.site_image,
    a.structure_signs_code,
    ss.name_fr AS structure_signs,
    a.stream_at_site_code,
    a.water_existed_code,
    we.name_fr AS water_existed,
    a.trail_end_code,
    te.name_fr AS trail_end,
    a.access_method_code,
    am.name_fr AS access_method,
    a.close_by_code,
    cb.name_fr AS close_by,
    a.dam_name,
    a.partial_dam_removal_code,
    pdr.name_fr AS partial_dam_removal,
    a.downstream_direction_image,
    a.downstream_side_image,
    a.water_passing_code,
    wp.name_fr AS water_passing,
    a.dam_size_code,
    ds.name_fr AS dam_size,
    a.has_fish_structure,
    a.fishway_image,
    a.upstream_direction_image,
    a.upstream_side_image,
    a.notes
   FROM cabd.community_holding a
     LEFT JOIN cabd.feature_type_codes ftc ON ftc.code = a.feature_type_code
     LEFT JOIN cabd.feature_type_codes ftc2 ON ftc2.code = a.to_feature_type_code
     LEFT JOIN cabd.response_codes sa ON sa.code = a.site_accessible_code
     LEFT JOIN cabd.no_access_reason_codes nar ON nar.code = a.no_access_reason_code
     LEFT JOIN stream_crossings.outlet_drop_codes dbh ON dbh.code = a.downstream_blockage_height_code
     LEFT JOIN stream_crossings.outlet_drop_codes ubh ON ubh.code = a.upstream_blockage_height_code
     LEFT JOIN cabd.response_codes mcb ON mcb.code = a.multiple_closed_bottom_code
     LEFT JOIN cabd.flowing_codes wfup ON wfup.code = a.water_flowing_upstream_code
     LEFT JOIN cabd.flowing_codes wft ON wft.code = a.water_flowing_through_code
     LEFT JOIN cabd.response_codes pdr ON pdr.code = a.partial_dam_removal_code
     LEFT JOIN cabd.flowing_codes wfu ON wfu.code = a.water_flowing_under_code
     LEFT JOIN cabd.access_method_codes am ON am.code = a.access_method_code
     LEFT JOIN stream_crossings.cbs_constriction_codes cc ON cc.code = a.cbs_constriction_code
     LEFT JOIN cabd.response_codes cb ON cb.code = a.close_by_code
     LEFT JOIN stream_crossings.crossing_type_codes ct ON ct.code = a.crossing_type_code
     LEFT JOIN dams.size_codes ds ON ds.code = a.dam_size_code
     LEFT JOIN stream_crossings.ford_type_codes ft ON ft.code = a.ford_type_code
     LEFT JOIN stream_crossings.obs_constriction_codes oc ON oc.code = a.obs_constriction_code
     LEFT JOIN stream_crossings.outlet_drop_codes od ON od.code = a.outlet_drop_code
     LEFT JOIN cabd.road_type_codes rt ON rt.code = a.road_type_code
     LEFT JOIN cabd.response_codes ss ON ss.code = a.structure_signs_code
     LEFT JOIN cabd.response_codes te ON te.code = a.trail_end_code
     LEFT JOIN cabd.response_codes we ON we.code = a.water_existed_code
     LEFT JOIN cabd.response_codes wfo ON wfo.code = a.water_flowing_over_code
     LEFT JOIN dams.side_channel_bypass_codes wp ON wp.code = a.water_passing_code
     LEFT JOIN ( SELECT community_holding.id,
            array_agg(cc_1.name_fr) AS name_fr
           FROM cabd.community_holding
             JOIN LATERAL unnest(community_holding.upstream_physical_blockages_code) code_ids(code_ids) ON true
             LEFT JOIN cabd.blockage_type_codes cc_1 ON cc_1.code = code_ids.code_ids
          GROUP BY community_holding.id) uppb ON uppb.id = a.id
     LEFT JOIN ( SELECT community_holding.id,
            array_agg(cc_1.name_fr) AS name_fr
           FROM cabd.community_holding
             JOIN LATERAL unnest(community_holding.downstream_physical_blockages_code) code_ids(code_ids) ON true
             LEFT JOIN cabd.blockage_type_codes cc_1 ON cc_1.code = code_ids.code_ids
          GROUP BY community_holding.id) downph ON downph.id = a.id
  WHERE a.cabd_feature_type = 'stream_crossings' and a.status = 'PROCESSED'::cabd.community_holding_status_type;

alter view stream_crossings.assessment_rapid_en owner to cabd;
alter view stream_crossings.assessment_rapid_fr owner to cabd;



CREATE OR REPLACE VIEW cabd.community_data_staging_view
AS SELECT a.id,
    a.cabd_id,
    a.user_id,
    a.uploaded_datetime,
    a.data,
    'stream_crossings'::text AS feature_type,
        CASE
            WHEN b.status IS NULL THEN 'NEW'::character varying
            ELSE b.status::character varying
        END AS status,
        CASE
            WHEN b.passability_status_code IS NULL THEN 4
            ELSE b.passability_status_code::integer
        END AS passability_status_code
   FROM stream_crossings.stream_crossings_community_staging a
     LEFT JOIN cabd.community_holding b ON a.id = b.id
UNION
 SELECT a.id,
    a.cabd_id,
    a.user_id,
    a.uploaded_datetime,
    a.data,
    'dams'::text AS feature_type,
    'NEW'::character varying AS status,
        CASE
            WHEN b.passability_status_code IS NULL THEN 4
            ELSE b.passability_status_code::integer
        END AS passability_status_code
   FROM dams.dams_community_staging a
     LEFT JOIN cabd.community_holding b ON a.id = b.id;

  alter view cabd.community_data_staging_view owner to cabd;