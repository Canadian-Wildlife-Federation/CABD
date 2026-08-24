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
		snapped_ncc = st_transform(cabd.snap_point_to_nhn_network(newpoint, STREAM_SNAP_TOLERANCE), 3979)
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
            st_transform(cabd.snap_point_to_nhn_network(newpoint, STREAM_SNAP_TOLERANCE), 3979),
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
            st_transform(cabd.snap_point_to_nhn_network( newpoint, STREAM_SNAP_TOLERANCE),3979),
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
