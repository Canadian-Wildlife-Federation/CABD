-- trigger to apply new community data to sites and structures

CREATE OR REPLACE FUNCTION stream_crossings.stream_crossings_community_holding_data_trg() 
RETURNS TRIGGER  
LANGUAGE plpgsql 
as $$
DECLARE
  cabdid uuid;
  structureid uuid;
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
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.road_type_code_dsid
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
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.crossing_type_code_dsid
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
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.num_structures_dsid
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
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.photo_id_inlet_dsid
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
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.photo_id_outlet_dsid
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
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.photo_id_upstream_dsid
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
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.photo_id_downstream_dsid
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
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.crossing_comments_dsid
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
			left join stream_crossings.stream_crossings_community_holding b on b.id = a.original_point_dsid
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
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.physical_blockages_code_dsid                
            where a.structure_id in (select structure_id from stream_crossings.structures where site_id = cabdid);
			
            if (doupdate) then
                update stream_crossings.structures set physical_blockages_code = case when NEW.upstream_physical_blockages_code is null and NEW.downstream_physical_blockages_code is null then null when NEW.upstream_physical_blockages_code is null and NEW.downstream_physical_blockages_code is not null then NEW.downstream_physical_blockages_code when NEW.upstream_physical_blockages_code is not null and NEW.downstream_physical_blockages_code is null then NEW.upstream_physical_blockages_code else ARRAY(SELECT DISTINCT UNNEST(NEW.upstream_physical_blockages_code || NEW.downstream_physical_blockages_code)) end 
                where site_id = cabdid;
                update stream_crossings.structures_attribute_source 
                    set physical_blockages_code_src = 'c', physical_blockages_code_dsid = NEW.id 
                    where structure_id in (select structure_id from stream_crossings.structures where site_id = cabdid);
            end if;

        end if;
	    
        update stream_crossings.stream_crossings_community_holding set status = 'PROCESSED' where id = NEW.id;

    else

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
        structureid := gen_random_uuid();
        insert into stream_crossings.structures(structure_id, site_id, physical_blockages_code,	primary_structure, structure_number, passability_status_code)
	    values (structureid, NEW.cabd_id,  
            case when NEW.upstream_physical_blockages_code is null and NEW.downstream_physical_blockages_code is null then null when NEW.upstream_physical_blockages_code is null and NEW.downstream_physical_blockages_code is not null then NEW.downstream_physical_blockages_code when NEW.upstream_physical_blockages_code is not null and NEW.downstream_physical_blockages_code is null then NEW.upstream_physical_blockages_code else ARRAY(SELECT DISTINCT UNNEST(NEW.upstream_physical_blockages_code || NEW.downstream_physical_blockages_code)) end,
            true, 1, 99);
        
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

	    update stream_crossings.stream_crossings_community_holding set status = 'PROCESSED' where id = NEW.id;

    END IF;
	RETURN NEW;
END;
$$;



CREATE OR REPLACE TRIGGER stream_crossings_community_holding_data_trg
AFTER INSERT OR UPDATE ON stream_crossings.stream_crossings_community_holding
FOR EACH ROW
WHEN (NEW.status = 'REVIEWED')
EXECUTE FUNCTION stream_crossings.stream_crossings_community_holding_data_trg();
