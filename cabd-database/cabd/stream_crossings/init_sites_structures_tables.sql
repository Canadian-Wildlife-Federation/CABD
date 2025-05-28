-- function to initial populate stream crossings

-- TODO (see todo text):
-- * assessment data
-- * fix distance tolerance to use metres not degrees
-- * sort out nhn, chyf snapping (I don't have these setup to test locally)
-- * review settings of province_territory_code, nhn_watershed_id

CREATE OR REPLACE FUNCTION stream_crossings.init_stream_crossings() 
RETURNS void 
LANGUAGE plpgsql 
as $$
DECLARE
  field_list TEXT[];
  field varchar;
  field_src varchar;
  field_dsid varchar;
  t varchar;
  srowcnt integer;
  rec record;
  doupdate boolean;

  STREAM_SNAP_TOLERANCE integer := 500;
  CABD_FEATURE_DISANCE_MATCH_TOLDERANCE integer:= 1; --todo this is currently in degrees - need to change to m


BEGIN

    --   ***** MODELLED CROSSINGS *****
    --populated sites from modelled crossings
    insert into stream_crossings.sites(
        cabd_id, municipality,
        stream_name, road_name, crossing_type_code, num_structures, site_type_code,
        crossing_comments, original_point, province_territory_code, nhn_watershed_id,
        strahler_order, 
        assessment_type_code, 
        addressed_status_code
    )
    select 
      cabd_id, municipality, 
      stream_name_1, transport_feature_name, crossing_type_code, 1, 2, 
      comments, geometry, province_territory_code, nhn_watershed_id, 
      stream_order, 
      case when assessment_type_code = 3 then 6 else assessment_type_code end, 
      addressed_status_code
    from modelled_crossings.modelled_crossings;

    -- populate sites attribute source details
    insert into stream_crossings.sites_attribute_source(cabd_id) select cabd_id from modelled_crossings.modelled_crossings;    
    field_list := ARRAY['municipality', 'stream_name', 'road_name', 'crossing_type_code', 'num_structures', 'site_type_code', 'crossing_comments', 'original_point','strahler_order', 'assessment_type_code', 'addressed_status_code'];
    FOREACH field IN ARRAY field_list LOOP
        field_src := field || '_src';
        field_dsid := field || '_dsid';
        t := format ('update stream_crossings.sites_attribute_source set %I = ''m'', %I = a.cabd_id from stream_crossings.sites a where a.cabd_id = stream_crossings.sites_attribute_source.cabd_id and %I is not null', field_src, field_dsid, field);
		execute t;
    END LOOP;


    --populate structures from modelled crossings
    insert into stream_crossings.structures(
        structure_id, site_id,
        primary_structure, structure_number, passability_status_code
    )
    select gen_random_uuid(), cabd_id,
    true, 1, passability_status_code
    from modelled_crossings.modelled_crossings;

    -- populate strcutures attribute source details
    insert into stream_crossings.structures_attribute_source(structure_id) select structure_id from stream_crossings.structures;
    field_list := ARRAY['primary_structure', 'structure_number', 'passability_status_code'];
    FOREACH field IN ARRAY field_list LOOP
        field_src := field || '_src';
        field_dsid := field || '_dsid';
        t := format ('update stream_crossings.structures_attribute_source set %I = ''m'', %I = a.site_id from stream_crossings.structures a where a.structure_id = stream_crossings.structures_attribute_source.structure_id and %I is not null', field_src, field_dsid, field);
		execute t;
    END LOOP;


    --   ***** Satellite CROSSINGS *****
    -- will not create any new records; we will only replace existing records and 
    -- remove them in the case of dams

    --sites data to update
    create temp table supdates as 
    select 
        cabd_id as cabd_id, 
        id as id, 
        case when driveway_crossing then 4 else null end as road_type_code,
        case when crossing_type_code is not null then crossing_type_code else null end as crossing_type_code,
        case when reviewer_comments is not null then reviewer_comments end as crossing_comments,
        4 as assessment_type_code
    from stream_crossings.cwf_satellite_review
    where status = 2 and (new_crossing_type is null or new_crossing_type not ilike 'dam');

   -- assumption is that only one record per cabd_id; if there are more throw an error
   select count(*) into srowcnt
   from (select cabd_id, count(*) from supdates group by cabd_id having count(*) > 1) foo;

	if (srowcnt > 0) then
		raise exception 'Multiple satelitte review records for a single cabd id. This is not supported.';
	end if;

    field_list := ARRAY['road_type_code', 'crossing_type_code', 'crossing_comments', 'assessment_type_code'];
    FOREACH field IN ARRAY field_list LOOP
        field_src := field || '_src';
        field_dsid := field || '_dsid';
        t := format ('update stream_crossings.sites_attribute_source 
						set %I = ''s'', %I = a.id 
						from supdates a 
						where a.cabd_id = stream_crossings.sites_attribute_source.cabd_id and %I is not null and %I in (''m'', ''s'')', field_src, field_dsid, field, field_src);
		execute t;


        t := format ('update stream_crossings.sites 
						set %I = a.%I 
						from supdates a join stream_crossings.sites_attribute_source b on a.cabd_id = b.cabd_id
						where a.cabd_id = stream_crossings.sites.cabd_id and a.%I is not null and b.%I in (''m'', ''s'')', field, field, field, field_src);
		execute t;

    END LOOP;

    drop table supdates;
    --there are no structure attributes to update
    update stream_crossings.cwf_satellite_review set status = 3 where status = 2 and new_crossing_type not ilike 'dam';


    -- deal with dams cases
	
	--remove dams from site/structures tables
	delete from stream_crossings.structures_attribute_source where structure_id in (select structure_id from stream_crossings.structures a where a.site_id in (select cabd_id from stream_crossings.cwf_satellite_review where status = 2 and new_crossing_type ilike 'dam'));
    delete from stream_crossings.sites_attribute_source where cabd_id in (select cabd_id from stream_crossings.cwf_satellite_review where status = 2 and new_crossing_type ilike 'dam');
    delete from stream_crossings.structures where site_id in (select cabd_id from stream_crossings.cwf_satellite_review where status = 2 and new_crossing_type ilike 'dam');
    delete from stream_crossings.sites where cabd_id in (select cabd_id from stream_crossings.cwf_satellite_review where status = 2 and new_crossing_type ilike 'dam');

	--add to dams
    -- TODO: nhn_watershed_id, municipality?
    insert into dams.dams (cabd_id, assessment_type_code, province_territory_code, original_point, snapped_point, snapped_ncc)
    select cabd_id, 4, cabd.find_province_territory_code(st_transform(st_setsrid(st_makepoint(new_dam_longitude, new_dam_latitude), 4326), 4617)),
		st_transform(st_setsrid(st_makepoint(new_dam_longitude, new_dam_latitude), 4326), 4617), 
		null, --todo: cabd.snap_point_to_chyf_network(st_transform(st_setsrid(st_makepoint(new_dam_longitude, new_dam_latitude), 4326), 4617), STREAM_SNAP_TOLERANCE),
		null --todo: cabd.snap_point_to_nhn_network(st_transform(st_setsrid(st_makepoint(new_dam_longitude, new_dam_latitude), 4326), 4617), STREAM_SNAP_TOLERANCE)
    from stream_crossings.cwf_satellite_review 
    where status = 2 and new_crossing_type ilike 'dam' and create_dam ;

    insert into dams.dams_attribute_source(cabd_id, assessment_type_code_ds, original_point_ds)
	select cabd_id, b.id, b.id
	from stream_crossings.cwf_satellite_review, (select id from cabd.data_source where name = 'cwf') b
	where status = 2 and new_crossing_type ilike 'dam' and create_dam ;

    update stream_crossings.cwf_satellite_review set status = 3 where status = 2 and new_crossing_type ilike 'dam';

   -- **** COMMUNITY DATA ****
   
   alter table stream_crossings.stream_crossings_community_holding add column site_cabd_id uuid;
   alter table stream_crossings.stream_crossings_community_holding add column structure_cabd_id uuid;
	update stream_crossings.stream_crossings_community_holding set site_cabd_id = null, structure_cabd_id = null where status = 2;

    --matching cabd_ids
    update stream_crossings.stream_crossings_community_holding set site_cabd_id = cabd_id 
    where cabd_id in (select cabd_id from stream_crossings.sites) and 
        status = 2 and -- reviewed
        feature_type_code = 1 and --stream_crossing
        to_feature_type_code in (1, 3, 4); --streamcorssing;nostructure,noaccess

	--matching "distance tolerance"
	with toprocess as (
    	select *, st_transform(st_setsrid(st_makepoint(longitude, latitude), 4326), 4617) as point  from stream_crossings.stream_crossings_community_holding where status = 2 and site_cabd_id is null and  feature_type_code = 1 and to_feature_type_code in (1, 3, 4)
	),
    updates as (
		SELECT sat.id, structs.cabd_id, structs.dist as distance
		FROM toprocess sat
		CROSS JOIN LATERAL (
		  SELECT sat.id, b.cabd_id, b.original_point <-> sat.point AS dist
		  FROM stream_crossings.sites b
		  ORDER BY dist
		  LIMIT 1
		) structs
	)
	update stream_crossings.stream_crossings_community_holding set site_cabd_id = updates.cabd_id
	from updates
	where updates.id = stream_crossings_community_holding.id and updates.distance < CABD_FEATURE_DISANCE_MATCH_TOLDERANCE; 

    -- update structure id with primary structure
	update stream_crossings.stream_crossings_community_holding set structure_cabd_id = a.structure_id
	from stream_crossings.structures a 
	where a.site_id = stream_crossings_community_holding.site_cabd_id and a.primary_structure;


    create temp table supdates as 
    select 
        site_cabd_id as cabd_id, 
		structure_cabd_id as structure_id,
        id as id, 
        id as cabd_assessment_id, 
        uploaded_datetime as date_assessed, 
        road_type_code as road_type_code,
        crossing_type_code as crossing_type_code,
        structure_count as num_structures,
        structure_inlet_image as photo_id_inlet,
        structure_outlet_image as photo_id_outlet,
        upstream_direction_image as photo_id_upstream,
        downstream_direction_image as photo_id_downstream,
        notes as crossing_comments,
        st_transform(st_setsrid(st_makepoint(longitude, latitude), 4326), 4617) as original_point,
		case when upstream_physical_blockages_code is null and downstream_physical_blockages_code is null then null when upstream_physical_blockages_code is null and downstream_physical_blockages_code is not null then downstream_physical_blockages_code when upstream_physical_blockages_code is not null and downstream_physical_blockages_code is null then upstream_physical_blockages_code else ARRAY(SELECT DISTINCT UNNEST(upstream_physical_blockages_code || downstream_physical_blockages_code)) end as physical_blockages_code
    from stream_crossings.stream_crossings_community_holding
    where site_cabd_id is not null and status = 2 and feature_type_code = 1 and to_feature_type_code in (1, 3, 4);

    
    field_list := ARRAY['road_type_code', 'crossing_type_code', 'num_structures', 'photo_id_inlet', 'photo_id_outlet', 'photo_id_upstream', 'photo_id_downstream', 'crossing_comments','original_point'];

	--sites
   FOR rec IN SELECT * FROM supdates LOOP
		
        FOREACH field IN ARRAY field_list LOOP

            field_src := field || '_src';
            field_dsid := field || '_dsid';

			t := format('select case 
				when u.%I is null then false 
				when %I is null then true
				when (%I in (''m'', ''s'') or (%I = ''c'' and b.uploaded_datetime < u.date_assessed)) then true 
				else false end  
			from supdates u left join stream_crossings.sites_attribute_source a on u.cabd_id = a.cabd_id 
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.%I 
			where u.id = ''%s''', field, field_src, field_src, field_src, field_dsid, rec.id);
			
			execute t into doupdate ;

            if (doupdate) then
               
                t := format ('update stream_crossings.sites_attribute_source  set %I = ''c'', %I = ''%s'' where cabd_id = ''%s''', field_src, field_dsid, rec.id, rec.cabd_id);
				execute t;
				
                t := format ('update stream_crossings.sites set %I = (select %I from supdates where id = ''%s'') where cabd_id = ''%s''', field, field, rec.id, rec.cabd_id);
                execute t;
                
				if (field = 'original_point') then
					--todo update province territory code or nhn watershed id??
					update stream_crossings.sites set
						snapped_point = null, --todo: cabd.snap_point_to_chyf_network(original_point, STREAM_SNAP_TOLERANCE),
						snapped_ncc = null --todo: cabd.snap_point_to_nhn_network(original_point, STREAM_SNAP_TOLERANCE)
					where cabd_id = rec.cabd_id;
				end if;
            end if;

        END LOOP;

    END LOOP; 
	
	--structures
	field_list := ARRAY['physical_blockages_code'];

	FOR rec IN SELECT * FROM supdates LOOP
	    FOREACH field IN ARRAY field_list LOOP
            field_src := field || '_src';
            field_dsid := field || '_dsid';

			t := format('select case 
				when u.%I is null then false 
				when %I is null then true
				when (%I in (''m'', ''s'') or (%I = ''c'' and b.uploaded_datetime < u.date_assessed)) then true 
				else false end  
			from supdates u left join stream_crossings.structures_attribute_source a on u.structure_id = a.structure_id
				left join stream_crossings.stream_crossings_community_holding b on b.id = a.%I 
			where u.id = ''%s''', field, field_src, field_src, field_src, field_dsid, rec.id);

			execute t into doupdate ;

            if (doupdate) then
                
                t := format ('update stream_crossings.structures_attribute_source  set %I = ''c'', %I = ''%s'' where structure_id = ''%s''', field_src, field_dsid, rec.id, rec.structure_id);
				execute t;
				
                t := format ('update stream_crossings.structures set %I = (select %I from supdates where id = ''%s'') where structure_id = ''%s''', field, field, rec.id, rec.structure_id);
                execute t;
                
            end if;

        END LOOP;

    END LOOP; 
	--status   
	update stream_crossings.stream_crossings_community_holding set status = 3 where site_cabd_id is not null;
	drop table supdates;

	--new sites&structures
	update stream_crossings.stream_crossings_community_holding set site_cabd_id = cabd_id, structure_cabd_id = uuid_generate_v4()
    where status = 2 and -- reviewed
        feature_type_code = 1 and --stream_crossing
        to_feature_type_code in (1, 3, 4); --streamcorssing;nostructure,noaccess

   
     
	with idata as (
		select site_cabd_id as cabd_id, 
	        road_type_code,
	        crossing_type_code,
	        structure_count as num_structures,
	        structure_inlet_image as photo_id_inlet,
	        structure_outlet_image as photo_id_outlet,
	        upstream_direction_image as photo_id_upstream,
	        downstream_direction_image as photo_id_downstream,
	        notes as crossing_comments,
			st_transform(st_setsrid(st_makepoint(longitude, latitude), 4326), 4617) as original_point

	    from stream_crossings.stream_crossings_community_holding
		    where site_cabd_id is not null and status = 2 and feature_type_code = 1 and to_feature_type_code in (1, 3, 4)
	)

	insert into stream_crossings.sites(cabd_id, road_type_code, crossing_type_code, 
		num_structures, photo_id_inlet, photo_id_outlet, photo_id_upstream,
		 photo_id_downstream, crossing_comments, original_point, 
		snapped_point,
		 snapped_ncc, 
		province_territory_code, nhn_watershed_id,
		 site_type_code, assessment_type_code, addressed_status_code)
	select cabd_id, road_type_code, crossing_type_code,
        num_structures, photo_id_inlet, photo_id_outlet, photo_id_upstream,
        photo_id_downstream, crossing_comments, original_point,
		null, --todo: cabd.snap_point_to_chyf_network( original_point, STREAM_SNAP_TOLERANCE),
		null, -- todo: cabd.snap_point_to_nhn_network( original_point, STREAM_SNAP_TOLERANCE),
		cabd.find_province_territory_code(original_point),
		null, --todo nhn watershed id
		99, 1, 99
    from idata;

	with idata as (
		select id as id, 
			site_cabd_id as cabd_id, 
	        road_type_code,
	        crossing_type_code,
	        structure_count as num_structures,
	        structure_inlet_image as photo_id_inlet,
	        structure_outlet_image as photo_id_outlet,
	        upstream_direction_image as photo_id_upstream,
	        downstream_direction_image as photo_id_downstream,
	        notes as crossing_comments,
			st_transform(st_setsrid(st_makepoint(longitude, latitude), 4326), 4617) as original_point
	    from stream_crossings.stream_crossings_community_holding
		    where site_cabd_id is not null and status = 2 and feature_type_code = 1 and to_feature_type_code in (1, 3, 4)
	)
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
		addressed_status_code_src, addressed_status_code_dsid)

	select cabd_id, 
			case when road_type_code is null then null else 'c' end,
			case when road_type_code is null then null else id end,
			case when crossing_type_code is null then null else 'c' end,
			case when crossing_type_code is null then null else id end,
			case when num_structures is null then null else 'c' end,
			case when num_structures is null then null else id end,
			case when photo_id_inlet is null then null else 'c' end,
			case when photo_id_inlet is null then null else id end,
			case when photo_id_outlet is null then null else 'c' end,
			case when photo_id_outlet is null then null else id end,
			case when photo_id_upstream is null then null else 'c' end,
			case when photo_id_upstream is null then null else id end,
			case when photo_id_downstream is null then null else 'c' end,
			case when photo_id_downstream is null then null else id end,
			case when crossing_comments is null then null else 'c' end,
			case when crossing_comments is null then null else id end,
			'c', id, --original point
			'c', id, --site_type_code
			'c', id, --assessment_type_code
			'c', id --addressed status code
	  from idata;

	with idata as (
		select structure_cabd_id as structure_id, 
			site_cabd_id as site_id, 
			case when upstream_physical_blockages_code is null and downstream_physical_blockages_code is null then null when upstream_physical_blockages_code is null and downstream_physical_blockages_code is not null then downstream_physical_blockages_code when upstream_physical_blockages_code is not null and downstream_physical_blockages_code is null then upstream_physical_blockages_code else ARRAY(SELECT DISTINCT UNNEST(upstream_physical_blockages_code || downstream_physical_blockages_code)) end as physical_blockages_code       

	    from stream_crossings.stream_crossings_community_holding
		    where site_cabd_id is not null and status = 2 and feature_type_code = 1 and to_feature_type_code in (1, 3, 4)
	)

	insert into stream_crossings.structures(structure_id, site_id, physical_blockages_code,	primary_structure, structure_number, passability_status_code)
	select structure_id, site_id, physical_blockages_code, true, 1, 99
    from idata;

	with idata as (
		select structure_cabd_id as structure_id, 
			site_cabd_id as site_id, 
			id as id,
			case when upstream_physical_blockages_code is null and downstream_physical_blockages_code is null then null when upstream_physical_blockages_code is null and downstream_physical_blockages_code is not null then downstream_physical_blockages_code when upstream_physical_blockages_code is not null and downstream_physical_blockages_code is null then upstream_physical_blockages_code else ARRAY(SELECT DISTINCT UNNEST(upstream_physical_blockages_code || downstream_physical_blockages_code)) end as physical_blockages_code       

	    from stream_crossings.stream_crossings_community_holding
		    where site_cabd_id is not null and status = 2 and feature_type_code = 1 and to_feature_type_code in (1, 3, 4)
	)
	insert into stream_crossings.structures_attribute_source(structure_id, 
		physical_blockages_code_src, physical_blockages_code_dsid, 
		primary_structure_src, primary_structure_dsid, 
		structure_number_src, structure_number_dsid, 
		passability_status_code_src, passability_status_code_dsid)

	select structure_id, 
			case when physical_blockages_code is null then null else 'c' end,
			case when physical_blockages_code is null then null else id end,
			'c', id, --primary_structure
			'c', id, --structure_number
			'c', id --passability_status_code
	  from idata;

	update stream_crossings.stream_crossings_community_holding set status = 3 where site_cabd_id is not null;
	alter table stream_crossings.stream_crossings_community_holding drop column site_cabd_id;
    alter table stream_crossings.stream_crossings_community_holding drop column structure_cabd_id;


	-- ***** ASSESSMENT DATA *******
	-- TODO
END;
$$;
