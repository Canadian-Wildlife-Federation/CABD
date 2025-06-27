-- function to initial populate stream crossings

-- You may need to drop triggers related to maintenance before running:
-- drop TRIGGER if exists stream_crossings_assessment_data_trg on stream_crossings.assessment_data ;
-- drop TRIGGER if exists stream_crossings_community_holding_data_trg ON stream_crossings.stream_crossings_community_holding;
-- drop TRIGGER if exists cwf_satellite_data_data_trg ON stream_crossings.cwf_satellite_review;

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

  STREAM_SNAP_TOLERANCE integer := 500; ----for snapping points to stream network chyf and nhn
  CABD_FEATURE_DISANCE_MATCH_TOLDERANCE integer:= 100; --feature matching distance in meters


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
        case when crossing_type_code is not null then crossing_type_code else null end as crossing_type_code ,
        case when reviewer_comments is not null then reviewer_comments end as crossing_comments,
        4 as assessment_type_code
    from stream_crossings.cwf_satellite_review
    where status = 'REVIEWED' and (new_crossing_type is null or new_crossing_type != 'dam');

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
    update stream_crossings.cwf_satellite_review set status = 'PROCESSED' where status = 'REVIEWED' and new_crossing_type != 'dam';


    -- deal with dams cases
	
	--remove dams from site/structures tables
	delete from stream_crossings.structures_attribute_source where structure_id in (select structure_id from stream_crossings.structures a where a.site_id in (select cabd_id from stream_crossings.cwf_satellite_review where status = 'REVIEWED' and new_crossing_type = 'dam'));
    delete from stream_crossings.sites_attribute_source where cabd_id in (select cabd_id from stream_crossings.cwf_satellite_review where status = 'REVIEWED' and new_crossing_type = 'dam');
    delete from stream_crossings.structures where site_id in (select cabd_id from stream_crossings.cwf_satellite_review where status = 'REVIEWED' and new_crossing_type = 'dam');
    delete from stream_crossings.sites where cabd_id in (select cabd_id from stream_crossings.cwf_satellite_review where status = 'REVIEWED' and new_crossing_type = 'dam');

	--add to dams
    -- TODO: municipality?
    insert into dams.dams (cabd_id, assessment_type_code, 
        province_territory_code, nhn_watershed_id, original_point, snapped_point, snapped_ncc)
    select cabd_id, 4, 
        cabd.find_province_territory_code(st_transform(st_setsrid(st_makepoint(new_dam_longitude, new_dam_latitude), 4326), 4617)),
        cabd.find_nhn_watershed_id(st_transform(st_setsrid(st_makepoint(new_dam_longitude, new_dam_latitude), 4326), 4617)),
		st_transform(st_setsrid(st_makepoint(new_dam_longitude, new_dam_latitude), 4326), 4617), 
		cabd.snap_point_to_chyf_network(st_transform(st_setsrid(st_makepoint(new_dam_longitude, new_dam_latitude), 4326), 4617), STREAM_SNAP_TOLERANCE),
		cabd.snap_point_to_nhn_network(st_transform(st_setsrid(st_makepoint(new_dam_longitude, new_dam_latitude), 4326), 4617), STREAM_SNAP_TOLERANCE)
    from stream_crossings.cwf_satellite_review 
    where status = 'REVIEWED' and new_crossing_type = 'dam' and create_dam ;

    insert into dams.dams_attribute_source(cabd_id, assessment_type_code_ds, original_point_ds)
	select cabd_id, b.id, b.id
	from stream_crossings.cwf_satellite_review, (select id from cabd.data_source where name = 'cwf') b
	where status = 'REVIEWED' and new_crossing_type = 'dam' and create_dam ;

    update stream_crossings.cwf_satellite_review set status = 'PROCESSED' where status = 'REVIEWED' and new_crossing_type = 'dam';

   -- **** COMMUNITY DATA ****
   
   alter table stream_crossings.stream_crossings_community_holding add column site_cabd_id uuid;
   alter table stream_crossings.stream_crossings_community_holding add column structure_cabd_id uuid;
   
	update stream_crossings.stream_crossings_community_holding set site_cabd_id = null, structure_cabd_id = null where status = 'REVIEWED';

    --matching cabd_ids
    update stream_crossings.stream_crossings_community_holding set site_cabd_id = cabd_id 
    where cabd_id in (select cabd_id from stream_crossings.sites) and 
        status = 'REVIEWED' and
        feature_type_code is not null and feature_type_code = 1 and --stream_crossing
        to_feature_type_code is not null and to_feature_type_code in (1, 3, 4); --streamcorssing;nostructure,noaccess

	--matching "distance tolerance"
	with toprocess as (
    	select *, st_transform(st_setsrid(st_makepoint(longitude, latitude), 4326), 4617)::geography as point 
        from stream_crossings.stream_crossings_community_holding 
        where status = 'REVIEWED' and 
            site_cabd_id is null and  
            feature_type_code is not null and 
            feature_type_code = 1 and 
            to_feature_type_code is not null and 
            to_feature_type_code in (1, 3, 4)
	),
    updates as (
		SELECT sat.id, structs.cabd_id, structs.dist as distance
		FROM toprocess sat
		CROSS JOIN LATERAL (
		  SELECT sat.id, b.cabd_id, b.original_point::geography <-> sat.point AS dist
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
    where site_cabd_id is not null and status = 'REVIEWED' and (feature_type_code is not null and feature_type_code = 1) and (to_feature_type_code is not null and to_feature_type_code in (1, 3, 4));

    
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

					update stream_crossings.sites set
						snapped_point = cabd.snap_point_to_chyf_network(original_point, STREAM_SNAP_TOLERANCE),
						snapped_ncc = cabd.snap_point_to_nhn_network(original_point, STREAM_SNAP_TOLERANCE),
                        province_territory_code = cabd.find_province_territory_code(original_point),
                        nhn_watershed_id = cabd.find_nhn_watershed_id(original_point)
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
	update stream_crossings.stream_crossings_community_holding set status = 'PROCESSED' where site_cabd_id is not null;
	drop table supdates;

	--new sites&structures
	update stream_crossings.stream_crossings_community_holding set site_cabd_id = cabd_id, structure_cabd_id = uuid_generate_v4()
    where status = 'REVIEWED' and -- reviewed
        feature_type_code is not null and feature_type_code = 1 and --stream_crossing
        to_feature_type_code is not null and to_feature_type_code in (1, 3, 4); --streamcorssing;nostructure,noaccess

   
     
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
		    where site_cabd_id is not null and status = 'REVIEWED' and feature_type_code is not null and feature_type_code = 1 and to_feature_type_code is not null and to_feature_type_code in (1, 3, 4)
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
		cabd.snap_point_to_chyf_network( original_point, STREAM_SNAP_TOLERANCE),
		cabd.snap_point_to_nhn_network( original_point, STREAM_SNAP_TOLERANCE),
		cabd.find_province_territory_code(original_point),
        cabd.find_nhn_watershed_id(original_point),		
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
		    where site_cabd_id is not null and status = 'REVIEWED' and feature_type_code is not null and feature_type_code = 1 and to_feature_type_code is not null and to_feature_type_code in (1, 3, 4)
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
		    where site_cabd_id is not null and status = 'REVIEWED' and feature_type_code is not null and feature_type_code = 1 and to_feature_type_code is not null and to_feature_type_code in (1, 3, 4)
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
		    where site_cabd_id is not null and status = 'REVIEWED' and feature_type_code is not null and feature_type_code = 1 and to_feature_type_code is not null and to_feature_type_code in (1, 3, 4)
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

	update stream_crossings.stream_crossings_community_holding set status = 'PROCESSED' where site_cabd_id is not null;
	alter table stream_crossings.stream_crossings_community_holding drop column site_cabd_id;
    alter table stream_crossings.stream_crossings_community_holding drop column structure_cabd_id;


	-- ***** ASSESSMENT DATA *******
	
	--find the newest assessment data 
	create temp table allassessmentdata as (
		select * from (
			SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cabd_id ORDER BY date_assessed DESC) AS rn
			FROM stream_crossings.assessment_data WHERE status = 'NEW')
		foo where rn = 1
	);
	
	alter table allassessmentdata add column site_cabd_id uuid;

	--calculate geometry
	alter table allassessmentdata add column newpoint geometry(point, 4617);
	update allassessmentdata set newpoint = st_transform(st_setsrid(st_makepoint(longitude, latitude), 4326), 4617);

    --calculate addressed_status_code
    alter table allassessmentdata add column addressed_status_code integer;
    update allassessmentdata set addressed_status_code = case when crossing_type_code = 6 then 1 else null end;

    with records as (
        select assessment_id from stream_crossings.assessment_structure_data where internal_structures_code = 2
    )
    update allassessmentdata set addressed_status_code = 4 where
        addressed_status_code is null and id in (select assessment_id from records);

    with records as (
        select assessment_id from stream_crossings.assessment_structure_data where backwatered_pct_code not in (1, 99)
    ) 
    update allassessmentdata set addressed_status_code = 5 where
        addressed_status_code is null and id in (select assessment_id from records);

	--find existing ids
	update allassessmentdata set site_cabd_id = cabd_id where cabd_id in (select cabd_id from stream_crossings.sites);

	--search based on distance
	with toprocess as (
    	select *, newpoint::geography as point from allassessmentdata where site_cabd_id is null
	),
    updates as (
		SELECT sat.id, structs.cabd_id, structs.dist as distance
		FROM toprocess sat
		CROSS JOIN LATERAL (
		  SELECT sat.id, b.cabd_id, b.original_point::geography <-> sat.point AS dist
		  FROM stream_crossings.sites b
		  ORDER BY dist
		  LIMIT 1
		) structs
	)
	update allassessmentdata set site_cabd_id = updates.cabd_id
	from updates
	where site_cabd_id is null and updates.id = allassessmentdata.id and updates.distance < CABD_FEATURE_DISANCE_MATCH_TOLDERANCE; 

    --keep existing strahler_order if possible
    alter table allassessmentdata add column strahler_order integer;
    alter table allassessmentdata add column strahler_order_dsid uuid;
    alter table allassessmentdata add column strahler_order_src varchar;
    update allassessmentdata set strahler_order = a.strahler_order from stream_crossings.sites a where a.cabd_id = site_cabd_id;
    update allassessmentdata set strahler_order_dsid = a.strahler_order_dsid, strahler_order_src = a.strahler_order_src from stream_crossings.sites_attribute_source a where a.cabd_id = site_cabd_id;
    
	--these will all be new features
	update allassessmentdata set site_cabd_id = uuid_generate_v4() where site_cabd_id is null;

	--delete any existing site/structure data for these ids and then add new information
	delete from stream_crossings.structures_attribute_source where structure_id in (select structure_id from stream_crossings.structures where site_id in (select site_cabd_id from allassessmentdata));
	delete from stream_crossings.sites_attribute_source where cabd_id in (select site_cabd_id from allassessmentdata);
	delete from stream_crossings.structures where site_id in (select site_cabd_id from allassessmentdata);
	delete from stream_crossings.sites where cabd_id in (select site_cabd_id from allassessmentdata);

	
	--create a sites based on assessment
    INSERT INTO stream_crossings.sites (
            cabd_id,
            other_id,
            cabd_assessment_id,
            original_assessment_id,
            date_assessed,
            lead_assessor,
            municipality,
            stream_name,
            road_name,
            road_type_code,
            location_description,
            land_ownership_context,
            incomplete_assess_code,
            crossing_type_code,
            num_structures,
            photo_id_inlet,
            photo_id_outlet,
            photo_id_upstream,
            photo_id_downstream,
            photo_id_road_surface,
            photo_id_other_a,
            photo_id_other_b,
            photo_id_other_c,
            flow_condition_code,
            crossing_condition_code,
            site_type_code,
            alignment_code,
            road_fill_height_m,
            bankfull_width_upstr_a_m,
            bankfull_width_upstr_b_m,
            bankfull_width_upstr_c_m,
            bankfull_width_upstr_avg_m,
            bankfull_width_dnstr_a_m,
            bankfull_width_dnstr_b_m,
            bankfull_width_dnstr_c_m,
            bankfull_width_dnstr_avg_m,
            bankfull_confidence_code,
            scour_pool_tailwater_code,
            crossing_comments,
            original_point,
            snapped_point,
            snapped_ncc,
            province_territory_code,
            nhn_watershed_id,
            strahler_order,
            assessment_type_code,
            addressed_status_code
          )
    select site_cabd_id,
        other_id,
        cabd_assessment_id,
        original_assessment_id,
        date_assessed,
        lead_assessor,
        municipality,
        stream_name,
        road_name,
        road_type_code,
        location_description,
        land_ownership_context,
        incomplete_assess_code,
        crossing_type_code,
        num_structures,
        photo_id_inlet,
        photo_id_outlet,
        photo_id_upstream,
        photo_id_downstream,
        photo_id_road_surface,
        photo_id_other_a,
        photo_id_other_b,
        photo_id_other_c,
        flow_condition_code,
        crossing_condition_code,
        site_type_code,
        alignment_code,
        road_fill_height_m,
        bankfull_width_upstr_a_m,
        bankfull_width_upstr_b_m,
        bankfull_width_upstr_c_m,
        bankfull_width_upstr_avg_m,
        bankfull_width_dnstr_a_m,
        bankfull_width_dnstr_b_m,
        bankfull_width_dnstr_c_m,
        bankfull_width_dnstr_avg_m,
        bankfull_confidence_code,
        scour_pool_tailwater_code,
        crossing_comments,
        newpoint, --original_point
        cabd.snap_point_to_chyf_network(newpoint, STREAM_SNAP_TOLERANCE), --snapped point
        cabd.snap_point_to_nhn_network(newpoint, STREAM_SNAP_TOLERANCE),  --snapped ncc
        cabd.find_province_territory_code(newpoint),
        cabd.find_nhn_watershed_id(newpoint),       
        strahler_order,
        CASE WHEN original_assessment_id IS NULL THEN 2 ELSE 3  END,
        addressed_status_code
        
    from allassessmentdata;

    --data sources
    insert into stream_crossings.sites_attribute_source(
            cabd_id,
            other_id_src,other_id_dsid,
            municipality_src,municipality_dsid,
            stream_name_src,stream_name_dsid,
            road_name_src,road_name_dsid,
            road_type_code_src,road_type_code_dsid,
            location_description_src,location_description_dsid,
            land_ownership_context_src,land_ownership_context_dsid,
            incomplete_assess_code_src,incomplete_assess_code_dsid,
            crossing_type_code_src,crossing_type_code_dsid,
            num_structures_src,num_structures_dsid,
            photo_id_inlet_src,photo_id_inlet_dsid,
            photo_id_outlet_src,photo_id_outlet_dsid,
            photo_id_upstream_src,photo_id_upstream_dsid,
            photo_id_downstream_src,photo_id_downstream_dsid,
            photo_id_road_surface_src,photo_id_road_surface_dsid,
            photo_id_other_a_src,photo_id_other_a_dsid,
            photo_id_other_b_src,photo_id_other_b_dsid,
            photo_id_other_c_src,photo_id_other_c_dsid,
            flow_condition_code_src,flow_condition_code_dsid,
            crossing_condition_code_src,crossing_condition_code_dsid,
            site_type_code_src,site_type_code_dsid,
            alignment_code_src,alignment_code_dsid,
            road_fill_height_m_src,road_fill_height_m_dsid,
            bankfull_width_upstr_a_m_src,bankfull_width_upstr_a_m_dsid,
            bankfull_width_upstr_b_m_src,bankfull_width_upstr_b_m_dsid,
            bankfull_width_upstr_c_m_src,bankfull_width_upstr_c_m_dsid,
            bankfull_width_upstr_avg_m_src,bankfull_width_upstr_avg_m_dsid,
            bankfull_width_dnstr_a_m_src,bankfull_width_dnstr_a_m_dsid,
            bankfull_width_dnstr_b_m_src,bankfull_width_dnstr_b_m_dsid,
            bankfull_width_dnstr_c_m_src,bankfull_width_dnstr_c_m_dsid,
            bankfull_width_dnstr_avg_m_src,bankfull_width_dnstr_avg_m_dsid,
            bankfull_confidence_code_src,bankfull_confidence_code_dsid,
            scour_pool_tailwater_code_src,scour_pool_tailwater_code_dsid,
            crossing_comments_src,crossing_comments_dsid,
            original_point_src,original_point_dsid,
            strahler_order_src,strahler_order_dsid, 
            assessment_type_code_src,assessment_type_code_dsid,
            addressed_status_code_src,addressed_status_code_dsid)
    select
        site_cabd_id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            'a', id,
            strahler_order_src, strahler_order_dsid,
            'a', id,
            'a', id
    from allassessmentdata;

    --structures
    insert into stream_crossings.structures(structure_id,site_id,
                cabd_assessment_id,original_assessment_id,primary_structure,
                structure_number,outlet_shape_code,structure_material_code,internal_structures_code,
                liner_material_code,outlet_armouring_code,outlet_grade_code,outlet_width_m,
                outlet_height_m,outlet_substrate_water_width_m,outlet_water_depth_m,abutment_height_m,
                outlet_drop_to_water_surface_m,outlet_drop_to_stream_bottom_m,outlet_water_surface_to_residual_pool_top_m,residual_pool_confidence_code,
                structure_length_m,inlet_shape_code,inlet_type_code,inlet_grade_code,
                inlet_width_m,inlet_height_m,inlet_substrate_water_width_m,inlet_water_depth_m,
                structure_slope_pct,structure_slope_method_code,structure_slope_to_channel_code,substrate_type_code,
                substrate_matches_stream_code,substrate_coverage_code,substrate_depth_consistent_code,backwatered_pct_code,
                physical_blockages_code,physical_blockage_severity_code,water_depth_matches_stream_code,water_velocity_matches_stream_code,
                dry_passage_code,height_above_dry_passage_m,structure_comments,passability_status_code) 
    select
        uuid_generate_v4(), --structure_id
        a.site_cabd_id, --site_id
        a.cabd_assessment_id, --cabd_assessment_id
        a.original_assessment_id, -- original_assessment_id
        
		case when b.structure_number = 1 then true else false end, --primary_structure
        b.structure_number, --structure number
        b.outlet_shape_code, 
        b.structure_material_code,
        b.internal_structures_code,
        b.liner_material_code,
        b.outlet_armouring_code,
        b.outlet_grade_code,
        b.outlet_width_m,
        b.outlet_height_m,
        b.outlet_substrate_water_width_m,
        b.outlet_water_depth_m,
        b.abutment_height_m,
        b.outlet_drop_to_water_surface_m,
        b.outlet_drop_to_stream_bottom_m,
        b.outlet_water_surface_to_residual_pool_top_m,
        b.residual_pool_confidence_code,
        b.structure_length_m,
        b.inlet_shape_code,
        b.inlet_type_code,
        b.inlet_grade_code,
        b.inlet_width_m,
        b.inlet_height_m,
        b.inlet_substrate_water_width_m,
        b.inlet_water_depth_m,
        b.structure_slope_pct,
        b.structure_slope_method_code,
        b.structure_slope_to_channel_code,
        b.substrate_type_code,
        b.substrate_matches_stream_code,
        b.substrate_coverage_code,
        b.substrate_depth_consistent_code,
        b.backwatered_pct_code,
        b.physical_blockages_code,
        b.physical_blockage_severity_code,
        b.water_depth_matches_stream_code,
        b.water_velocity_matches_stream_code,
        b.dry_passage_code,
        b.height_above_dry_passage_m,
        b.structure_comments,
        b.passability_status_code
    from allassessmentdata a join stream_crossings.assessment_structure_data b on a.id = b.assessment_id;

    --insert datasource details
    insert into stream_crossings.structures_attribute_source (
 		structure_id,
        cabd_assessment_id_src, cabd_assessment_id_dsid,
        original_assessment_id_src,original_assessment_id_dsid,
        primary_structure_src,primary_structure_dsid,
        structure_number_src,structure_number_dsid,
        outlet_shape_code_src,outlet_shape_code_dsid,
        structure_material_code_src,structure_material_code_dsid,
        internal_structures_code_src,internal_structures_code_dsid,
        liner_material_code_src,liner_material_code_dsid,
        outlet_armouring_code_src,outlet_armouring_code_dsid,
        outlet_grade_code_src,outlet_grade_code_dsid,
        outlet_width_m_src,outlet_width_m_dsid,
        outlet_height_m_src,outlet_height_m_dsid,
        outlet_substrate_water_width_m_src,outlet_substrate_water_width_m_dsid,
        outlet_water_depth_m_src,outlet_water_depth_m_dsid,
        abutment_height_m_src,abutment_height_m_dsid,
        outlet_drop_to_water_surface_m_src,outlet_drop_to_water_surface_m_dsid,
        outlet_drop_to_stream_bottom_m_src,outlet_drop_to_stream_bottom_m_dsid,
        outlet_water_surface_to_residual_pool_top_m_src,outlet_water_surface_to_residual_pool_top_m_dsid,
        residual_pool_confidence_code_src,residual_pool_confidence_code_dsid,
        structure_length_m_src,structure_length_m_dsid,
        inlet_shape_code_src,inlet_shape_code_dsid,
        inlet_type_code_src,inlet_type_code_dsid,
        inlet_grade_code_src,inlet_grade_code_dsid,
        inlet_width_m_src,inlet_width_m_dsid,
        inlet_height_m_src,inlet_height_m_dsid,
        inlet_substrate_water_width_m_src,inlet_substrate_water_width_m_dsid,
        inlet_water_depth_m_src,inlet_water_depth_m_dsid,
        structure_slope_pct_src,structure_slope_pct_dsid,
        structure_slope_method_code_src,structure_slope_method_code_dsid,
        structure_slope_to_channel_code_src,structure_slope_to_channel_code_dsid,
        substrate_type_code_src,substrate_type_code_dsid,
        substrate_matches_stream_code_src,substrate_matches_stream_code_dsid,
        substrate_coverage_code_src,substrate_coverage_code_dsid,
        substrate_depth_consistent_code_src,substrate_depth_consistent_code_dsid,
        backwatered_pct_code_src,backwatered_pct_code_dsid,
        physical_blockages_code_src,physical_blockages_code_dsid,
        physical_blockage_severity_code_src,physical_blockage_severity_code_dsid,
        water_depth_matches_stream_code_src,water_depth_matches_stream_code_dsid,
        water_velocity_matches_stream_code_src,water_velocity_matches_stream_code_dsid,
        dry_passage_code_src,dry_passage_code_dsid,
        height_above_dry_passage_m_src,height_above_dry_passage_m_dsid,
        structure_comments_src,structure_comments_dsid,
        passability_status_code_src,passability_status_code_dsid
    )
    select 
        b.structure_id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id,
        'a',a.id
	from stream_crossings.structures b join allassessmentdata a on a.site_cabd_id = b.site_id;
	
	update stream_crossings.assessment_data set status = 'PROCESSED' where status = 'NEW';

	drop table allassessmentdata;



    -- populate chu fields
    with matches as (	
        select a.cabd_id, b.id, b.chu_12_id, b.chu_10_id, b.chu_8_id, b.chu_6_id, b.chu_4_id, b.chu_2_id
        from stream_crossings.sites a, cabd.chu b
        where st_intersects(st_transform(a.original_point, 3979), b.geom)
    )
    update stream_crossings.sites set 
        chu_12_id = a.chu_12_id,
        chu_10_id = a.chu_10_id,
        chu_8_id = a.chu_8_id,
        chu_6_id = a.chu_6_id,
        chu_4_id = a.chu_4_id,
        chu_2_id = a.chu_2_id
    from matches a where a.cabd_id = sites.cabd_id;
END;
$$;
