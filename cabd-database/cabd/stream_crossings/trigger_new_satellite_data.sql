-- trigger to apply new satellite data to sites and structures

CREATE OR REPLACE FUNCTION stream_crossings.cwf_satellite_data_data_trg() 
RETURNS TRIGGER  
LANGUAGE plpgsql 
as $$
DECLARE
  dampoint geometry;
  doupdate boolean;
  STREAM_SNAP_TOLERANCE integer := 500; --for snapping points to stream network chyf and nhn

BEGIN

    -- set enum to null if 'NULL' option selected
    if (NEW.new_crossing_type = 'NULL') then
        NEW.new_crossing_type = NULL;
    end if;

    -- revert changes made to columns which should not be edited
    NEW.id = OLD.id;
    NEW.crossing_type = OLD.crossing_type;

    --sites data to update
    if (NEW.status != 'REVIEWED') then
        return NEW;
    end if;

    if (NEW.new_crossing_type is not null and NEW.new_crossing_type = 'dam') then
        --this is a dam
    
	    --remove dams from site/structures tables
	    delete from stream_crossings.structures_attribute_source where structure_id in (select structure_id from stream_crossings.structures a where a.site_id = NEW.cabd_id);
        delete from stream_crossings.sites_attribute_source where cabd_id = NEW.cabd_id;
        delete from stream_crossings.structures where site_id = NEW.cabd_id;
        delete from stream_crossings.sites where cabd_id = NEW.cabd_id;

        dampoint := st_transform(st_setsrid(st_makepoint(NEW.new_dam_longitude, NEW.new_dam_latitude), 4326), 4617);
    	--add to dams
        insert into dams.dams (cabd_id, assessment_type_code, 
            province_territory_code, nhn_watershed_id, 
            original_point, snapped_point, snapped_ncc)
        values (NEW.cabd_id, 4, 
            cabd.find_province_territory_code(dampoint),
            cabd.find_nhn_watershed_id(dampoint),
		    dampoint,
		    cabd.snap_point_to_chyf_network(dampoint, STREAM_SNAP_TOLERANCE),
		    cabd.snap_point_to_nhn_network(dampoint, STREAM_SNAP_TOLERANCE)
        );
    
        insert into dams.dams_attribute_source(cabd_id, assessment_type_code_ds, original_point_ds)
        select NEW.cabd_id, id, id from cabd.data_source where name = 'cwf'; 

        update stream_crossings.cwf_satellite_review set status = 'PROCESSED' where id = NEW.id;

    else    
        --this is not a dam; update attributes where appropriate

        IF (NEW.driveway_crossing is not null) then            
            select case 
				when road_type_code_src is null then true
				when (road_type_code_src in ('m') or (road_type_code_src = 's' and b.date_of_review < NEW.date_of_review)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join stream_crossings.cwf_satellite_review b on b.id = a.road_type_code_dsid
            where a.cabd_id = NEW.cabd_id; 
			
            if (doupdate) then
                update stream_crossings.sites set road_type_code = case when NEW.driveway_crossing is not null and NEW.driveway_crossing then 4 else null end where cabd_id = NEW.cabd_id;
                update stream_crossings.sites_attribute_source set road_type_code_src = 's', road_type_code_dsid = NEW.id where cabd_id = NEW.cabd_id;
            end if;
        end if;

        if (NEW.crossing_type_code is not null) then 
            select case 
				when crossing_type_code_src is null then true
				when (crossing_type_code_src in ('m') or (crossing_type_code_src = 's' and b.date_of_review < NEW.date_of_review)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join stream_crossings.cwf_satellite_review b on b.id = a.crossing_type_code_dsid
            where a.cabd_id = NEW.cabd_id;
			
            if (doupdate) then
                update stream_crossings.sites set crossing_type_code = NEW.crossing_type_code where cabd_id = NEW.cabd_id;
                update stream_crossings.sites_attribute_source set crossing_type_code_src = 's', crossing_type_code_dsid = NEW.id where cabd_id = NEW.cabd_id;
            end if;
        end if;

        if (new.reviewer_comments is not null) then 
            select case 
				when crossing_comments_src is null then true
				when (crossing_comments_src in ('m') or (crossing_comments_src = 's' and b.date_of_review < NEW.date_of_review)) then true 
				else false end  into doupdate
			from stream_crossings.sites_attribute_source a 
				left join stream_crossings.cwf_satellite_review b on b.id = a.crossing_comments_dsid
            where a.cabd_id = NEW.cabd_id;
			
            if (doupdate) then
                update stream_crossings.sites set crossing_comments = NEW.reviewer_comments where cabd_id = NEW.cabd_id;
                update stream_crossings.sites_attribute_source set crossing_comments_src = 's', crossing_comments_dsid = NEW.id where cabd_id = NEW.cabd_id;
            end if;
        end if;

        
        select case 
			when assessment_type_code_src is null then true
			when (assessment_type_code_src in ('m') or (assessment_type_code_src = 's' and b.date_of_review < NEW.date_of_review)) then true 
			else false end  into doupdate
		from stream_crossings.sites_attribute_source a 
			left join stream_crossings.cwf_satellite_review b on b.id = a.assessment_type_code_dsid
        where a.cabd_id = NEW.cabd_id;
			
        if (doupdate) then
            update stream_crossings.sites set assessment_type_code = 4 where cabd_id = NEW.cabd_id;
            update stream_crossings.sites_attribute_source set assessment_type_code_src = 's', assessment_type_code_dsid = NEW.id where cabd_id = NEW.cabd_id;
        end if;
        
        update stream_crossings.cwf_satellite_review set status = 'PROCESSED' where id = NEW.id;
    end if ;
	RETURN NEW;
END;
$$;



CREATE OR REPLACE TRIGGER cwf_satellite_data_data_trg
AFTER INSERT OR UPDATE ON stream_crossings.cwf_satellite_review
FOR EACH ROW
WHEN (NEW.status = 'REVIEWED')
EXECUTE FUNCTION stream_crossings.cwf_satellite_data_data_trg();
