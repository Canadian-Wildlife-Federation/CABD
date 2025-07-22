-- trigger to apply new satellite data to sites and structures

CREATE OR REPLACE FUNCTION stream_crossings.assessment_data_trg() 
RETURNS TRIGGER  
LANGUAGE plpgsql 
as $$
DECLARE
  cabdid uuid;
  structureid uuid;
  rec RECORD;
  cnt integer;
  distance double precision;
  doupdate boolean;
  last_date_assessed date;
  newpoint geometry;
  addressed_status_code_value integer;
  STREAM_SNAP_TOLERANCE integer := 500; --for snapping points to stream network chyf and nhn
  CABD_FEATURE_DISANCE_MATCH_TOLDERANCE integer:= 100; --maximum feature matching distance in m

BEGIN

    --sites data to update
    if (NEW.status != 'NEW') then
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
            update stream_crossings.stream_crossings_community_holding set cabd_id = cabdid where id = NEW.id;
        end if;
	end if;
	
    --compute address status code
    addressed_status_code_value = null;
    if (NEW.crossing_type_code = 6) then
        addressed_status_code_value := 1;
    else 
        select count(*) into cnt from stream_crossings.assessment_structure_data where assessment_id = NEW.id and internal_structures_code = 2;
        if (cnt > 1) then
            addressed_status_code_value := 4;
        else
            select count(*) into cnt from stream_crossings.assessment_structure_data where assessment_id = NEW.id and backwatered_pct_code in (1, 99);
            if (cnt = 0) then
                addressed_status_code_value := 5;
            end if;
        end if;
    end if;


    if (cabdid is not null) THEN

        --if this assessment is newer than the existing assessment then replace all the data otherwise don't replace anything
        select date_assessed into last_date_assessed from stream_crossings.sites where cabd_id = cabdid and cabd_assessment_id is not null;


        if (last_date_assessed is null OR NEW.date_assessed > last_date_assessed) then
            --replace all site and structure data
            update stream_crossings.sites set
                other_id = NEW.other_id,
                cabd_assessment_id = NEW.cabd_assessment_id,
                original_assessment_id = NEW.original_assessment_id,
                date_assessed = NEW.date_assessed,
                lead_assessor = NEW.lead_assessor,
                municipality = NEW.municipality,
                stream_name = NEW.stream_name,
                road_name = NEW.road_name,
                road_type_code = NEW.road_type_code,
                location_description = NEW.location_description,
                land_ownership_context = NEW.land_ownership_context,
                incomplete_assess_code = NEW.incomplete_assess_code,
                crossing_type_code = NEW.crossing_type_code,
                num_structures = NEW.num_structures,
                photo_id_inlet = NEW.photo_id_inlet,
                photo_id_outlet = NEW.photo_id_outlet,
                photo_id_upstream = NEW.photo_id_upstream,
                photo_id_downstream = NEW.photo_id_downstream,
                photo_id_road_surface = NEW.photo_id_road_surface,
	            photo_id_other_a = NEW.photo_id_other_a,
	            photo_id_other_b = NEW.photo_id_other_b,
	            photo_id_other_c = NEW.photo_id_other_c,
	            flow_condition_code = NEW.flow_condition_code,
	            crossing_condition_code = NEW.crossing_condition_code,
	            site_type_code = NEW.site_type_code,
	            alignment_code = NEW.alignment_code,
	            road_fill_height_m = NEW.road_fill_height_m,
	            bankfull_width_upstr_a_m = NEW.bankfull_width_upstr_a_m,
	            bankfull_width_upstr_b_m = NEW.bankfull_width_upstr_b_m,
	            bankfull_width_upstr_c_m = NEW.bankfull_width_upstr_c_m,
	            bankfull_width_upstr_avg_m = NEW.bankfull_width_upstr_avg_m,
	            bankfull_width_dnstr_a_m = NEW.bankfull_width_dnstr_a_m,
	            bankfull_width_dnstr_b_m = NEW.bankfull_width_dnstr_b_m,
	            bankfull_width_dnstr_c_m = NEW.bankfull_width_dnstr_c_m,
	            bankfull_width_dnstr_avg_m = NEW.bankfull_width_dnstr_avg_m,
	            bankfull_confidence_code = NEW.bankfull_confidence_code,
	            scour_pool_tailwater_code = NEW.scour_pool_tailwater_code,
	            crossing_comments = NEW.crossing_comments,
            	original_point = newpoint,        
                snapped_point = cabd.snap_point_to_chyf_network(newpoint, STREAM_SNAP_TOLERANCE),
                snapped_ncc = cabd.snap_point_to_nhn_network(newpoint, STREAM_SNAP_TOLERANCE),                
                province_territory_code =cabd.find_province_territory_code(newpoint),
	            nhn_watershed_id = cabd.find_nhn_watershed_id(newpoint),
	            assessment_type_code = case when NEW.original_assessment_id is null then 2 else 3 end, 
                addressed_status_code =  addressed_status_code_value
            WHERE cabd_id = cabdid;

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

            --update datasources             
            update stream_crossings.sites_attribute_source set
                other_id_src='a',
                other_id_dsid=NEW.id,
                municipality_src='a',
                municipality_dsid=NEW.id,
                stream_name_src='a',
                stream_name_dsid=NEW.id,
                road_name_src='a',
                road_name_dsid=NEW.id,
                road_type_code_src='a',
                road_type_code_dsid=NEW.id,
                location_description_src='a',
                location_description_dsid=NEW.id,
                land_ownership_context_src='a',
                land_ownership_context_dsid=NEW.id,
                incomplete_assess_code_src='a',
                incomplete_assess_code_dsid=NEW.id,
                crossing_type_code_src='a',
                crossing_type_code_dsid=NEW.id,
                num_structures_src='a',
                num_structures_dsid=NEW.id,
                photo_id_inlet_src='a',
                photo_id_inlet_dsid=NEW.id,
                photo_id_outlet_src='a',
                photo_id_outlet_dsid=NEW.id,
                photo_id_upstream_src='a',
                photo_id_upstream_dsid=NEW.id,
                photo_id_downstream_src='a',
                photo_id_downstream_dsid=NEW.id,
                photo_id_road_surface_src='a',
                photo_id_road_surface_dsid=NEW.id,
                photo_id_other_a_src='a',
                photo_id_other_a_dsid=NEW.id,
                photo_id_other_b_src='a',
                photo_id_other_b_dsid=NEW.id,
                photo_id_other_c_src='a',
                photo_id_other_c_dsid=NEW.id,
                flow_condition_code_src='a',
                flow_condition_code_dsid=NEW.id,
                crossing_condition_code_src='a',
                crossing_condition_code_dsid=NEW.id,
                site_type_code_src='a',
                site_type_code_dsid=NEW.id,
                alignment_code_src='a',
                alignment_code_dsid=NEW.id,
                road_fill_height_m_src='a',
                road_fill_height_m_dsid=NEW.id,
                bankfull_width_upstr_a_m_src='a',
                bankfull_width_upstr_a_m_dsid=NEW.id,
                bankfull_width_upstr_b_m_src='a',
                bankfull_width_upstr_b_m_dsid=NEW.id,
                bankfull_width_upstr_c_m_src='a',
                bankfull_width_upstr_c_m_dsid=NEW.id,
                bankfull_width_upstr_avg_m_src='a',
                bankfull_width_upstr_avg_m_dsid=NEW.id,
                bankfull_width_dnstr_a_m_src='a',
                bankfull_width_dnstr_a_m_dsid=NEW.id,
                bankfull_width_dnstr_b_m_src='a',
                bankfull_width_dnstr_b_m_dsid=NEW.id,
                bankfull_width_dnstr_c_m_src='a',
                bankfull_width_dnstr_c_m_dsid=NEW.id,
                bankfull_width_dnstr_avg_m_src='a',
                bankfull_width_dnstr_avg_m_dsid=NEW.id,
                bankfull_confidence_code_src='a',
                bankfull_confidence_code_dsid=NEW.id,
                scour_pool_tailwater_code_src='a',
                scour_pool_tailwater_code_dsid=NEW.id,
                crossing_comments_src='a',
                crossing_comments_dsid=NEW.id,
                original_point_src='a',
                original_point_dsid=NEW.id,
                assessment_type_code_src='a',
                assessment_type_code_dsid=NEW.id,
                addressed_status_code_src='a',
                addressed_status_code_dsid=NEW.id
            where cabd_id = cabdid;
                        
            --structures
            -- we will delete all existing structures and re-create them
            -- they way we ensure and removed structures are deleted 
            -- but it means structure_id won't be retained -> this was deemed to be ok
            delete from stream_crossings.structures_attribute_source where structure_id in (select structure_id from stream_crossings.structures where site_id = cabdid);
            delete from stream_crossings.structures where site_id = cabdid;
            
            FOR rec in 
              select * from stream_crossings.assessment_structure_data where assessment_id = NEW.id
            LOOP
                --add new structure
                structureid := uuid_generate_v4();

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
                values (
                    structureid, --structure_id
                    cabdid, --site_id
                    NEW.cabd_assessment_id, --cabd_assessment_id
                    NEW.original_assessment_id, -- original_assessment_id
                    case when rec.structure_number = 1 then true else false end, --primary_structure
                    rec.structure_number, --structure number
                    rec.outlet_shape_code, 
                    rec.structure_material_code,
                    rec.internal_structures_code,
                    rec.liner_material_code,
                    rec.outlet_armouring_code,
                    rec.outlet_grade_code,
                    rec.outlet_width_m,
                    rec.outlet_height_m,
                    rec.outlet_substrate_water_width_m,
                    rec.outlet_water_depth_m,
                    rec.abutment_height_m,
                    rec.outlet_drop_to_water_surface_m,
                    rec.outlet_drop_to_stream_bottom_m,
                    rec.outlet_water_surface_to_residual_pool_top_m,
                    rec.residual_pool_confidence_code,
                    rec.structure_length_m,
                    rec.inlet_shape_code,
                    rec.inlet_type_code,
                    rec.inlet_grade_code,
                    rec.inlet_width_m,
                    rec.inlet_height_m,
                    rec.inlet_substrate_water_width_m,
                    rec.inlet_water_depth_m,
                    rec.structure_slope_pct,
                    rec.structure_slope_method_code,
                    rec.structure_slope_to_channel_code,
                    rec.substrate_type_code,
                    rec.substrate_matches_stream_code,
                    rec.substrate_coverage_code,
                    rec.substrate_depth_consistent_code,
                    rec.backwatered_pct_code,
                    rec.physical_blockages_code,
                    rec.physical_blockage_severity_code,
                    rec.water_depth_matches_stream_code,
                    rec.water_velocity_matches_stream_code,
                    rec.dry_passage_code,
                    rec.height_above_dry_passage_m,
                    rec.structure_comments,
                    rec.passability_status_code
                );

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
                )values(
                    structureid,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id,
                    'a',NEW.id
                );                
            END LOOP;
        end if; --date assessed
             
        update stream_crossings.assessment_data set status = 'PROCESSED' where id = NEW.id;

    else

        --create a new site
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
        ) VALUES (
            NEW.cabd_id,
            NEW.other_id,
            NEW.cabd_assessment_id,
            NEW.original_assessment_id,
            NEW.date_assessed,
            NEW.lead_assessor,
            NEW.municipality,
            NEW.stream_name,
            NEW.road_name,
            NEW.road_type_code,
            NEW.location_description,
            NEW.land_ownership_context,
            NEW.incomplete_assess_code,
            NEW.crossing_type_code,
            NEW.num_structures,
            NEW.photo_id_inlet,
            NEW.photo_id_outlet,
            NEW.photo_id_upstream,
            NEW.photo_id_downstream,
            NEW.photo_id_road_surface,
            NEW.photo_id_other_a,
            NEW.photo_id_other_b,
            NEW.photo_id_other_c,
            NEW.flow_condition_code,
            NEW.crossing_condition_code,
            NEW.site_type_code,
            NEW.alignment_code,
            NEW.road_fill_height_m,
            NEW.bankfull_width_upstr_a_m,
            NEW.bankfull_width_upstr_b_m,
            NEW.bankfull_width_upstr_c_m,
            NEW.bankfull_width_upstr_avg_m,
            NEW.bankfull_width_dnstr_a_m,
            NEW.bankfull_width_dnstr_b_m,
            NEW.bankfull_width_dnstr_c_m,
            NEW.bankfull_width_dnstr_avg_m,
            NEW.bankfull_confidence_code,
            NEW.scour_pool_tailwater_code,
            NEW.crossing_comments,
            newpoint, --original_point
            cabd.snap_point_to_chyf_network(newpoint, STREAM_SNAP_TOLERANCE),
            cabd.snap_point_to_nhn_network(newpoint, STREAM_SNAP_TOLERANCE),    
            cabd.find_province_territory_code(newpoint),
            cabd.find_nhn_watershed_id(newpoint),            
            NULL, --strahler order
            CASE 
                WHEN NEW.original_assessment_id IS NULL THEN 2 
                ELSE 3 
            END,
            addressed_status_code_value --addressed_status_code            
        );

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
            addressed_status_code_src,addressed_status_code_dsid
        ) values (
            NEW.cabd_id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id,
            'a', NEW.id
        );

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
                NEW.cabd_id, --site_id
                NEW.cabd_assessment_id, --cabd_assessment_id
                NEW.original_assessment_id, -- original_assessment_id
                case when structure_number = 1 then true else false end, --primary_structure
                structure_number, --structure number
                outlet_shape_code, 
                structure_material_code,
                internal_structures_code,
                liner_material_code,
                outlet_armouring_code,
                outlet_grade_code,
                outlet_width_m,
                outlet_height_m,
                outlet_substrate_water_width_m,
                outlet_water_depth_m,
                abutment_height_m,
                outlet_drop_to_water_surface_m,
                outlet_drop_to_stream_bottom_m,
                outlet_water_surface_to_residual_pool_top_m,
                residual_pool_confidence_code,
                structure_length_m,
                inlet_shape_code,
                inlet_type_code,
                inlet_grade_code,
                inlet_width_m,
                inlet_height_m,
                inlet_substrate_water_width_m,
                inlet_water_depth_m,
                structure_slope_pct,
                structure_slope_method_code,
                structure_slope_to_channel_code,
                substrate_type_code,
                substrate_matches_stream_code,
                substrate_coverage_code,
                substrate_depth_consistent_code,
                backwatered_pct_code,
                physical_blockages_code,
                physical_blockage_severity_code,
                water_depth_matches_stream_code,
                water_velocity_matches_stream_code,
                dry_passage_code,
                height_above_dry_passage_m,
                structure_comments,
                passability_status_code
            from stream_crossings.assessment_structure_data where assessment_id = NEW.id;

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
                structure_id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id,
                'a',NEW.id
        FROM stream_crossings.structures where site_id = NEW.cabd_id;
        
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

	    update stream_crossings.assessment_data set status = 'PROCESSED' where id = NEW.id;

    END IF;
	RETURN NEW;
END;
$$;



CREATE OR REPLACE TRIGGER stream_crossings_assessment_data_trg
AFTER INSERT OR UPDATE ON stream_crossings.assessment_data
FOR EACH ROW
WHEN (NEW.status = 'NEW')
EXECUTE FUNCTION stream_crossings.assessment_data_trg();
