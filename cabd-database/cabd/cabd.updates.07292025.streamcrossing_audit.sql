---- !! AUDIT LOG TRIGGERS !! ----

-- create audit log triggers for sites and sites_attribute_source tables
create trigger sites_delete_trg after delete on stream_crossings.sites for each row execute function cabd.audit_cabdid_delete();
create trigger sites_insert_trg after insert on stream_crossings.sites for each row execute function cabd.audit_cabdid_insert();
create trigger sites_update_trg after update on stream_crossings.sites for each row execute function cabd.audit_cabdid_update();

create trigger sites_attribute_source_delete_trg after delete on stream_crossings.sites_attribute_source for each row execute function cabd.audit_cabdid_delete();
create trigger sites_attribute_source_insert_trg after insert on stream_crossings.sites_attribute_source for each row execute function cabd.audit_cabdid_insert();
create trigger sites_attribute_source_update_trg after update on stream_crossings.sites_attribute_source for each row execute function cabd.audit_cabdid_update();

-- create audit log functions for tracking structure_id for structures tables
CREATE OR REPLACE FUNCTION cabd.audit_structure_id_insert()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, id_pk, newvalues) VALUES('INSERT', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.site_id, NEW.structure_id, to_jsonb(NEW)::jsonb);
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION cabd.audit_structure_id_delete()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, id_pk, oldvalues) VALUES('DELETE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, OLD.site_id, OLD.structure_id, to_jsonb(OLD)::jsonb);
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION cabd.audit_structure_id_update()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

DECLARE
  js_new jsonb := row_to_json(NEW)::jsonb;
  js_old jsonb := row_to_json(OLD)::jsonb;
  oldvalues jsonb;
  newvalues jsonb;
 
begin

  -- remove last modified, snapped, snapped_ncc from the fields
  -- so these don't report changes
  js_old :=  js_old #- '{last_modified}' #- '{snapped_ncc}' #- '{snapped_point}';
  js_new :=  js_new #- '{last_modified}' #- '{snapped_ncc}' #- '{snapped_point}';

  -- get old and new values
  oldvalues := js_old - js_new;
  newvalues := js_new - js_old;

  if (oldvalues <> '{}' OR newvalues <> '{}') then
    --log change only if there is a change;
    INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, id_pk, oldvalues, newvalues) VALUES('UPDATE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.site_id, NEW.structure_id, oldvalues, newvalues);
  end if;
  
  RETURN NEW;
END;
$function$
;

-- create audit log triggers for sites and sites_attribute_source tables
create trigger structures_delete_trg after delete on stream_crossings.structures for each row execute function cabd.audit_structure_id_delete();
create trigger structures_insert_trg after insert on stream_crossings.structures for each row execute function cabd.audit_structure_id_insert();
create trigger structures_update_trg after update on stream_crossings.structures for each row execute function cabd.audit_structure_id_update();

create trigger structures_attribute_source_delete_trg after delete on stream_crossings.structures_attribute_source for each row execute function cabd.audit_structure_id_delete();
create trigger structures_attribute_source_insert_trg after insert on stream_crossings.structures_attribute_source for each row execute function cabd.audit_structure_id_insert();
create trigger structures_attribute_source_update_trg after update on stream_crossings.structures_attribute_source for each row execute function cabd.audit_structure_id_update();



---- !! VECTOR TILE CACHE !! ----

--fix schema of existing function
ALTER FUNCTION public.clear_cache set schema cabd;

--create a function to clear by type
-- NOTE: we may want to test the performance of this; it might be better to just use
-- clear_cache to clear the entire cache
CREATE OR REPLACE FUNCTION cabd.clear_cache_by_type()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
begin
delete from cabd.vector_tile_cache where key like  TG_ARGV[0] || '_%';
return new;
end;
$function$
;


--TODO if sites/structures are going to be represented by different feature types you'll want to change the type here
create trigger trg_clear_vector_cache_sites after insert or delete or update on stream_crossings.sites for each statement execute function cabd.clear_cache_by_type('sites');
create trigger trg_clear_vector_cache_sites_structures after insert or delete or update on stream_crossings.sites for each statement execute function cabd.clear_cache_by_type('structures');

create trigger trg_clear_vector_cache_structures after insert or delete or update on stream_crossings.structures for each statement execute function cabd.clear_cache_by_type('structures');
create trigger trg_clear_vector_cache_structures_sites after insert or delete or update on stream_crossings.structures for each statement execute function cabd.clear_cache_by_type('sites');


---- !! LAST MODIFIED TRIGGERS !! ----
create trigger sites_last_modified_insert_trg before insert on stream_crossings.sites for each row execute function cabd.updatelastmodified();
create trigger structures_last_modified_insert_trg before insert on stream_crossings.structures for each row execute function cabd.updatelastmodified();
create trigger structures_last_modified_update_trg before update on stream_crossings.structures for each row when ((old.* <> new.*)) execute function cabd.updatelastmodified();

--this one is special because you can't use old.* <> new.* with a table that has a generated_column
-- and sites has a generated column
-- if we add a column to this table we need to update this trigger
CREATE TRIGGER sites_last_modified_update_trg
BEFORE UPDATE ON stream_crossings.sites
FOR EACH ROW
WHEN (
  OLD.other_id IS DISTINCT FROM NEW.other_id OR
  OLD.cabd_assessment_id IS DISTINCT FROM NEW.cabd_assessment_id OR
  OLD.original_assessment_id IS DISTINCT FROM NEW.original_assessment_id OR
  OLD.date_assessed IS DISTINCT FROM NEW.date_assessed OR
  OLD.lead_assessor IS DISTINCT FROM NEW.lead_assessor OR
  OLD.municipality IS DISTINCT FROM NEW.municipality OR
  OLD.stream_name IS DISTINCT FROM NEW.stream_name OR
  OLD.road_name IS DISTINCT FROM NEW.road_name OR
  OLD.road_type_code IS DISTINCT FROM NEW.road_type_code OR
  OLD.location_description IS DISTINCT FROM NEW.location_description OR
  OLD.land_ownership_context IS DISTINCT FROM NEW.land_ownership_context OR
  OLD.incomplete_assess_code IS DISTINCT FROM NEW.incomplete_assess_code OR
  OLD.crossing_type_code IS DISTINCT FROM NEW.crossing_type_code OR
  OLD.num_structures IS DISTINCT FROM NEW.num_structures OR
  OLD.photo_id_inlet IS DISTINCT FROM NEW.photo_id_inlet OR
  OLD.photo_id_outlet IS DISTINCT FROM NEW.photo_id_outlet OR
  OLD.photo_id_upstream IS DISTINCT FROM NEW.photo_id_upstream OR
  OLD.photo_id_downstream IS DISTINCT FROM NEW.photo_id_downstream OR
  OLD.photo_id_road_surface IS DISTINCT FROM NEW.photo_id_road_surface OR
  OLD.photo_id_other_a IS DISTINCT FROM NEW.photo_id_other_a OR
  OLD.photo_id_other_b IS DISTINCT FROM NEW.photo_id_other_b OR
  OLD.photo_id_other_c IS DISTINCT FROM NEW.photo_id_other_c OR
  OLD.flow_condition_code IS DISTINCT FROM NEW.flow_condition_code OR
  OLD.crossing_condition_code IS DISTINCT FROM NEW.crossing_condition_code OR
  OLD.site_type_code IS DISTINCT FROM NEW.site_type_code OR
  OLD.alignment_code IS DISTINCT FROM NEW.alignment_code OR
  OLD.road_fill_height_m IS DISTINCT FROM NEW.road_fill_height_m OR
  OLD.bankfull_width_upstr_a_m IS DISTINCT FROM NEW.bankfull_width_upstr_a_m OR
  OLD.bankfull_width_upstr_b_m IS DISTINCT FROM NEW.bankfull_width_upstr_b_m OR
  OLD.bankfull_width_upstr_c_m IS DISTINCT FROM NEW.bankfull_width_upstr_c_m OR
  OLD.bankfull_width_upstr_avg_m IS DISTINCT FROM NEW.bankfull_width_upstr_avg_m OR
  OLD.bankfull_width_dnstr_a_m IS DISTINCT FROM NEW.bankfull_width_dnstr_a_m OR
  OLD.bankfull_width_dnstr_b_m IS DISTINCT FROM NEW.bankfull_width_dnstr_b_m OR
  OLD.bankfull_width_dnstr_c_m IS DISTINCT FROM NEW.bankfull_width_dnstr_c_m OR
  OLD.bankfull_width_dnstr_avg_m IS DISTINCT FROM NEW.bankfull_width_dnstr_avg_m OR
  OLD.bankfull_confidence_code IS DISTINCT FROM NEW.bankfull_confidence_code OR
  OLD.scour_pool_tailwater_code IS DISTINCT FROM NEW.scour_pool_tailwater_code OR
  OLD.crossing_comments IS DISTINCT FROM NEW.crossing_comments OR
  OLD.original_point IS DISTINCT FROM NEW.original_point OR
  OLD.snapped_point IS DISTINCT FROM NEW.snapped_point OR
  OLD.snapped_ncc IS DISTINCT FROM NEW.snapped_ncc OR
  OLD.province_territory_code IS DISTINCT FROM NEW.province_territory_code OR
  OLD.nhn_watershed_id IS DISTINCT FROM NEW.nhn_watershed_id OR
  OLD.strahler_order IS DISTINCT FROM NEW.strahler_order OR
  OLD.assessment_type_code IS DISTINCT FROM NEW.assessment_type_code OR
  OLD.addressed_status_code IS DISTINCT FROM NEW.addressed_status_code OR
  OLD.chu_12_id IS DISTINCT FROM NEW.chu_12_id OR
  OLD.chu_10_id IS DISTINCT FROM NEW.chu_10_id OR
  OLD.chu_8_id IS DISTINCT FROM NEW.chu_8_id OR
  OLD.chu_6_id IS DISTINCT FROM NEW.chu_6_id OR
  OLD.chu_4_id IS DISTINCT FROM NEW.chu_4_id OR
  OLD.chu_2_id IS DISTINCT FROM NEW.chu_2_id
)
EXECUTE FUNCTION cabd.updatelastmodified();