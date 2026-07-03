-- DROP FUNCTION cabd.audit_structure_id_delete();

CREATE OR REPLACE FUNCTION cabd.audit_structure_id_nosite_delete()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, id_pk, oldvalues) VALUES('DELETE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, null, OLD.structure_id, to_jsonb(OLD)::jsonb);
  RETURN NEW;
END;
$function$
;


CREATE OR REPLACE FUNCTION cabd.audit_structure_id_nosite_insert()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, id_pk, newvalues) VALUES('INSERT', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, null, NEW.structure_id, to_jsonb(NEW)::jsonb);
  RETURN NEW;
END;
$function$
;


-- DROP FUNCTION cabd.audit_structure_id_update();

CREATE OR REPLACE FUNCTION cabd.audit_structure_id_nosite_update()
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
    INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, id_pk, oldvalues, newvalues) VALUES('UPDATE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, null, NEW.structure_id, oldvalues, newvalues);
  end if;
  
  RETURN NEW;
END;
$function$
;

drop trigger structures_attribute_source_delete_trg on stream_crossings.structures_attribute_source ;
drop trigger structures_attribute_source_insert_trg on stream_crossings.structures_attribute_source ;
drop trigger structures_attribute_source_update_trg on stream_crossings.structures_attribute_source ;


create trigger structures_attribute_source_delete_trg after
delete
    on
    stream_crossings.structures_attribute_source for each row execute function cabd.audit_structure_id_nosite_delete();

create trigger structures_attribute_source_insert_trg after
insert
    on
    stream_crossings.structures_attribute_source for each row execute function cabd.audit_structure_id_nosite_insert();

create trigger structures_attribute_source_update_trg after
update
    on
    stream_crossings.structures_attribute_source for each row execute function cabd.audit_structure_id_nosite_update();