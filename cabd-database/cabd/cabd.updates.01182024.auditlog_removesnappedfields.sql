-- DROP FUNCTION cabd.audit_cabdid_update();

alter table cabd.contacts add column mailing_list boolean not null default false;
alter table cabd.contacts alter column mailing_list set default false;

CREATE OR REPLACE FUNCTION cabd.audit_cabdid_update()
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
    INSERT INTO cabd.audit_log(action, schemaname, tablename, cabd_id, oldvalues, newvalues) VALUES('UPDATE', TG_TABLE_SCHEMA::text, TG_TABLE_NAME::text, NEW.cabd_id, oldvalues, newvalues);
  end if;
  
  RETURN NEW;
END;
$function$
;
