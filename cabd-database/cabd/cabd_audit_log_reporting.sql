--create a version_history table
--I've populated it with some random dates but the assumption
--is that the first version has a start date of null, and the current version
--has an end date of null
create table cabd.feature_type_version_history(type varchar(32) references cabd.feature_types(type), version varchar, start_date date, end_date date);
insert into cabd.feature_type_version_history values ('dams', '1.0', null, '2023-04-04');
insert into cabd.feature_type_version_history values ('dams', '1.1', '2023-04-05', null);
insert into cabd.feature_type_version_history values ('waterfalls', '1.0', null, null);
insert into cabd.feature_type_version_history values ('fishways', '1.0', null, null);
insert into cabd.feature_type_version_history values ('stream_crossings', '1.0', null, null);

COMMENT on TABLE cabd.feature_type_version_history IS 'Tracks the version number for each feature type by date range. Start and end dates are inclusive so the start date of the next version should be one day after the end date of the current version';

--update the feature types table to add the data table associated with it
--this just makes the reporting a bit easier and less feature-type specific
drop view if exists cabd.dams_audit_log_vw;
drop view if exists cabd.fishways_audit_log_vw;
drop view if exists cabd.waterfalls_audit_log_vw;
drop view if exists cabd.feature_insert_version;

alter table cabd.feature_types drop column if exists data_table ;
alter table cabd.feature_types add column data_table varchar[];
update cabd.feature_types set data_table = '{"dams.dams"}' where type in ('dams', 'dams_ncc');
update cabd.feature_types set data_table = '{"waterfalls.waterfalls"}' where type in ('waterfalls', 'waterfalls_ncc');
update cabd.feature_types set data_table = '{"fishways.fishways"}' where type in ('fishways', 'fishways_ncc');
update cabd.feature_types set data_table = '{"stream_crossings.non_tidal_sites","stream_crossings.tidal_sites"}' where type in ('stream_crossings', 'stream_crossings_ncc');

--this view determines what version each cabd feature was inserted into the database
-- if there is no insert statement in the audit log we assume the feature was inserted
--in the 'first' version (the row from the feature_type_version_history with a null start date)
drop view if exists cabd.feature_insert_version;
create view cabd.feature_insert_version
as
with featuretypeversions as 
(
select type, version, 
  case when start_date is null then '1990-01-01' else start_date end as start_date, 
  case when end_date is null then now() else end_date end as end_date
from cabd.feature_type_version_history
),
insertversions as (
--audit log insert versions
select cabd_id, datetime, t.type, v.version
from cabd.audit_log a 
join cabd.feature_types t on a.schemaname || '.' || a.tablename  = ANY (t.data_table)
join featuretypeversions v on v.type = t.type and cast(a.datetime as date) between v.start_date and v.end_date
where a.action = 'INSERT'
)
select * from insertversions
union
--get all the other features and find the first version for them
select f.cabd_id, null, v.type, v.version
from cabd.all_features_view_en f join cabd.feature_type_version_history v 
on f.feature_type = v.type and v.start_date is null 
and f.cabd_id not in (select s.cabd_id from insertversions s);

COMMENT on VIEW cabd.feature_insert_version IS 'The version each feature was inserted into the database';


-- this view determines the date/time each datasource was the valid source
-- for a given feature attribute  
-- this is done by ordering the changes and assuming the valid date is from the
-- date of the change to the date of the next change
-- the enddt in this view is exclusive, the startdt is inclusive
drop view if exists cabd.feature_attribute_datasource_history;
create view cabd.feature_attribute_datasource_history
as 
with changeset as (
select 
  a.cabd_id, 
  a.datetime as startdt, 
  p.id as field, 
  p.data as new_datasource, 
  oldvalues->>p.id as old_datasource, 
  a.action, 
  row_number() over (order by cabd_id, p.id, a.datetime) as sortorder
from cabd.audit_log a
cross join jsonb_each_text(newvalues) as p(id,data)
where schemaname || '.' || tablename in (select attribute_source_table from cabd.feature_types ft )
and p.data is not null 
order by cabd_id, p.id, a.datetime
)
select 
  a.cabd_id as cabd_id, 
  a.field as field_name,
  cast(a.new_datasource as uuid) as new_datasource_id, 
  cast(a.old_datasource as uuid) as old_datasource_id, 
  a.startdt as start_datetime,
  case when b.startdt is null then now()::timestamp without time zone else b.startdt::timestamp without time zone end as end_datetime,  
  a.action
from changeset a 
left join changeset b on a.cabd_id = b.cabd_id and a.field = b.field and a.sortorder = b.sortorder-1;


-- the view for dams
drop view if exists cabd.dams_audit_log_vw;
create or replace view cabd.dams_audit_log_vw as
with dams_audit_log as (
--insert
select l.cabd_id, l.revision, l.datetime, l.action,p.id as field_name, newvalues->p.id as newvalue, cast(null as jsonb) as oldvalue
from 
cabd.audit_log l
cross join jsonb_each_text(newvalues) as p(id,data)
where tablename  = 'dams' and schemaname ='dams' and action = 'INSERT' 
and p.data is not null
and p.id not in ( 'last_modified', 'snapped_ncc', 'snapped_point')
union 
--update
select l.cabd_id, l.revision, l.datetime, l.action, p.id as field_name, newvalues->p.id as newvalue, oldvalues->p.id as oldvalue
from 
cabd.audit_log l
cross join jsonb_each_text(oldvalues) as p(id,data) 
where tablename  = 'dams' and schemaname ='dams' and action = 'UPDATE' 
and p.id not in ( 'last_modified', 'snapped_ncc', 'snapped_point')
union
--delete
select l.cabd_id, l.revision, l.datetime, l.action, null as field_name, null as newvalue, null as oldvalue
from cabd.audit_log l
where tablename  = 'dams' and schemaname ='dams' and action = 'DELETE'
),
featuretypeversions as 
(
select type, version, 
  case when start_date is null then '1990-01-01' else start_date end as start_date, 
  case when end_date is null then now() else end_date end as end_date
from cabd.feature_type_version_history 
)
select 
   l.revision, 
   l.datetime, 
   case 
	  when l.action = 'INSERT' and l.field_name = 'cabd_id' then 'INSERT'
	  when l.action = 'INSERT' and iv.version = v.version then 'NEW_POINT_UPDATE' 
	  when l.action = 'INSERT' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
	  when l.action = 'UPDATE' and iv.version = v.version then 'NEW_POINT_UPDATE'
	  when l.action = 'UPDATE' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
	  else l.action 
	end as action,
    l.cabd_id,  
    iv.type as feature_type, 
    l.field_name as field_name, 
	case 
	   when oc.name_en is not null then oc.name_en 
	   when up.name_en is not null then up.name_en
	   when ps.name_en is not null then ps.name_en
	   when st.name_en is not null then st.name_en
	   when cm.name_en is not null then cm.name_en
	   when sc.name_en is not null then sc.name_en
	   when sw.name_en is not null then sw.name_en
	   when tt.name_en is not null then tt.name_en
	   when dr.name_en is not null then dr.name_en
	   when cl.name_en is not null then cl.name_en
	   when lc.name_en is not null then lc.name_en
	   when os.name_en is not null then os.name_en
	   when du.name_en is not null then du.name_en
	   when uc1.name_en is not null then uc1.name_en
	   when uc2.name_en is not null then uc2.name_en
	   when uc3.name_en is not null then uc3.name_en
	   when uc4.name_en is not null then uc4.name_en
	   when uc5.name_en is not null then uc5.name_en
	   when uc6.name_en is not null then uc6.name_en
	   when uc7.name_en is not null then uc7.name_en
	   when uc8.name_en is not null then uc8.name_en
	   when uc9.name_en is not null then uc9.name_en
	   when uc10.name_en is not null then uc10.name_en
	   when uc11.name_en is not null then uc11.name_en
	   when fc.name_en is not null then fc.name_en
  	   when cc.name_en is not null then cc.name_en
	   when cast(newvalue as varchar) = 'null' then null
	   else cast(newvalue as varchar) 
	end as new_value,
    dsnew.name as new_datasource_name, 		
    case 
	   when oco.name_en is not null then oco.name_en 
   	   when upo.name_en is not null then upo.name_en 
	   when pso.name_en is not null then pso.name_en
	   when sto.name_en is not null then sto.name_en
	   when cmo.name_en is not null then cmo.name_en
 	   when sco.name_en is not null then sco.name_en
       when swo.name_en is not null then swo.name_en
	   when tto.name_en is not null then tto.name_en
	   when dro.name_en is not null then dro.name_en
	   when clo.name_en is not null then clo.name_en
	   when lco.name_en is not null then lco.name_en
	   when os.name_en is not null then oso.name_en
	   when duo.name_en is not null then duo.name_en
	   when uc1o.name_en is not null then uc1o.name_en
	   when uc2o.name_en is not null then uc2o.name_en
	   when uc3o.name_en is not null then uc3o.name_en
	   when uc4o.name_en is not null then uc4o.name_en
	   when uc5o.name_en is not null then uc5o.name_en
	   when uc6o.name_en is not null then uc6o.name_en
	   when uc7o.name_en is not null then uc7o.name_en
	   when uc8o.name_en is not null then uc8o.name_en
	   when uc9o.name_en is not null then uc9o.name_en
	   when uc10o.name_en is not null then uc10o.name_en
	   when uc11o.name_en is not null then uc11o.name_en
	   when fco.name_en is not null then fco.name_en
	   when cco.name_en is not null then cco.name_en
	   when cast(oldvalue as varchar) = 'null' then null
	   else cast(oldvalue as varchar) 
	end as old_value,
	
	dsold.name as old_datasource_name,
	v.version,
	newvalue as new_raw_value, 
	oldvalue as old_raw_value,
	iv.version as insert_version,
	h.new_datasource_id as new_datasource_id, 
	h.old_datasource_id as old_datasource_id, 
	l.action as raw_action
from 
dams_audit_log l
left join featuretypeversions v on v.type = 'dams' and cast(l.datetime as date) between v.start_date and v.end_date
left join cabd.feature_insert_version iv on iv.cabd_id = l.cabd_id
left join cabd.feature_attribute_datasource_history h on h.cabd_id = l.cabd_id  and h.field_name  = l.field_name || '_ds' and l.datetime >= h.start_datetime and l.datetime < h.end_datetime
left join cabd.data_source dsnew on h.new_datasource_id = dsnew.id
left join cabd.data_source dsold on h.old_datasource_id = dsold.id

left join cabd.barrier_ownership_type_codes oc on oc.code = case when l.field_name = 'ownership_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join cabd.barrier_ownership_type_codes oco on oco.code = case when l.field_name = 'ownership_type_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join cabd.upstream_passage_type_codes up on up.code = case when l.field_name = 'up_passage_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join cabd.upstream_passage_type_codes upo on upo.code = case when l.field_name = 'up_passage_type_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join cabd.passability_status_codes ps on ps.code = case when l.field_name = 'passability_status_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join cabd.passability_status_codes pso on pso.code = case when l.field_name = 'passability_status_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.structure_type_codes st on st.code = case when l.field_name = 'structure_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.structure_type_codes sto on sto.code = case when l.field_name = 'structure_type_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.construction_material_codes cm on cm.code = case when l.field_name = 'construction_material_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.construction_material_codes cmo on cmo.code = case when l.field_name = 'construction_material_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.size_codes sc on sc.code = case when l.field_name = 'size_class_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.size_codes sco on sco.code = case when l.field_name = 'size_class_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.spillway_type_codes sw on sw.code = case when l.field_name = 'spillway_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.spillway_type_codes swo on swo.code = case when l.field_name = 'spillway_type_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.turbine_type_codes tt on tt.code = case when l.field_name = 'turbine_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.turbine_type_codes tto on tto.code = case when l.field_name = 'turbine_type_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.downstream_passage_route_codes dr on dr.code = case when l.field_name = 'down_passage_route_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.downstream_passage_route_codes dro on dro.code = case when l.field_name = 'down_passage_route_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_complete_level_codes cl on cl.code = case when l.field_name = 'complete_level_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_complete_level_codes clo on clo.code = case when l.field_name = 'complete_level_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.lake_control_codes lc on lc.code = case when l.field_name = 'lake_control_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.lake_control_codes lco on lco.code = case when l.field_name = 'lake_control_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.operating_status_codes os on os.code = case when l.field_name = 'operating_status_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.operating_status_codes oso on oso.code = case when l.field_name = 'operating_status_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes du on du.code = case when l.field_name = 'use_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes duo on duo.code = case when l.field_name = 'use_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc1 on uc1.code = case when l.field_name = 'use_irrigation_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc1o on uc1o.code = case when l.field_name = 'use_irrigation_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc2 on uc2.code = case when l.field_name = 'use_electricity_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc2o on uc2o.code = case when l.field_name = 'use_electricity_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc3 on uc3.code = case when l.field_name = 'use_supply_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc3o on uc3o.code = case when l.field_name = 'use_supply_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc4 on uc4.code = case when l.field_name = 'use_floodcontrol_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc4o on uc4o.code = case when l.field_name = 'use_floodcontrol_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc5 on uc5.code = case when l.field_name = 'use_recreation_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc5o on uc5o.code = case when l.field_name = 'use_recreation_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc6 on uc6.code = case when l.field_name = 'use_navigation_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc6o on uc6o.code = case when l.field_name = 'use_navigation_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc7 on uc7.code = case when l.field_name = 'use_fish_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc7o on uc7o.code = case when l.field_name = 'use_fish_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc8 on uc8.code = case when l.field_name = 'use_pollution_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc8o on uc8o.code = case when l.field_name = 'use_pollution_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc9 on uc9.code = case when l.field_name = 'use_invasivespecies_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc9o on uc9o.code = case when l.field_name = 'use_invasivespecies_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc10 on uc10.code = case when l.field_name = 'use_conservation_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc10o on uc10o.code = case when l.field_name = 'use_conservation_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.dam_use_codes uc11 on uc11.code = case when l.field_name = 'use_other_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.dam_use_codes uc11o on uc11o.code = case when l.field_name = 'use_other_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.function_codes fc on fc.code = case when l.field_name = 'function_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.function_codes fco on fco.code = case when l.field_name = 'function_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join dams.condition_codes cc on cc.code = case when l.field_name = 'condition_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join dams.condition_codes cco on cco.code = case when l.field_name = 'condition_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end
;


-- view for waterfalls --
drop view if exists cabd.waterfalls_audit_log_vw;
create or replace view cabd.waterfalls_audit_log_vw as
with waterfalls_audit_log as (
--insert
select l.cabd_id, l.revision, l.datetime, l.action,p.id as field_name, newvalues->p.id as newvalue, cast(null as jsonb) as oldvalue
from 
cabd.audit_log l
cross join jsonb_each_text(newvalues) as p(id,data)
where tablename  = 'waterfalls' and schemaname ='waterfalls' and action = 'INSERT' 
and p.data is not null
and p.id not in ( 'last_modified', 'snapped_ncc', 'snapped_point')
union 
--update
select l.cabd_id, l.revision, l.datetime, l.action, p.id as field_name, newvalues->p.id as newvalue, oldvalues->p.id as oldvalue
from 
cabd.audit_log l
cross join jsonb_each_text(oldvalues) as p(id,data) 
where tablename  = 'waterfalls' and schemaname ='waterfalls' and action = 'UPDATE' 
and p.id not in ( 'last_modified', 'snapped_ncc', 'snapped_point')
union
--delete
select l.cabd_id, l.revision, l.datetime, l.action, null as field_name, null as newvalue, null as oldvalue
from cabd.audit_log l
where tablename  = 'waterfalls' and schemaname ='waterfalls' and action = 'DELETE'
),
featuretypeversions as 
(
select type, version, 
  case when start_date is null then '1990-01-01' else start_date end as start_date, 
  case when end_date is null then now() else end_date end as end_date
from cabd.feature_type_version_history 
)
select 
   l.revision, 
   l.datetime, 
   case 
	  when l.action = 'INSERT' and l.field_name = 'cabd_id' then 'INSERT'
	  when l.action = 'INSERT' and iv.version = v.version then 'NEW_POINT_UPDATE' 
	  when l.action = 'INSERT' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
	  when l.action = 'UPDATE' and iv.version = v.version then 'NEW_POINT_UPDATE'
	  when l.action = 'UPDATE' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
	  else l.action 
	end as action,
    l.cabd_id,  
    iv.type as feature_type, 
    l.field_name as field_name, 
	case 
	   when ps.name_en is not null then ps.name_en
	   when cl.name_en is not null then cl.name_en
	   when tc.name_en is not null then tc.name_en
	   when cast(newvalue as varchar) = 'null' then null
	   else cast(newvalue as varchar) 
	end as new_value,
    dsnew.name as new_datasource_name, 		
    case 
	   when pso.name_en is not null then pso.name_en
	   when clo.name_en is not null then clo.name_en
	   when tco.name_en is not null then tco.name_en
	   when cast(oldvalue as varchar) = 'null' then null
	   else cast(oldvalue as varchar) 
	end as old_value,
	
	dsold.name as old_datasource_name,
	v.version,
	newvalue as new_raw_value, 
	oldvalue as old_raw_value,
	iv.version as insert_version,
	h.new_datasource_id as new_datasource_id, 
	h.old_datasource_id as old_datasource_id, 
	l.action as raw_action
from 
waterfalls_audit_log l
left join featuretypeversions v on v.type = 'waterfalls' and cast(l.datetime as date) between v.start_date and v.end_date
left join cabd.feature_insert_version iv on iv.cabd_id = l.cabd_id
left join cabd.feature_attribute_datasource_history h on h.cabd_id = l.cabd_id  and h.field_name  = l.field_name || '_ds' and l.datetime >= h.start_datetime and l.datetime < h.end_datetime
left join cabd.data_source dsnew on h.new_datasource_id = dsnew.id
left join cabd.data_source dsold on h.old_datasource_id = dsold.id

left join cabd.passability_status_codes ps on ps.code = case when l.field_name = 'passability_status_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join cabd.passability_status_codes pso on pso.code = case when l.field_name = 'passability_status_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join waterfalls.waterfall_complete_level_codes cl on cl.code = case when l.field_name = 'complete_level_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join waterfalls.waterfall_complete_level_codes clo on clo.code = case when l.field_name = 'complete_level_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join waterfalls.waterfall_type_codes tc on tc.code = case when l.field_name = 'waterfall_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join waterfalls.waterfall_type_codes tco on tco.code = case when l.field_name = 'waterfall_type_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end
;

drop view if exists cabd.fishways_audit_log_vw;
create or replace view cabd.fishways_audit_log_vw as
with fishways_audit_log as (
--insert
select l.cabd_id, l.revision, l.datetime, l.action,p.id as field_name, newvalues->p.id as newvalue, cast(null as jsonb) as oldvalue
from 
cabd.audit_log l
cross join jsonb_each_text(newvalues) as p(id,data)
where tablename  = 'fishways' and schemaname ='fishways' and action = 'INSERT' 
and p.data is not null
and p.id not in ( 'last_modified', 'snapped_ncc', 'snapped_point')
union 
--update
select l.cabd_id, l.revision, l.datetime, l.action, p.id as field_name, newvalues->p.id as newvalue, oldvalues->p.id as oldvalue
from 
cabd.audit_log l
cross join jsonb_each_text(oldvalues) as p(id,data) 
where tablename  = 'fishways' and schemaname ='fishways' and action = 'UPDATE' 
and p.id not in ( 'last_modified', 'snapped_ncc', 'snapped_point')
union
--delete
select l.cabd_id, l.revision, l.datetime, l.action, null as field_name, null as newvalue, null as oldvalue
from cabd.audit_log l
where tablename  = 'fishways' and schemaname ='fishways' and action = 'DELETE'
),
featuretypeversions as 
(
select type, version, 
  case when start_date is null then '1990-01-01' else start_date end as start_date, 
  case when end_date is null then now() else end_date end as end_date
from cabd.feature_type_version_history 
where type = 'fishways'
)
select 
   l.revision, 
   l.datetime, 
   case 
	  when l.action = 'INSERT' and l.field_name = 'cabd_id' then 'INSERT'
	  when l.action = 'INSERT' and iv.version = v.version then 'NEW_POINT_UPDATE' 
	  when l.action = 'INSERT' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
	  when l.action = 'UPDATE' and iv.version = v.version then 'NEW_POINT_UPDATE'
	  when l.action = 'UPDATE' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
	  else l.action 
	end as action,
    l.cabd_id,  
    iv.type as feature_type, 
    l.field_name as field_name, 
	case 
	   when up.name_en is not null then up.name_en 
	   when elc.name_en is not null then elc.name_en
	   when epc.name_en is not null then epc.name_en
	   when cc.name_en is not null then cc.name_en
	   when cast(newvalue as varchar) = 'null' then null
	   else cast(newvalue as varchar) 
	end as new_value,
    dsnew.name as new_datasource_name, 		
    case 
	   when upo.name_en is not null then upo.name_en 
	   when elco.name_en is not null then elco.name_en
	   when epco.name_en is not null then epco.name_en
	   when cco.name_en is not null then cco.name_en
	   when cast(oldvalue as varchar) = 'null' then null
	   else cast(oldvalue as varchar) 
	end as old_value,
	
	dsold.name as old_datasource_name,
	v.version,
	newvalue as new_raw_value, 
	oldvalue as old_raw_value,
	iv.version as insert_version,
	h.new_datasource_id as new_datasource_id, 
	h.old_datasource_id as old_datasource_id, 
	l.action as raw_action
from 
fishways_audit_log l
left join featuretypeversions v on v.type = 'fishways' and cast(l.datetime as date) between v.start_date and v.end_date
left join cabd.feature_insert_version iv on iv.cabd_id = l.cabd_id
left join cabd.feature_attribute_datasource_history h on h.cabd_id = l.cabd_id  and h.field_name  = l.field_name || '_ds' and l.datetime >= h.start_datetime and l.datetime < h.end_datetime
left join cabd.data_source dsnew on h.new_datasource_id = dsnew.id
left join cabd.data_source dsold on h.old_datasource_id = dsold.id

left join cabd.upstream_passage_type_codes up on up.code = case when l.field_name = 'fishpass_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join cabd.upstream_passage_type_codes upo on upo.code = case when l.field_name = 'fishpass_type_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join fishways.entrance_location_codes elc on elc.code = case when l.field_name = 'entrance_location_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join fishways.entrance_location_codes elco on elco.code = case when l.field_name = 'entrance_location_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join fishways.entrance_position_codes epc on epc.code = case when l.field_name = 'entrance_position_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join fishways.entrance_position_codes epco on epco.code = case when l.field_name = 'entrance_position_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end

left join fishways.fishway_complete_level_codes cc on cc.code = case when l.field_name = 'complete_level_code' and newvalue != 'null' then cast(newvalue as integer) else null end
left join fishways.fishway_complete_level_codes cco on cco.code = case when l.field_name = 'complete_level_code' and oldvalue != 'null' then cast(oldvalue as integer) else null end
;


create view cabd.all_audit_log_vw
as 
select * from cabd.dams_audit_log_vw dalv 
union 
select * from cabd.waterfalls_audit_log_vw walv 
union
select * from cabd.fishways_audit_log_vw falv ;




--- ========== STREAM CROSSINGS ===================
 --- **** 
 -- These are the views for stream crossings, at this time the stream crossing 
 -- triggers don't exist to track audit log stuff as the data structure and managements
 -- of these attributes is still be worked on
 --- ****
-- drop view if exists cabd.tidal_stream_crossings_audit_log_vw;
-- create view cabd.tidal_stream_crossins_audit_log_vw as
-- with tidal_audit_log as (
-- --insert
-- select l.cabd_id, l.revision, l.datetime, l.action,p.id as field_name, newvalues->p.id as newvalue, cast(null as jsonb) as oldvalue
-- from 
-- cabd.audit_log l
-- cross join jsonb_each_text(newvalues) as p(id,data)
-- where tablename  = 'tidal_sites' and schemaname ='stream_crossings' and action = 'INSERT' 
-- and p.data is not null
-- and p.id != 'last_modified'
-- union 
-- --update
-- select l.cabd_id, l.revision, l.datetime, l.action, p.id as field_name, newvalues->p.id as newvalue, oldvalues->p.id as oldvalue
-- from 
-- cabd.audit_log l
-- cross join jsonb_each_text(oldvalues) as p(id,data) 
-- where tablename  = 'tidal_sites' and schemaname ='stream_crossings' and action = 'UPDATE' 
-- and p.id != 'last_modified'
-- union
-- --delete
-- select l.cabd_id, l.revision, l.datetime, l.action, null as field_name, null as newvalue, null as oldvalue
-- from cabd.audit_log l
-- where tablename  = 'tidal_sites' and schemaname ='stream_crossings' and action = 'DELETE'
-- ),
-- featuretypeversions as 
-- (
-- select type, version, 
--   case when start_date is null then '1990-01-01' else start_date end as start_date, 
--   case when end_date is null then now() else end_date end as end_date
-- from cabd.feature_type_version_history 
-- )
-- select 
--    l.revision, 
--    l.datetime, 
--    case 
-- 	  when l.action = 'INSERT' and l.field_name = 'cabd_id' then 'INSERT'
-- 	  when l.action = 'INSERT' and iv.version = v.version then 'NEW_POINT_UPDATE' 
-- 	  when l.action = 'INSERT' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
-- 	  when l.action = 'UPDATE' and iv.version = v.version then 'NEW_POINT_UPDATE'
-- 	  when l.action = 'UPDATE' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
-- 	  else l.action 
-- 	end as action,
--     l.cabd_id,  
--     iv.type as feature_type, 
--     l.field_name as field_name, 
-- 	case 
-- 	   when ac.name_en is not null then ac.name_en 
-- 	   when ccc.name_en is not null then ccc.name_en
-- 	   when ctc.name_en is not null then ctc.name_en
-- 	   when fcc.name_en is not null then fcc.name_en
-- 	   when rtc.name_en is not null then rtc.name_en
-- 	   when stc.name_en is not null then stc.name_en
-- 	   when tc.name_en is not null then tc.name_en
-- 	   when tsc.name_en is not null then tsc.name_en
-- 	   when vuc.name_en is not null then vuc.name_en
-- 	   when vcc.name_en is not null then vcc.name_en
-- 	   when cast(newvalue as varchar) = 'null' then null
-- 	   else cast(newvalue as varchar) 
-- 	end as new_value,
--     dsnew.name as new_datasource_name, 		
--     case 
-- 	   when aco.name_en is not null then aco.name_en 
-- 	   when ccco.name_en is not null then ccco.name_en
-- 	   when ctco.name_en is not null then ctco.name_en
-- 	   when fcco.name_en is not null then fcco.name_en
-- 	   when rtco.name_en is not null then rtco.name_en
-- 	   when stco.name_en is not null then stco.name_en
-- 	   when tco.name_en is not null then tco.name_en
-- 	   when tsco.name_en is not null then tsco.name_en
-- 	   when vuco.name_en is not null then vuco.name_en
-- 	   when vcco.name_en is not null then vcco.name_en
-- 	   when cast(oldvalue as varchar) = 'null' then null
-- 	   else cast(oldvalue as varchar) 
-- 	end as old_value,
-- 	
-- 	dsold.name as old_datasource_name,
-- 	v.version,
-- 	newvalue as new_raw_value, 
-- 	oldvalue as old_raw_value,
-- 	iv.version as insert_version,
-- 	h.new_datasource_id as new_datasource_id, 
-- 	h.old_datasource_id as old_datasource_id, 
-- 	l.action as raw_action
-- from 
-- tidal_audit_log l
-- left join featuretypeversions v on v.type = 'dams' and cast(l.datetime as date) between v.start_date and v.end_date
-- left join cabd.feature_insert_version iv on iv.cabd_id = l.cabd_id
-- left join cabd.feature_attribute_datasource_history h on h.cabd_id = l.cabd_id  and h.field_name  = l.field_name || '_ds' and l.datetime >= h.start_datetime and l.datetime < h.end_datetime
-- left join cabd.data_source dsnew on h.new_datasource_id = dsnew.id
-- left join cabd.data_source dsold on h.old_datasource_id = dsold.id
-- 
-- left join stream_crossings.alignment_codes ac on ac.code = case when l.field_name = 'alignment_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.alignment_codes aco on aco.code = case when l.field_name = 'alignment_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.crossing_condition_codes ccc on ccc.code = case when l.field_name = 'crossing_condition_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.crossing_condition_codes ccco on ccco.code = case when l.field_name = 'crossing_condition_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.crossing_type_codes ctc on ctc.code = case when l.field_name = 'crossing_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.crossing_type_codes ctco on ctco.code = case when l.field_name = 'crossing_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.flow_condition_codes fcc on fcc.code = case when l.field_name = 'flow_condition_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.flow_condition_codes fcco on fcco.code = case when l.field_name = 'flow_condition_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.road_type_codes rtc on rtc.code = case when l.field_name = 'road_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.road_type_codes rtco on rtco.code = case when l.field_name = 'road_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.site_type_codes stc on stc.code = case when l.field_name = 'site_type' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.site_type_codes stco on stco.code = case when l.field_name = 'site_type' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.stream_type_codes tc on tc.code = case when l.field_name = 'tidal_stream_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.stream_type_codes tco on tco.code = case when l.field_name = 'tidal_stream_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.tide_stage_codes tsc on tsc.code = case when l.field_name = 'tidal_tide_stage_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.tide_stage_codes tsco on tsco.code = case when l.field_name = 'tidal_tide_stage_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.visible_utilities_codes vuc on vuc.code = case when l.field_name = 'tital_visible_utilities_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.visible_utilities_codes vuco on vuco.code = case when l.field_name = 'tital_visible_utilities_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.vegetation_change_codes vcc on vcc.code = case when l.field_name = 'tidal_veg_change_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.vegetation_change_codes vcco on vcc.code = case when l.field_name = 'tidal_veg_change_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- 
-- 
-- 
-- drop view if exists cabd.nontidal_stream_crossings_audit_log_vw;
-- create view cabd.nontidal_stream_crossins_audit_log_vw as
-- with nontidal_audit_log as (
-- --insert
-- select l.cabd_id, l.revision, l.datetime, l.action,p.id as field_name, newvalues->p.id as newvalue, cast(null as jsonb) as oldvalue
-- from 
-- cabd.audit_log l
-- cross join jsonb_each_text(newvalues) as p(id,data)
-- where tablename  = 'tidal_sites' and schemaname ='stream_crossings' and action = 'INSERT' 
-- and p.data is not null
-- and p.id != 'last_modified'
-- union 
-- --update
-- select l.cabd_id, l.revision, l.datetime, l.action, p.id as field_name, newvalues->p.id as newvalue, oldvalues->p.id as oldvalue
-- from 
-- cabd.audit_log l
-- cross join jsonb_each_text(oldvalues) as p(id,data) 
-- where tablename  = 'tidal_sites' and schemaname ='stream_crossings' and action = 'UPDATE' 
-- and p.id != 'last_modified'
-- union
-- --delete
-- select l.cabd_id, l.revision, l.datetime, l.action, null as field_name, null as newvalue, null as oldvalue
-- from cabd.audit_log l
-- where tablename  = 'tidal_sites' and schemaname ='stream_crossings' and action = 'DELETE'
-- ),
-- featuretypeversions as 
-- (
-- select type, version, 
--   case when start_date is null then '1990-01-01' else start_date end as start_date, 
--   case when end_date is null then now() else end_date end as end_date
-- from cabd.feature_type_version_history 
-- )
-- select 
--    l.revision, 
--    l.datetime, 
--    case 
-- 	  when l.action = 'INSERT' and l.field_name = 'cabd_id' then 'INSERT'
-- 	  when l.action = 'INSERT' and iv.version = v.version then 'NEW_POINT_UPDATE' 
-- 	  when l.action = 'INSERT' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
-- 	  when l.action = 'UPDATE' and iv.version = v.version then 'NEW_POINT_UPDATE'
-- 	  when l.action = 'UPDATE' and iv.version != v.version then 'EXISTING_POINT_UPDATE'
-- 	  else l.action 
-- 	end as action,
--     l.cabd_id,  
--     iv.type as feature_type, 
--     l.field_name as field_name, 
-- 	case 
-- 	   when ac.name_en is not null then ac.name_en 
-- 	   when ctc.name_en is not null then ctc.name_en
-- 	   when ccc.name_en is not null then ccc.name_en
-- 	   when dsbfcc.name_en is not null then dsbfcc.name_en
-- 	   when fcc.name_en is not null then fcc.name_en
-- 	   when rtc.name_en is not null then rtc.name_en
-- 	   when stc.name_en is not null then stc.name_en
-- 	   when usbfcc.name_en is not null then usbfcc.name_en
-- 	   when spc.name_en is not null then spc.name_en
-- 	   when cc.name_en is not null then cc.name_en
-- 	   when cast(newvalue as varchar) = 'null' then null
-- 	   else cast(newvalue as varchar) 
-- 	end as new_value,
--     dsnew.name as new_datasource_name, 		
--     case 
-- 	   when aco.name_en is not null then aco.name_en 
-- 	   when ctco.name_en is not null then ctco.name_en
-- 	   when ccco.name_en is not null then ccco.name_en
-- 	   when dsbfcco.name_en is not null then dsbfcco.name_en
-- 	   when fcco.name_en is not null then fcco.name_en
-- 	   when rtco.name_en is not null then rtco.name_en
-- 	   when stco.name_en is not null then stco.name_en
-- 	   when usbfcco.name_en is not null then usbfcco.name_en
-- 	   when spco.name_en is not null then spco.name_en
-- 	   when cco.name_en is not null then cco.name_en
-- 	   when cast(oldvalue as varchar) = 'null' then null
-- 	   else cast(oldvalue as varchar) 
-- 	end as old_value,
-- 	
-- 	dsold.name as old_datasource_name,
-- 	v.version,
-- 	newvalue as new_raw_value, 
-- 	oldvalue as old_raw_value,
-- 	iv.version as insert_version,
-- 	h.new_datasource_id as new_datasource_id, 
-- 	h.old_datasource_id as old_datasource_id, 
-- 	l.action as raw_action
-- from 
-- nontidal_audit_log l
-- left join featuretypeversions v on v.type = 'dams' and cast(l.datetime as date) between v.start_date and v.end_date
-- left join cabd.feature_insert_version iv on iv.cabd_id = l.cabd_id
-- left join cabd.feature_attribute_datasource_history h on h.cabd_id = l.cabd_id  and h.field_name  = l.field_name || '_ds' and l.datetime >= h.start_datetime and l.datetime < h.end_datetime
-- left join cabd.data_source dsnew on h.new_datasource_id = dsnew.id
-- left join cabd.data_source dsold on h.old_datasource_id = dsold.id
-- 
-- 
-- left join stream_crossings.alignment_codes ac on ac.code = case when l.field_name = 'alignment_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.alignment_codes aco on aco.code = case when l.field_name = 'alignment_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.crossing_type_codes ctc on ctc.code = case when l.field_name = 'crossing_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.crossing_type_codes ctco on ctco.code = case when l.field_name = 'crossing_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.crossing_condition_codes ccc on ccc.code = case when l.field_name = 'crossing_condition_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.crossing_condition_codes ccco on ccco.code = case when l.field_name = 'crossing_condition_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.confidence_codes dsbfcc on dsbfcc.code = case when l.field_name = 'downstream_bankfull_width_confidence_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.confidence_codes dsbfcco on dsbfcco.code = case when l.field_name = 'downstream_bankfull_width_confidence_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.flow_condition_codes fcc on fcc.code = case when l.field_name = 'flow_condition_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.flow_condition_codes fcco on fcco.code = case when l.field_name = 'flow_condition_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.road_type_codes rtc on rtc.code = case when l.field_name = 'road_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.road_type_codes rtco on rtco.code = case when l.field_name = 'road_type_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.site_type_codes stc on stc.code = case when l.field_name = 'site_type' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.site_type_codes stco on stco.code = case when l.field_name = 'site_type' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.confidence_codes usbfcc on usbfcc.code = case when l.field_name = 'upstream_bankfull_width_confidence_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.confidence_codes usbfcco on usbfcco.code = case when l.field_name = 'upstream_bankfull_width_confidence_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.scour_pool_codes spc  on spc.code = case when l.field_name = 'tailwater_scour_pool_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.scour_pool_codes spco on spco.code = case when l.field_name = 'tailwater_scour_pool_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- 
-- left join stream_crossings.constriction_codes cc on cc.code = case when l.field_name = 'constriction_code' and newvalue != 'null' then cast(newvalue as integer) else null end
-- left join stream_crossings.constriction_codes cco on cco.code = case when l.field_name = 'constriction_code' and newvalue != 'null' then cast(newvalue as integer) else null end
--- ========== END OF STREAM CROSSINGS ===================



---- ITEM 2 in Notions Document -----
-- for a specific data source
-- find all updates to any feature in that data source from the audit log
 
create or replace function cabd.featureupdates_by_datasource(datasource_name varchar)
returns table(
datasource_feature_id varchar,
revision integer,
datetime timestamp,
action varchar,
cabd_id uuid,
feature_type varchar(32),
field_name text,
new_value varchar,
new_datasource_name varchar,
old_value varchar,
old_datasource_name varchar,
version varchar,
new_raw_value jsonb,
old_raw_value jsonb,
insert_version varchar,
new_datasource_id uuid,
old_datasource_id uuid, 
raw_action varchar(6)
)
as $$
declare
  dsid uuid;
  fquery varchar;
  fsource varchar;
  
begin 
	select id into dsid from cabd.data_source where name = datasource_name;
	if not found then
		raise exception 'datasource with name % not found', datasource_name;
	end if;

 	fquery := 'select null as cabd_id, null as datasource_feature_id ';
	for fsource in select distinct feature_source_table from cabd.feature_types loop
		if (fsource is not null) then
			fquery := fquery || ' UNION select cabd_id, datasource_feature_id FROM ' || fsource || ' WHERE datasource_id = ''' || dsid || '''';
		end if;
	end loop;
	
	fquery := '
	with cabdids as (' || fquery || ') 
	select b.datasource_feature_id as datasource_feature_id, a.* from cabd.all_audit_log_vw a join cabdids b on a.cabd_id = b.cabd_id
	ORDER BY a.cabd_id, a.datetime desc';
	RETURN QUERY EXECUTE fquery;
	--return fquery;

end;
$$  language plpgsql;





----------- ITEM 3 in Notions Document -----------
--- Returns features added to cabd that are not in a given datasource for a given feature types ---
--- only works for data sources with geographic coverage

-- add geographic coverage column
alter table cabd.data_source add column province_territory_code varchar[];

update cabd.data_source set province_territory_code = ARRAY['bc'] where geographic_coverage = 'British Columbia';
update cabd.data_source set province_territory_code = ARRAY['ab'] where geographic_coverage = 'Alberta';
update cabd.data_source set province_territory_code = ARRAY['sk'] where geographic_coverage = 'Saskatchewan';
update cabd.data_source set province_territory_code = ARRAY['mb'] where geographic_coverage = 'Manitoba';
update cabd.data_source set province_territory_code = ARRAY['on'] where geographic_coverage = 'Ontario';
update cabd.data_source set province_territory_code = ARRAY['ns'] where geographic_coverage = 'Nova Scotia';
update cabd.data_source set province_territory_code = ARRAY['nb'] where geographic_coverage = 'New Brunswick';
update cabd.data_source set province_territory_code = ARRAY['nl'] where geographic_coverage = 'Newfoundland and Labrador';
update cabd.data_source set province_territory_code = ARRAY['qc'] where geographic_coverage = 'Québec';
update cabd.data_source set province_territory_code = ARRAY['ns', 'nb', 'pe', 'nl'] where geographic_coverage = 'Maritimes';
update cabd.data_source set province_territory_code = ARRAY['bc', 'ab', 'sk', 'mb', 'on', 'qc', 'nb', 'ns', 'nl', 'pe', 'nu', 'nt', 'yt'] where geographic_coverage = 'National';

-- for any data source and feature type
-- this will return a list of cabd_id that do not exist in that
-- data source for the geographic regions assigned to the data source
-- and the version they were added to the database
-- it only returns cabd_id and version_added fields

create or replace function cabd.newfeatures_by_datasource(datasource_name varchar, in_feature_type varchar)
returns table(cabd_id uuid, version_added varchar)
as $$
declare
  dsid uuid;
  dsprov varchar[];
  fstable varchar;
  query varchar;
  
begin 
	select id, province_territory_code into dsid, dsprov from cabd.data_source where name = datasource_name;
	if not found then
		raise exception 'datasource with name % not found', datasource_name;
	end if;
	if (dsprov is null) then
		raise exception 'datasource % does not have a province territory set so cannot determine new features', datasource_name;
	end if;
	select feature_source_table into fstable from cabd.feature_types where type = in_feature_type;
	if not found then
		raise exception 'feature type % not found', in_feature_type;
	end if;

	query := '
	with newfeatures as (
		select a.cabd_id from cabd.all_features_view_en a where 
			a.feature_type = $1 and 
	  	    province_territory_code in (select unnest(province_territory_code) from cabd.data_source where id = $2)
	  and a.cabd_id not in (
	    select dfs.cabd_id from ' || fstable || ' dfs where dfs.datasource_id = $3
	)
	)
	select nf.cabd_id, v.version as version_added  from newfeatures nf join cabd.feature_insert_version v on nf.cabd_id = v.cabd_id';
	RETURN QUERY EXECUTE query using in_feature_type, dsid,  dsid;

end;
$$  language plpgsql;
COMMENT on function cabd.newfeatures_by_datasource(datasource_name varchar, in_feature_type varchar) IS 'For any data source and feature type this will return a list of cabd_ids that do not exist in that data source for the geographic region associated with the data source and the provided feature type. It returns only the cabd_id and the version the feature was added to the database.';

-- returns an sql string
-- that if run returns all the feature details for 
-- the new features in a given datasource along with the
-- version it was added
-- does not run the query; users have to do that manually
create or replace function cabd.newfeatures_by_datasource_query(datasource_name varchar, in_feature_type varchar, locale varchar)
returns varchar
as $$
declare
 dataview varchar;
begin 
	
	select data_view into dataview from cabd.feature_types where type = in_feature_type;
	if not found then
		raise exception 'feature type % not found', in_feature_type;
	end if;
	return 'select b.version_added, a.* from ' || dataview || '_' || locale || ' a join cabd.newfeatures_by_datasource(''' || datasource_name || ''', '''|| in_feature_type || ''') b on a.cabd_id = b.cabd_id';
end;
$$  language plpgsql;
COMMENT on function cabd.newfeatures_by_datasource_query(datasource_name varchar, in_feature_type varchar, locale varchar) IS 'Returns a sql string that, if run, will return all the feature details for the features added to the database that do not exist in the data source.';

select * from cabd.newfeatures_by_datasource_query('bcflnrord_wris_pubdams', 'dams', 'en');
select b.version_added, a.* from cabd.dams_view_en a join cabd.newfeatures_by_datasource('aep_bf_hy', 'dams') b on a.cabd_id = b.cabd_id;

