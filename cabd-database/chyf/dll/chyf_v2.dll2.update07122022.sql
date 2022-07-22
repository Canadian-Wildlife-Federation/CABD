--add nids 
alter table fpinput.eflowpath add column nid varchar(32);
alter table fpinput.ecatchment add column nid varchar(32);

alter table fpoutput.eflowpath add column nid varchar(32);
alter table fpoutput.ecatchment add column nid varchar(32);

alter table chyf2.eflowpath add column nid varchar(32);
alter table chyf2.ecatchment add column nid varchar(32);


------------- The queries below attempt to update nid of already processed data ----------------------------- 

--to update processed layers in the chyf2 schema run this query then run
-- the queries that this query outputs
select 
'with edges as (select c.id, a.nid from nhn' || LOWER(short_name) || '.eflowpath a join fpoutput.eflowpath b on a.id = b.id join chyf2.eflowpath c on b.internal_id  = c.id) update chyf2.eflowpath set nid = ed.nid from edges ed where ed.id = chyf2.eflowpath.id'
from chyf2.aoi;

--do the same for ecatchments
select id, short_name, 
'with wbs as (select c.id, a.nid from nhn' || LOWER(short_name) || '.ecatchment a join fpoutput.ecatchment b on a.id = b.id join chyf2.ecatchment c on b.internal_id  = c.id) update chyf2.ecatchment set nid = w.nid from wbs w where w.id = chyf2.ecatchment.id'
from chyf2.aoi

--to check that most nids are copied correctly:
-- for flowpaths most nids should be not null, but all skeletons will be null
-- for a single aoi if all nids are null then the data did not get copied correctly
select a.short_name, case when b.nid is null then 'NULL' else 'NOT NULL' end as has_nid, count(*)
from chyf2.aoi a join chyf2.eflowpath b on a.id = b.aoi_id
group by a.short_name, has_nid
order by a.short_name;

-- for catchments all nids should be not null
-- for a single aoi if there are any null catchments then something is wrong
select a.short_name, case when b.nid is null then 'NULL' else 'NOT NULL' end as has_nid, count(*)
from chyf2.aoi a join chyf2.ecatchment b on a.id = b.aoi_id
group by a.short_name, has_nid
order by a.short_name;



--now if you want to update aois in fpoutput that have not
--yet been loaded into chyf2 schema
select 
'with edges as (select b.id, a.nid from nhn' || LOWER(name) || '.eflowpath a join fpoutput.eflowpath b on a.id = b.id ) update fpoutput.eflowpath set nid = ed.nid from edges ed where ed.id = fpoutput.eflowpath.id'
from fpoutput.aoi where status != 'CHYF_DONE'

--same for ecatchments
select 
'with wbs as (select b.id, a.nid from nhn' || LOWER(name) || '.ecatchment a join fpoutput.ecatchment b on a.id = b.id ) update fpoutput.ecatchment set nid = w.nid from wbs w where w.id = fpoutput.ecatchment.id'
from fpoutput.aoi where status != 'CHYF_DONE'

--to check that most nids are copied correctly:
-- for flowpaths most nids should be not null, but all skeletons will be null
-- for a single aoi if all nids are null then the data did not get copied correctly
select a.name, case when b.nid is null then 'NULL' else 'NOT NULL' end as has_nid, count(*)
from fpoutput.aoi a join fpoutput.eflowpath b on a.id = b.aoi_id
group by a.name, has_nid
order by a.name;

-- for catchments all nids should be not null
-- for a single aoi if there are any null catchments then something is wrong
select a.name, case when b.nid is null then 'NULL' else 'NOT NULL' end as has_nid, count(*)
from fpoutput.aoi a join fpoutput.ecatchment b on a.id = b.aoi_id
group by a.name, has_nid
order by a.name;
