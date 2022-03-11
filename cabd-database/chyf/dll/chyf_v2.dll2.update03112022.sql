--we wanted to ask you what it would take set up a flag on the
--chyf2 schema AOI table, like what you did with the AOI table for
-- processing, that would allow us to decide which networks we
-- want/don't want displayed in the app at any given time.
--To support "releases" of data

--Implementation:
--create views for the data tables
--for the app to reference

alter table chyf2.aoi add column display_status smallint default 0;
update chyf2.aoi set display_status = 1;

create view chyf2.eflowpath_vw as 
select a.* 
from chyf2.eflowpath a join chyf2.aoi b on a.aoi_id = b.id
where b.display_status = 1;

create view chyf2.ecatchment_vw as 
select a.* 
from chyf2.ecatchment a join chyf2.aoi b on a.aoi_id = b.id
where b.display_status = 1;

create view chyf2.shoreline_vw as 
select a.* 
from chyf2.shoreline a join chyf2.aoi b on a.aoi_id = b.id
where b.display_status = 1;


create view chyf2.nexus_vw as 
select a.* 
from chyf2.nexus a join chyf2.eflowpath b on a.id = b.from_nexus_id
join chyf2.eflowpath c on a.id = c.to_nexus_id
join chyf2.aoi d on d.id = b.aoi_id
join chyf2.aoi e on e.id = b.aoi_id 
where (d.display_status = 1 or e.display_status = 1);

truncate chyf2.vector_tile_cache ;

