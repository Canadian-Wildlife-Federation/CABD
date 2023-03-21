----------------------------
-- This series of SQL comments creates modelled crossing for N.B. data.
-- It requires the following data to exist in the nb_data schema:
-- * canvec_rail, eflowpath (from chyf), eflowpath_properties (from chyf), gnb_roads, nbrn_roads
--
-- The road and rail layers are intersected with the chyf hydro network. Results are clustered in an
-- attempt to remove duplicates, then the intersection with the "largest" stream is retained 
-- and used for the modelled crossing point. Priority is given to nbrn roads - so those crossing points are
-- used before other crossings points. Upstream length is used as a proxy for "largest".
-- Stream information and road/rail information is then added back to crossing points.
--
-- Scripts below have not been optimized. They run resonable quickly on the NB data. However
-- they may need optimzation to run on larger datasets.
--
-- There are some select statements included in the script below to validate the results are as expected
--
-- Known Issue/Limitation: 
-- When a cluster contains multiple intersections with secondary streams we don't have upstream length
-- and the proxy for largest stream is simple the stream edge length. In the case of NB data this didn't matter, however it
-- may matter for other datasets.
--

drop table if exists nb_data.canvec_crossings_t;
drop table if exists nb_data.gnb_crossings_t;
drop table if exists nb_data.nbrn_crossings_t;

drop table if exists nb_data.canvec_crossings;
drop table if exists nb_data.gnb_crossings;
drop table if exists nb_data.nbrn_crossings;

-- CANVEC CROSSINGS
create table nb_data.canvec_crossings_t as 
select a.fid as canvec_fid,
b.id as eflowpath_id, st_intersection(a.geometry, b.geometry) as geometry
from nb_data.canvec_rail a, nb_data.eflowpath b
where st_intersects(a.geometry, b.geometry);

create table nb_data.canvec_crossings as 
select canvec_fid, eflowpath_id, (st_dump(geometry)).geom as geometry from nb_data.canvec_crossings_t;

drop table nb_data.canvec_crossings_t;

-- GNB CROSSINGS
create table nb_data.gnb_crossings_t as 
select a.fid as gnb_fid,
b.id as eflowpath_id, st_intersection(a.geometry, b.geometry) as geometry
from nb_data.gnb_roads a, nb_data.eflowpath b
where st_intersects(a.geometry, b.geometry);

create table nb_data.gnb_crossings as 
select gnb_fid, eflowpath_id, (st_dump(geometry)).geom as geometry from nb_data.gnb_crossings_t;

drop table nb_data.gnb_crossings_t;

-- NBRN CROSSINGS
create table nb_data.nbrn_crossings_t as 
select a.fid as nbrn_fid,
b.id as eflowpath_id, st_intersection(a.geometry, b.geometry) as geometry
from nb_data.nbrn_roads a, nb_data.eflowpath b
where st_intersects(a.geometry, b.geometry);


create table nb_data.nbrn_crossings as 
select nbrn_fid, eflowpath_id, (st_dump(geometry)).geom as geometry from nb_data.nbrn_crossings_t;

drop table nb_data.nbrn_crossings_t;


-- COMBINE ALL
drop table if exists nb_data.all_crossings;
create table nb_data.all_crossings (id serial, eflowpath_id uuid, canvec_fid integer, gnb_fid integer, nbrn_fid integer, geometry geometry(POINT, 4617));
insert into nb_data.all_crossings(canvec_fid, eflowpath_id, geometry) select canvec_fid, eflowpath_id, geometry from nb_data.canvec_crossings;
insert into nb_data.all_crossings(gnb_fid, eflowpath_id, geometry) select gnb_fid, eflowpath_id, geometry from nb_data.gnb_crossings;
insert into nb_data.all_crossings(nbrn_fid, eflowpath_id, geometry) select nbrn_fid, eflowpath_id, geometry from nb_data.nbrn_crossings;

create index all_crossings_geometry on nb_data.all_crossings using gist(geometry);
analyze nb_data.all_crossings;


--cluster to remove similar points

ALTER TABLE nb_data.all_crossings ADD COLUMN geometry_m geometry(point, 2953);
update nb_data.all_crossings set geometry_m = st_transform(geometry, 2953) ;

drop table if exists nb_data.clustered ;
drop table if exists nb_data.clustered2 ;

CREATE TABLE nb_data.clustered AS
SELECT  unnest(st_clusterwithin(geometry_m, 15)) as geometry FROM nb_data.all_crossings;

--add a cluster id
alter table nb_data.clustered add column cluster_id serial;

--explode clusters
create table nb_data.clustered2 as 
select st_geometryn(geometry, 1) as geometry, st_numgeometries(geometry) as num_merged, cluster_id from nb_data.clustered c;


-- FIND CLUSTER POINT TO RETAIN
-- clean up clusters
drop table  if exists nb_data.clustered_by_id;

create table nb_data.clustered_by_id as 
select cluster_id, st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as geometry from nb_data.clustered c;


drop table  if exists  nb_data.clustered_by_id_with_data ;
create table nb_data.clustered_by_id_with_data as 
select a.cluster_id, a.geometry, b.eflowpath_id, b.canvec_fid, b.gnb_fid, b.nbrn_fid
from nb_data.clustered_by_id a left join nb_data.all_crossings b 
on a.geometry = b.geometry_m ;

--should return nothing
select * from nb_data.clustered_by_id_with_data where eflowpath_id is null;

-- temp1 - is a temporary table for holding cluster point to keep.

-- add single point clusters
drop table  if exists nb_data.temp1;
create table nb_data.temp1 as 
select cluster_id, geometry, eflowpath_id from nb_Data.clustered_by_id_with_data where cluster_id in(
select cluster_id from nb_data.clustered_by_id_with_data group by cluster_id having count(*) = 1
);

--remove these from processing
delete from nb_data.clustered_by_id_with_data where cluster_id in (select cluster_id from nb_Data.temp1);

--- find the largest stream on prioritizing nbrn segments ---
--for each cluster find nbrn segments
drop table  if exists nb_data.cluster_nrbn_id;
create table nb_data.cluster_nrbn_id as 
select * from nb_data.clustered_by_id_with_data where nbrn_fid is not null; 

--for all these ones where there is only one 
--use this as the cluster point 
insert into nb_data.temp1 
select cluster_id, geometry, eflowpath_id from nb_data.cluster_nrbn_id where cluster_id in (
select cluster_id from nb_data.cluster_nrbn_id group by cluster_id having count(*) = 1
);

--remove these from nrbn data & clustered_by_id_with_data
delete from nb_data.clustered_by_id_with_data where cluster_id in (select cluster_id from nb_Data.temp1);
delete from nb_data.cluster_nrbn_id where cluster_id in (select cluster_id from nb_Data.temp1);

-- add location on line
alter table nb_data.cluster_nrbn_id add column point_on_line double precision;
update nb_Data.cluster_nrbn_id set point_on_line = st_linelocatepoint(e.geometry, st_transform(cluster_nrbn_id.geometry, 4617))
from nb_data.eflowpath e where e.id = nb_Data.cluster_nrbn_id.eflowpath_id;

--add upstream length
alter table nb_data.cluster_nrbn_id add column upstream_length double precision;

update nb_data.cluster_nrbn_id set upstream_length = case when a.max_uplength is null then b.length else a.max_uplength + b.length end
from nb_data.eflowpath_properties a join nb_data.eflowpath b on a.id = b.id 
where nb_data.cluster_nrbn_id.eflowpath_id = a.id;
--case statement above is to deal with secondaries - these have no upstream length
--there were a few cases where these were crossed by they were all on the same edge so it 
--doesn't matter
--we are using this up length to determine which edge to pick when there is more than
--one stream edge being crossed

--for each cluster we need edge id with the maximum 
drop table  if exists  nb_Data.temp3;
create table nb_data.temp3 as select cluster_id, max(upstream_length) as max_length from nb_data.cluster_nrbn_id group by cluster_id;

--map the cluster to the edge
drop table  if exists nb_Data.temp4;
create table nb_data.temp4 as select distinct a.cluster_id, a.max_length, b.eflowpath_id from  nb_data.cluster_nrbn_id b, nb_data.temp3 a
where a.cluster_id = b.cluster_id and b.upstream_length = a.max_length;

--for the flowpath we want to find the most downstream point in the cluster
drop table  if exists  nb_data.temp5;
create table nb_Data.temp5 as 
select a.cluster_id , max(point_on_line) as min_pol
from nb_data.cluster_nrbn_id a join nb_data.temp4 b on a.cluster_id = b.cluster_id and a.eflowpath_id = b.eflowpath_id
group by a.cluster_id;

--should be empty
select cluster_id from nb_Data.cluster_nrbn_id cni except select cluster_id from nb_Data.temp5;


--merge to find downstream point
drop table  if exists  nb_data.temp6;
create table nb_data.temp6 as select distinct a.cluster_id, a.geometry, a.eflowpath_id 
from nb_Data.cluster_nrbn_id a join nb_data.temp5 b on a.cluster_id  = b.cluster_id
and a.point_on_line = b.min_pol;

--should be empty
select cluster_id from nb_data.cluster_nrbn_id cni except select cluster_id from nb_data.temp6;

--add to main table and remove from processing
insert into nb_data.temp1 (cluster_id, geometry, eflowpath_id)
select cluster_id, geometry, eflowpath_id from nb_data.temp6;

--remove these from nrbn data & clustered_by_id_with_data
delete from nb_data.clustered_by_id_with_data where cluster_id in (select cluster_id from nb_Data.temp1);
delete from nb_data.cluster_nrbn_id where cluster_id in (select cluster_id from nb_Data.temp1);

--should be empty
select * from nb_data.cluster_nrbn_id;

--clustered_by_id_with_data
-- will only contain clusters that have multiple crossings but don't cross nrbn data
--for these find the most downstream edge
--similar to above

-- add location on line
alter table nb_data.clustered_by_id_with_data add column point_on_line double precision;

update nb_Data.clustered_by_id_with_data set point_on_line = st_linelocatepoint(e.geometry, st_transform(clustered_by_id_with_data.geometry, 4617))
from nb_data.eflowpath e where e.id = nb_Data.clustered_by_id_with_data.eflowpath_id ;

alter table nb_data.clustered_by_id_with_data add column upstream_length double precision;

update nb_data.clustered_by_id_with_data set upstream_length = case when a.max_uplength is null then b.length else a.max_uplength + b.length end
from nb_data.eflowpath_properties a join nb_data.eflowpath b on a.id = b.id 
where nb_data.clustered_by_id_with_data.eflowpath_id = a.id;
--case statement is to deal with secondaries - these have no upstream length
--there were a few cases where these were crossed by they were all on the same edge so it 
--doesn't matter

--for each cluster we need edge id with the maximum 

drop table  if exists nb_Data.temp3;
create table nb_data.temp3 as select cluster_id, max(upstream_length) as max_length from nb_data.clustered_by_id_with_data group by cluster_id;


--map the cluster to the edge
drop table  if exists  nb_Data.temp4;
create table nb_data.temp4 as select distinct a.cluster_id, a.max_length, b.eflowpath_id from  nb_data.clustered_by_id_with_data b, nb_data.temp3 a
where a.cluster_id = b.cluster_id and b.upstream_length = a.max_length;


--for the flowpath we want to find the most downstream point in the cluster
drop table  if exists  nb_data.temp5;
create table nb_Data.temp5 as 
select a.cluster_id , max(point_on_line) as min_pol
from nb_data.clustered_by_id_with_data a join nb_data.temp4 b on a.cluster_id = b.cluster_id and a.eflowpath_id = b.eflowpath_id
group by a.cluster_id;



--should be empty
select cluster_id from nb_Data.clustered_by_id_with_data cni except select cluster_id from nb_Data.temp5;

drop table  if exists nb_data.temp6;
create table nb_data.temp6 as select distinct a.cluster_id, a.geometry, a.eflowpath_id 
from nb_Data.clustered_by_id_with_data a join nb_data.temp5 b on a.cluster_id  = b.cluster_id
and a.point_on_line = b.min_pol;



select cluster_id from nb_data.cluster_nrbn_id cni except select cluster_id from nb_data.temp6;


insert into nb_data.temp1 (cluster_id, geometry, eflowpath_id)
select cluster_id, geometry, eflowpath_id from nb_data.temp6;


--remove these from nrbn data & clustered_by_id_with_data
delete from nb_data.clustered_by_id_with_data where cluster_id in (select cluster_id from nb_Data.temp1);

--further validation queries;
select count(*) from nb_data.clustered_by_id_with_data;
select cluster_id, count(*) from nb_data.temp1 group by cluster_id having count(*) > 1;

drop table if exists nb_Data.cluster_nrbn_id ;
drop table if exists nb_Data.clustered_by_id_with_data ;
drop table if exists nb_Data.temp6;
drop table if exists nb_Data.temp5;
drop table if exists nb_Data.temp4;
drop table if exists nb_Data.temp3;
drop table if exists nb_data.modelled_crossings;
alter table nb_data.temp1 rename to modelled_crossings;

--add 4617 geometry
alter table nb_data.modelled_crossings rename column geometry to geometry_m;
alter table nb_data.modelled_crossings add column geometry geometry(point, 4617);
update nb_data.modelled_crossings set geometry = st_Transform(geometry_m, 4617);

--add stream details
--in the chyf data; need to copy over the names for modelled crossings

alter table nb_data.modelled_crossings rename column eflowpath_id to chyf_stream_id;
alter table nb_data.modelled_crossings add column stream_name_id1 uuid;
alter table nb_data.modelled_crossings add column stream_name_id2 uuid;
alter table nb_data.modelled_crossings add column stream_name_1 varchar;
alter table nb_data.modelled_crossings add column stream_name_2 varchar;
alter table nb_data.modelled_crossings add column strahler_order varchar;

update nb_data.modelled_crossings set strahler_order = a.strahler_order 
from nb_data.eflowpath_properties a where a.id = nb_data.modelled_crossings.chyf_stream_id;

update nb_data.modelled_crossings set stream_name_id1 = a.rivernameid1, stream_name_id2 = a.rivernameid2 
from nb_data.eflowpath a where a.id = nb_data.modelled_crossings.chyf_stream_id;

update nb_data.modelled_crossings set stream_name_1 = a.name_en 
from nb_data.names a where a.name_id = nb_data.modelled_crossings.stream_name_id1;

update nb_data.modelled_crossings set stream_name_2 = a.name_en 
from nb_data.names a where a.name_id = nb_data.modelled_crossings.stream_name_id2;


--add road/rail network details

--figure out which feature id to use for cluster details
drop table if exists nb_data.temp1;
create table nb_data.temp1 as 
select a.cluster_id, b.*
from nb_data.modelled_crossings a, nb_data.all_crossings  b 
where a.geometry_m = b.geometry_m;

--deal with duplicates

drop table if exists nb_data.temp2;
create table nb_data.temp2 (cluster_id integer, canvec_fid integer, gnb_fid integer, nbrn_fid integer);

insert into nb_data.temp2(cluster_id, canvec_fid, gnb_fid, nbrn_fid)
select cluster_id, canvec_fid, gnb_fid, nbrn_fid from nb_data.temp1 where cluster_id in (
select cluster_id from nb_Data.temp1 group by cluster_id having count(*) = 1
);
delete from nb_data.temp1 where cluster_id in (select cluster_id from nb_Data.temp2);

insert into nb_data.temp2(cluster_id, nbrn_fid)
select cluster_id, max(nbrn_fid) as nbrn_fid from nb_data.temp1 where nbrn_fid is not null group by cluster_id;

delete from nb_data.temp1 where cluster_id in (select cluster_id from nb_Data.temp2);

insert into nb_data.temp2(cluster_id, gnb_fid)
select cluster_id, max(gnb_fid) as gnb_fid from nb_data.temp1 where gnb_fid is not null group by cluster_id;

delete from nb_data.temp1 where cluster_id in (select cluster_id from nb_Data.temp2);

insert into nb_data.temp2(cluster_id, canvec_fid)
select cluster_id, max(canvec_fid) as canvec_fid from nb_data.temp1 where canvec_fid is not null group by cluster_id;

delete from nb_data.temp1 where cluster_id in (select cluster_id from nb_Data.temp2);

--should be nothing left
select * from nb_Data.temp1;

--temp 2 should have only a single row for each cluster
select cluster_id, count(*) from nb_data.temp2 group by cluster_id having count(*) > 1;


-- add ids from the various layers
alter table nb_data.temp2 add column nbrn_nid varchar(32);
update nb_data.temp2 set nbrn_nid = a.nid from nb_data.nbrn_roads a where a.fid = nb_data.temp2.nbrn_fid;

alter table nb_data.temp2 add column canvec_feature_id varchar(64);
update nb_data.temp2 set canvec_feature_id = a.feature_id from nb_data.canvec_rail a where a.fid = nb_data.temp2.canvec_fid;

alter table nb_data.temp2 add column gnb_objectid int;
update nb_data.temp2 set gnb_objectid = a.objectid from nb_data.gnb_roads a where a.fid = nb_data.temp2.gnb_fid;


alter table nb_data.temp2 add column transport_feature_id varchar(64);
update nb_Data.temp2 set transport_feature_id = 
case when nbrn_nid is not null then nbrn_nid 
when canvec_feature_id is not null then canvec_feature_id 
when gnb_objectid is not null then cast(gnb_objectid as varchar)
else null end;

alter table nb_data.temp2 add column transport_feature_source varchar(6);


update nb_data.temp2 set transport_feature_source =
case when nbrn_fid is not null then 'NBRN'
when canvec_fid is not null then 'CANVEC'
when gnb_fid is not null then 'GNB'
else null end;

alter table nb_data.temp2 add column transport_feature_type varchar(15);

update nb_data.temp2 set transport_feature_type = 'resource_road'
from nb_data.nbrn_roads a
where a.fid = nb_data.temp2.nbrn_fid
and nb_data.temp2.nbrn_fid is not null and 
a.func_road_class in ('NBDNR Resource Road F6',
'NBDNR Resource Road F3'
'NBDNR Resource Road F5'
'NBDNR Resource Road F1'
'NBDNR Resource Road F2'
'NBDNR Resource Road F4'
);

update nb_data.temp2 set transport_feature_type = 'road' where transport_feature_type is null and nbrn_fid is not null;
update nb_data.temp2 set transport_feature_type = 'rail' where transport_feature_type is null and canvec_fid is not null;
update nb_data.temp2 set transport_feature_type = 'resource_road' where transport_feature_type is null and gnb_fid is not null;

alter table nb_data.temp2 add column transport_feature_name varchar;

update nb_data.temp2 set transport_feature_name = a.formatted_street_name 
from nb_data.nbrn_roads a
where a.fid = nb_data.temp2.nbrn_fid
and nb_data.temp2.nbrn_fid is not null;

update nb_data.temp2 set transport_feature_name = a.track_name_en 
from nb_data.canvec_rail a
where a.fid = nb_data.temp2.canvec_fid and transport_feature_name is null 
and nb_data.temp2.canvec_fid is not null;

alter table nb_data.temp2 add column crossing_type varchar(255);
alter table nb_data.temp2 add column route_name_1 varchar(100);
alter table nb_data.temp2 add column route_name_2 varchar(100);
alter table nb_data.temp2 add column roadway_type varchar(255);
alter table nb_data.temp2 add column roadway_paved_status varchar(255);
alter table nb_data.temp2 add column roadway_surface varchar(255);


update nb_data.temp2 set 
crossing_type = case when a.struc_type in ('Bridge Unknown', 'Covered Bridge') then 'Bridge' else a.struc_type end,
route_name_1 = a.route_name_english_1,
route_name_2 = a.route_name_english_2,
roadway_type = a.func_road_class,
roadway_paved_status = a.paved_status,
roadway_surface = a.unpaved_road_surf_type 
from nb_data.nbrn_roads a
where a.fid = nb_data.temp2.nbrn_fid
and nb_data.temp2.nbrn_fid is not null;

alter table nb_data.temp2 add column transport_feature_owner  varchar(100);
alter table nb_data.temp2 add column railway_operator  varchar(100);
alter table nb_data.temp2 add column num_railway_tracks integer;
alter table nb_data.temp2 add column transport_feature_condition  varchar(16);


update nb_data.temp2 set 
transport_feature_owner = a.track_owner_name_en,
railway_operator = a.operatr_name_en,
num_railway_tracks = a.number_of_tracks,
transport_feature_condition = 
case when a.track_status = 20 then 'unknown' when a.track_status = 467 then 'discontinued' when a.track_status = 468 then 'operational' else null end
from nb_data.canvec_rail a
where a.fid = nb_data.temp2.canvec_fid
and nb_data.temp2.canvec_fid is not null;


alter table nb_data.modelled_crossings add column transport_feature_source varchar(6);
alter table nb_data.modelled_crossings add constraint feature_source_ch CHECK (transport_feature_source in ('NBRN', 'CANVEC', 'GNB'));

alter table nb_data.modelled_crossings add column transport_feature_type varchar(15);
alter table nb_data.modelled_crossings add constraint feature_type_ch CHECK (transport_feature_type in ('road', 'resource_road', 'rail', 'trail'));

alter table nb_data.modelled_crossings add column transport_feature_name varchar;

alter table nb_data.modelled_crossings add column crossing_type varchar(255);
alter table nb_data.modelled_crossings add column route_name_1 varchar(100);
alter table nb_data.modelled_crossings add column route_name_2 varchar(100);
alter table nb_data.modelled_crossings add column roadway_type varchar(255);
alter table nb_data.modelled_crossings add column roadway_paved_status varchar(255);
alter table nb_data.modelled_crossings add column roadway_surface varchar(255);

alter table nb_data.modelled_crossings add column transport_feature_owner  varchar(100);
alter table nb_data.modelled_crossings add column railway_operator  varchar(100);
alter table nb_data.modelled_crossings add column num_railway_tracks integer;
alter table nb_data.modelled_crossings add column transport_feature_condition varchar(16);
alter table nb_data.modelled_crossings add constraint feature_condition_ch CHECK (transport_feature_condition in ('unknown', 'discontinued', 'operational'));

update nb_data.modelled_crossings set 
transport_feature_source = a.transport_feature_source,
transport_feature_type = a.transport_feature_type,
transport_feature_name = a.transport_feature_name,
crossing_type = a.crossing_type,
route_name_1 = a.route_name_1,
route_name_2 = a.route_name_2,
roadway_type = a.roadway_type,
roadway_paved_status = a.roadway_paved_status,
roadway_surface = a.roadway_surface,
transport_feature_owner = a.transport_feature_owner,
railway_operator = a.railway_operator,
num_railway_tracks = a.num_railway_tracks,
transport_feature_condition = a.transport_feature_condition
from nb_data.temp2 a where a.cluster_id = nb_data.modelled_crossings.cluster_id; 

alter table nb_data.modelled_crossings add column modelled_crossing_id uuid;
update nb_data.modelled_crossings set modelled_crossing_id = gen_random_uuid();
alter table nb_data.modelled_crossings add primary key (modelled_crossing_id);

drop table if exists nb_Data.temp2;
drop table if exists nb_Data.temp1;