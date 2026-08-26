-- See: https://github.com/Canadian-Wildlife-Federation/CHYF/issues/30
-- Notes: in the sql below you need to configure the AOI's of interest

create table public.new_nodes_temp as
with aoiids as (
  select id from fpoutput.aoi where name in 
  ('XXXX', 'YYYYY')
),
all_records as (
  select id, st_startpoint(geometry) as geometry from fpoutput.eflowpath where aoi_id in (select id from aoiids)
  union all
  select id, st_endpoint(geometry) as geometry from fpoutput.eflowpath where aoi_id in (select id from aoiids)
)
select id, geometry, st_x(geometry) as x, st_y(geometry) as y
from all_records;


-- add indexes
create index new_nodes_gix on public.new_nodes_temp using gist (geometry);
analyze public.new_nodes_temp;

--search for close nodes within these aois:
create table public.new_node_temp_inaois as
select a.id as a_id, b.id as b_id, a.geometry as a_geometry, b.geometry as b_geometry
from public.new_nodes_temp a, public.new_nodes_temp b
where a.id < b.id and ST_DWithin(a.geometry, b.geometry, 0.0000001) and (a.x != b.x or a.y != b.y);

-- Search for close nodes between these new data and the existing nexus data
create table public.new_node_temp_chyf2 as
select a.id as a_id, b.id as b_id, a.geometry as a_geometry, b.geometry as b_geometry
from public.new_nodes_temp a, chyf2.nexus b
where a.id < b.id and ST_DWithin(a.geometry, b.geometry, 0.0000001) and (a.x != b.x or a.y != b.y);
