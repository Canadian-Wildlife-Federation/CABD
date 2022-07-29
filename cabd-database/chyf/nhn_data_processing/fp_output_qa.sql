---------------------- NODE OUTPUT RANK TEST ---------------------------------------------
-- this query finds all nodes that have more than one primary output 
-- this violates the chyf model
-- this query checks all aois - uncomment the line specified below to test single aoi
-----------------------------------------------------------------------------------  
SELECT st_startpoint(a.geometry) as pnt, count(*)
    FROM fpoutput.eflowpath a join fpoutput.aoi b on a.aoi_id = b.id
    WHERE a.rank = 1 
    -- uncomment this line to filter on NHN
    --AND b.name = '02HB000'
GROUP BY pnt
HAVING count(*) > 1


------------------- DEGREE 2 NODE RANK TEST --------------------------------------
-- this query finds all degree 2 nodes (ignoring bank edges) where the input
-- is primary and the output is secondary
-- this violates the chyf model
-- this query checks all aois - uncomment the line specified below to test single aoi
----------------------------------------------------------------------------------- 
with single_out_node as (
    SELECT st_startpoint(a.geometry) as pnt, count(*)
    from fpoutput.eflowpath a join fpoutput.aoi b on a.aoi_id = b.id
    WHERE a.ef_type != 2 
    -- uncomment this line to filter on NHN
    --AND b.name = '02HB000'
    GROUP BY pnt
	HAVING count(*) = 1
),
single_in_node as (
    SELECT st_endpoint(a.geometry) as pnt, count(*)
    from fpoutput.eflowpath a join fpoutput.aoi b on a.aoi_id = b.id
    WHERE a.ef_type != 2 
    -- uncomment this line to filter on NHN
    --AND b.name = '02HB000'
    GROUP BY pnt
	HAVING count(*) = 1
)
select a.pnt, b.rank as out_rank, d.rank as in_rank
from single_out_node a join fpoutput.eflowpath b on a.pnt = st_Startpoint(b.geometry) and b.ef_type != 2
join single_in_node c on a.pnt = c.pnt join fpoutput.eflowpath d on c.pnt = st_endpoint(d.geometry) and d.ef_type != 2,
fpoutput.aoi e 
where b.rank = 1 and d.rank = 2 and b.aoi_id = d.aoi_id and e.id = d.aoi_id 
--AND e.name = '02HB000'


------------------------- FLOWPATHS OVERLAP ---------------------
-- this query finds flowpaths that overlap
-- this violates the chyf model
-- this query checks all aois - uncomment the line specified below to test single aoi
----------------------------------------------------------------------------------- 
select a.id, b.id, st_intersection(a.geometry, b.geometry) as pnt
from fpoutput.eflowpath a, fpoutput.eflowpath b, fpoutput.aoi c
WHERE a.id != b.id and a.geometry && b.geometry and st_relate(a.geometry, b.geometry, 'T********')
and a.aoi_id  = c.id and b.aoi_id  = c.id 
--and c.name = '02HB000'


------------------------- FLOWPATHS NEAR BUT NOT NODED ---------------------
-- this query finds flowpaths that are really close to each other but don't actually touch 
-- this is only a warning - doesn't violate model but may indicate noding error
-- this query checks all aois - uncomment the line specified below to test single aoi
----------------------------------------------------------------------------------- 
select a.id, b.id, st_closestpoint(a.geometry,b.geometry) as pnt
from fpoutput.eflowpath a, fpoutput.eflowpath b, fpoutput.aoi c
where  a.id != b.id and 
st_dwithin(a.geometry, b.geometry, 0.000001) and 
st_disjoint(a.geometry, b.geometry) 
and a.aoi_id = b.aoi_id and a.aoi_id  = c.id and b.aoi_id  = c.id 
--and c.name = '02HB000'
        


-------------------------  NON_SKELETON FLOWPATH IN WATERBODY ------------------
-- this query finds non skeleton flowpaths inside waterbodies
-- this violates the chyf model
-- this query checks all aois - uncomment the line specified below to test single aoi
-----------------------------------------------------------------------------------
select a.id, b.id, st_pointonsurface(st_intersection(a.geometry, b.geometry)) as pnt
from fpoutput.eflowpath a, fpoutput.ecatchment b, fpoutput.aoi c
WHERE a.ef_type != 3 and a.id != b.id and a.geometry && b.geometry and st_relate(a.geometry, b.geometry, 'T********')
and a.aoi_id = b.aoi_id and a.aoi_id  = c.id and b.aoi_id  = c.id 
--and c.name = '02HB000'

