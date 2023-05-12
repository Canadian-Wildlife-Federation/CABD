import psycopg2 as pg2
import sys
import subprocess

dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

#dbHost = "localhost"
#dbPort = "5432"
#dbName = "cabd3"
dbUser = sys.argv[1]
dbPassword = sys.argv[2]

#output data schema
schema = "nb_data_test"

#chyf aois that cover road network "'<uuid>,'<uuid>'"
aois = "'d58f808d-72ca-4652-9a0a-26b1763f4a9f'"

#the meters based projection for clustering
#the cluster distance will be based on this projection so this
#should be appropriate for the data 
mSRID = 2953
#cabd geometry srid 
cabdSRID = 4617
#distance in meters (mSRID projection units) for clustering points
clusterDistance = 15  

#each of these tables must have an id and geometry field names with the
#same name as the variables below
#if one of these tables doesn't exist for your dataset, set
#the value to None (railTable = None)
#the geometry fields need geometry indexes
railTable = "rail"
roadsTable = "roads"
resourceRoadsTable = None
trailTable = "trails"
#geometry and unique id fields from the above tables
#id MUST be an integer 
geometry = "geometry"
id = "fid"

#chyf stream data
streamTable = "eflowpath"
#use max_uplength from this table
#to determine "largest" stream
streamPropTable = "eflowpath_properties"


#all source transport layers to be used for computing crossings
layers = [railTable, roadsTable, resourceRoadsTable, trailTable];

#non-rail layers - these are included in the clustering
#these should be in order of priority for assigning ids to point
nonRailLayers = [roadsTable, resourceRoadsTable, trailTable];
railLayers = [railTable];

#prioritize roads layer over other layers in clusters for determining what cluster point to keep
priorityLayer=roadsTable


def executeQuery(connection, sql):
    #print (sql)
    with connection.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()
    
def checkEmpty(connection, sql, error):    
    with connection.cursor() as cursor:
        cursor.execute(sql)
        count = cursor.fetchone()
        if (count[0] != 0):
            print ("ERROR: " + error);
            sys.exit(-1);
    

# -- MAIN SCRIPT --  

print("Connecting to database...")

conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

# ----
# -- Copying Over CHyF Data
# ----

#determine if stream network data exists 
#if not copy over data from chyf using fdw
sql = f"SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = '{schema}' and tablename = '{streamTable}'"
copystreams = True
with conn.cursor() as cursor:
    cursor.execute(sql)
    count = cursor.fetchone()
    if (count[0] != 0):
        copystreams = False

if copystreams:
    sql = f"""
        DROP TABLE IF EXISTS {schema}.{streamTable};
        DROP TABLE IF EXISTS {schema}.{streamPropTable};
        
        CREATE TABLE {schema}.{streamTable} as SELECT * FROM chyf_flowpath WHERE aoi_id IN ({aois});
        CREATE TABLE {schema}.{streamPropTable} as SELECT * FROM chyf_flowpath_properties WHERE aoi_id IN ({aois});
        
        CREATE INDEX {schema}_{streamTable}_{geometry} on {schema}.{streamTable} using gist({geometry}); 
        CREATE INDEX {schema}_{streamTable}_id on {schema}.{streamTable} (id);
        CREATE INDEX {schema}_{streamPropTable}_id on {schema}.{streamPropTable} (id);
        
        ANALYZE {schema}.{streamPropTable};
        ANALYZE {schema}.{streamTable};

    """
    print(sql)
    executeQuery(conn, sql)


sql = f"SELECT case when count(*) = 0 then 1 else 0 end FROM {schema}.{streamTable}"
checkEmpty(conn, sql, "stream table is empty");

sql = f"SELECT case when count(*) = 0 then 1 else 0 end FROM {schema}.{streamPropTable}"
checkEmpty(conn, sql, "stream property table is empty");


# ----
# -- Compute Crossings For All Layers
# ----
for layer in layers:
    if (layer is None):
        continue;
    
    print("Computing crossings for " + layer)
    
    computeCrossing = f"""
        DROP TABLE IF EXISTS {schema}.{layer}_crossings;
    
        CREATE TABLE {schema}.crossing_temp as 
        SELECT a.{id} as {id}, b.id as eflowpath_id, st_intersection(a.{geometry}, b.{geometry}) as {geometry}
        FROM {schema}.{layer} a, {schema}.{streamTable} b
        WHERE st_intersects(a.{geometry}, b.{geometry});
        
        CREATE TABLE {schema}.{layer}_crossings as
        SELECT {id} as {id}, eflowpath_id, (st_dump({geometry})).geom as {geometry} 
        FROM {schema}.crossing_temp;
        
        DROP TABLE {schema}.crossing_temp;
    """
    executeQuery(conn, computeCrossing);
    
    
# ----
# -- combine all crossings EXCEPT rail crossings into a single layer-- COMBINE ALL
# ----
print("Combining crossings into single layer")

sql = f'DROP TABLE IF EXISTS {schema}.all_crossings;'
sql += f'CREATE TABLE {schema}.all_crossings (id serial, eflowpath_id uuid,'
for layer in nonRailLayers:
    if (layer is None):
        continue;
    sql = sql + f'{layer}_{id} integer, '
sql = sql + f'{geometry} geometry(POINT, {cabdSRID})); '

idFields = "";
for layer in nonRailLayers:
    if (layer is None):
        continue;
    idFields = idFields + f'z.{layer}_{id},'
    sql = sql + f'INSERT INTO {schema}.all_crossings ({layer}_{id}, eflowpath_id, {geometry}) SELECT {id}, eflowpath_id, {geometry} FROM {schema}.{layer}_crossings; '

sql += f'CREATE INDEX all_crossings_geometry on {schema}.all_crossings using gist({geometry}); ANALYZE {schema}.all_crossings'
executeQuery(conn, sql)
idFields = idFields[:-1]

# ----
# -- Cluster Points
# ----
print("Cluster Points")
sql = f"""
-- convert geometry to equal area projecion so we can cluster by distances

ALTER TABLE  {schema}.all_crossings ADD COLUMN {geometry}_m geometry(point, {mSRID});
UPDATE {schema}.all_crossings set {geometry}_m = st_transform({geometry}, {mSRID}) ;


DROP TABLE IF EXISTS {schema}.cluster_1 ;

CREATE TABLE {schema}.cluster_1 AS
SELECT unnest(st_clusterwithin(geometry_m, {clusterDistance})) as {geometry} FROM {schema}.all_crossings;

--add a cluster id
ALTER TABLE  {schema}.cluster_1 add column cluster_id serial;

--explode clusters
DROP TABLE IF EXISTS {schema}.cluster_2 ;

CREATE TABLE {schema}.cluster_2 AS  
SELECT st_geometryn({geometry}, 1) as {geometry}, st_numgeometries({geometry}) as num_merged, cluster_id 
FROM {schema}.cluster_1;


-- FIND CLUSTER POINT TO RETAIN
-- clean up clusters

DROP TABLE IF EXISTS {schema}.cluster_by_id ;

CREATE TABLE {schema}.cluster_by_id as 
SELECT cluster_id, st_geometryn({geometry}, generate_series(1, st_numgeometries({geometry}))) as {geometry}
FROM {schema}.cluster_1;


DROP TABLE IF EXISTS {schema}.cluster_by_id_with_data;

CREATE TABLE {schema}.cluster_by_id_with_data as 
SELECT a.cluster_id, a.{geometry}, z.eflowpath_id, {idFields}
FROM  {schema}.cluster_by_id a left join {schema}.all_crossings z 
on a.{geometry} = z.{geometry}_m ;
""";

executeQuery(conn, sql);


#QA check - this should return nothing
sql = f"SELECT count(*) FROM {schema}.cluster_by_id_with_data WHERE eflowpath_id is null"
checkEmpty(conn, sql, "cluster_by_id_with_data table should not have any rows with null eflowpath_id");

print("Extracting single points from cluster")
sql = f"""
DROP TABLE IF EXISTS {schema}.crossing_points;

--create an output table of clusters to keep
--start with clusters of a single point

CREATE TABLE {schema}.crossing_points AS  
SELECT cluster_id, geometry, eflowpath_id 
FROM {schema}.cluster_by_id_with_data 
WHERE cluster_id IN (
    SELECT cluster_id 
    FROM {schema}.cluster_by_id_with_data 
    GROUP BY cluster_id HAVING count(*) = 1
);

--remove these from processing table
DELETE FROM {schema}.cluster_by_id_with_data 
WHERE cluster_id IN (SELECT cluster_id FROM {schema}.crossing_points);
"""

executeQuery(conn, sql);


print("Computing point to keep for cluster with priority layer crossings")
# ----
# -- FIND POINT TO KEEP IN REMAINING CLUSTERS
# ----

#FIRST - process the "priority layer" - finding the most downstream point
# on the "biggest" stream using max upstreamm length as the proxy for "biggest" 

sql = f"""

--- find the largest stream on prioritizing road segments ---

DROP TABLE IF EXISTS {schema}.cluster_{priorityLayer}_id;

CREATE TABLE {schema}.cluster_{priorityLayer}_id as 
SELECT * FROM {schema}.cluster_by_id_with_data 
WHERE {priorityLayer}_{id} is not null; 

--for all these ones where there is only one 
--use this as the cluster point 
INSERT INTO {schema}.crossing_points 
SELECT cluster_id, {geometry}, eflowpath_id 
FROM {schema}.cluster_{priorityLayer}_id 
WHERE cluster_id in (
    SELECT cluster_id FROM {schema}.cluster_{priorityLayer}_id GROUP BY cluster_id HAVING count(*) = 1
);

--remove these from working data sets 
DELETE FROM {schema}.cluster_by_id_with_data WHERE cluster_id IN (select cluster_id FROM {schema}.crossing_points);
DELETE FROM {schema}.cluster_{priorityLayer}_id WHERE cluster_id IN (select cluster_id FROM {schema}.crossing_points);

-- for the remaining add location on line
ALTER TABLE  {schema}.cluster_{priorityLayer}_id ADD COLUMN point_on_line double precision;
UPDATE {schema}.cluster_{priorityLayer}_id
 set point_on_line = st_linelocatepoint(e.{geometry}, st_transform(cluster_{priorityLayer}_id.{geometry}, {cabdSRID}))
FROM {schema}.{streamTable} e where e.id = {schema}.cluster_{priorityLayer}_id.eflowpath_id;

--add upstream length
ALTER TABLE  {schema}.cluster_{priorityLayer}_id ADD COLUMN upstream_length double precision;

UPDATE {schema}.cluster_{priorityLayer}_id 
SET upstream_length = CASE WHEN a.max_uplength IS NULL THEN b.length ELSE a.max_uplength + b.length END
FROM {schema}.{streamPropTable} a JOIN {schema}.{streamTable} b on a.id = b.id 
where {schema}.cluster_{priorityLayer}_id.eflowpath_id = a.id;

--case statement above is to deal with secondaries - these have no upstream length
--there were a few cases where these were crossed by they were all on the same edge so it 
--doesn't matter
--we are using this up length to determine which edge to pick when there is more than
--one stream edge being crossed

--for each cluster we need edge id with the maximum 
DROP TABLE IF EXISTS {schema}.temp3;
CREATE TABLE {schema}.temp3 AS 
SELECT cluster_id, max(upstream_length) as max_length 
FROM {schema}.cluster_{priorityLayer}_id group by cluster_id;

--map the cluster to the edge
DROP TABLE IF EXISTS {schema}.temp4;
CREATE TABLE {schema}.temp4 AS 
SELECT distinct a.cluster_id, a.max_length, b.eflowpath_id 
FROM {schema}.cluster_{priorityLayer}_id b, {schema}.temp3 a
WHERE a.cluster_id = b.cluster_id and b.upstream_length = a.max_length;

--for the flowpath we want to find the most downstream point in the cluster
DROP TABLE IF EXISTS {schema}.temp5;
CREATE TABLE {schema}.temp5 AS 
SELECT a.cluster_id , max(point_on_line) as min_pol
FROM {schema}.cluster_{priorityLayer}_id a JOIN {schema}.temp4 b on a.cluster_id = b.cluster_id and a.eflowpath_id = b.eflowpath_id
GROUP BY a.cluster_id;
"""

executeQuery(conn, sql);

#should be empty
sql = f"SELECT COUNT(*) FROM (select cluster_id from {schema}.cluster_{priorityLayer}_id except select cluster_id from {schema}.temp5) foo";
checkEmpty(conn, sql, "error when computing clusters with priority layer - error with temporary table 5");

sql = f"""
--merge to find downstream point
DROP TABLE IF EXISTS {schema}.temp6;
CREATE TABLE {schema}.temp6 AS 
SELECT distinct a.cluster_id, a.geometry, a.eflowpath_id 
FROM {schema}.cluster_{priorityLayer}_id a JOIN {schema}.temp5 b on a.cluster_id  = b.cluster_id
and a.point_on_line = b.min_pol;
"""
executeQuery(conn, sql);

#should be empty
sql = f" SELECT count(*) FROM (SELECT cluster_id FROM {schema}.cluster_{priorityLayer}_id except SELECT cluster_id FROM {schema}.temp6) foo";
checkEmpty(conn, sql, "error when computing clusters with priority layer - error with temporary table 6");

sql = f"""
--add to main table and remove from processing
INSERT INTO {schema}.crossing_points (cluster_id, geometry, eflowpath_id)
SELECT cluster_id, geometry, eflowpath_id FROM {schema}.temp6;

--remove these from nrbn data & cluster_by_id_with_data
DELETE FROM {schema}.cluster_by_id_with_data WHERE cluster_id IN (SELECT cluster_id FROM {schema}.crossing_points);
DELETE FROM {schema}.cluster_{priorityLayer}_id WHERE cluster_id IN (SELECT cluster_id FROM {schema}.crossing_points);
"""
executeQuery(conn, sql);

#should be empty
sql = f"select count(*) from {schema}.cluster_{priorityLayer}_id;";
checkEmpty(conn, sql, "error when computing clusters with priority layer - not all clusters with point from the priority layer were processed");

sql = f"""
DROP TABLE {schema}.temp6;
DROP TABLE {schema}.temp5;
DROP TABLE {schema}.temp4;
DROP TABLE {schema}.temp3;
DROP TABLE {schema}.cluster_{priorityLayer}_id ;
"""
executeQuery(conn, sql);


print("Computing point to keep for cluster with remaining clusters")
#SECOND - process the remaining layers - finding the most downstream point
# on the "biggest" stream using max upstreamm length as the proxy for "biggest" 


sql = f"""
-- add location on line
ALTER TABLE  {schema}.cluster_by_id_with_data ADD COLUMN point_on_line double precision;

UPDATE {schema}.cluster_by_id_with_data 
SET point_on_line = st_linelocatepoint(e.{geometry}, st_transform(cluster_by_id_with_data.{geometry}, {cabdSRID}))
FROM {schema}.{streamTable} e 
WHERE e.id = {schema}.cluster_by_id_with_data.eflowpath_id ;

ALTER TABLE {schema}.cluster_by_id_with_data ADD COLUMN upstream_length double precision;

UPDATE {schema}.cluster_by_id_with_data 
SET upstream_length = CASE WHEN a.max_uplength is null THEN b.length ELSE a.max_uplength + b.length END
FROM {schema}.{streamPropTable} a join {schema}.{streamTable} b on a.id = b.id 
WHERE {schema}.cluster_by_id_with_data.eflowpath_id = a.id;
--case statement is to deal with secondaries - these have no upstream length
--there were a few cases where these were crossed by they were all on the same edge so it 
--doesn't matter

--for each cluster we need edge id with the maximum 
DROP TABLE IF EXISTS {schema}.temp3;
CREATE TABLE {schema}.temp3 AS
SELECT cluster_id, max(upstream_length) as max_length from {schema}.cluster_by_id_with_data 
GROUP BY cluster_id;


--map the cluster to the edge
DROP TABLE IF EXISTS {schema}.temp4;
CREATE TABLE {schema}.temp4 AS
SELECT distinct a.cluster_id, a.max_length, b.eflowpath_id 
FROM {schema}.cluster_by_id_with_data b, {schema}.temp3 a
WHERE a.cluster_id = b.cluster_id and b.upstream_length = a.max_length;


--for the flowpath we want to find the most downstream point in the cluster
DROP TABLE IF EXISTS {schema}.temp5;
CREATE TABLE {schema}.temp5 as 
SELECT a.cluster_id , max(point_on_line) as min_pol
FROM {schema}.cluster_by_id_with_data a JOIN {schema}.temp4 b on a.cluster_id = b.cluster_id and a.eflowpath_id = b.eflowpath_id
GROUP BY a.cluster_id;

"""
executeQuery(conn, sql)

#should be empty
sql = f"select count(*) FROM (select cluster_id from {schema}.cluster_by_id_with_data cni except select cluster_id from {schema}.temp5) foo"
checkEmpty(conn, sql, "Not all cluster points processed - error with temp5");

sql = f"""
DROP TABLE IF EXISTS {schema}.temp6;

CREATE TABLE {schema}.temp6 AS
SELECT distinct a.cluster_id, a.geometry, a.eflowpath_id 
FROM {schema}.cluster_by_id_with_data a JOIN {schema}.temp5 b on a.cluster_id  = b.cluster_id
and a.point_on_line = b.min_pol;
"""
executeQuery(conn, sql)

#should be empty
sql = f"select count(*) FROM (SELECT cluster_id from {schema}.cluster_by_id_with_data cni except select cluster_id from {schema}.temp6) foo"
checkEmpty(conn, sql, "Not all cluster points processed - error with temp6");


sql = f"""
INSERT INTO {schema}.crossing_points (cluster_id, geometry, eflowpath_id)
SELECT cluster_id, geometry, eflowpath_id FROM {schema}.temp6;

--remove these from cluster_by_id_with_data
DELETE FROM {schema}.cluster_by_id_with_data WHERE cluster_id IN (SELECT cluster_id FroM {schema}.crossing_points);
"""
executeQuery(conn, sql)

#should be empty
sql = f"select count(*) from {schema}.cluster_by_id_with_data";
checkEmpty(conn, sql, "error when computing clusters - not all clusters were processed");

sql = f"select count(*) FROM (select cluster_id, count(*) from {schema}.crossing_points group by cluster_id having count(*) > 1) foo";
checkEmpty(conn, sql, "error when computing clusters - multiple points from cluster retained");

sql = f"""
DROP TABLE {schema}.temp6;
DROP TABLE {schema}.temp5;
DROP TABLE {schema}.temp4;
DROP TABLE {schema}.temp3;
DROP TABLE {schema}.cluster_by_id_with_data ;
"""
executeQuery(conn, sql);

sql = f"""
alter table {schema}.crossing_points rename column {geometry} to {geometry}_m;
alter table {schema}.crossing_points add column {geometry} geometry(point, {cabdSRID});
update {schema}.crossing_points set {geometry} = st_Transform({geometry}_m, {cabdSRID});
"""

executeQuery(conn, sql);

print("Adding source data ids to crossing points")

fieldswithtype = ""
fields = "";
for layer in nonRailLayers:
    if (layer is None):
        continue;
    fieldswithtype += f'{layer}_{id} integer,'
    fields += f'{layer}_{id},'
fields = fields[:-1]
fieldswithtype = fieldswithtype[:-1]

sql = f"""
--figure out which feature id to use for cluster details
DROP TABLE IF EXISTS {schema}.temp1;

CREATE TABLE {schema}.temp1 as 
select a.cluster_id, b.*
from {schema}.crossing_points a, {schema}.all_crossings b 
where a.geometry_m = b.geometry_m;

--deal with duplicates
DROP TABLE IF EXISTS {schema}.temp2;
CREATE TABLE {schema}.temp2 (cluster_id integer, {fieldswithtype});

INSERT INTO {schema}.temp2(cluster_id, {fields})
SELECT cluster_id, {fields} 
FROM {schema}.temp1 
WHERE cluster_id in (
        select cluster_id from {schema}.temp1 GROUP BY cluster_id HAVING count(*) = 1
);
DELETE FROM {schema}.temp1 WHERE cluster_id in (SELECT cluster_id FROM {schema}.temp2);
"""
executeQuery(conn, sql)

idcasesql = "";
typecasesql = "";
for layer in nonRailLayers:
    if (layer is None):
        continue;
    idcasesql += f'WHEN {layer}_{id} IS NOT NULL THEN {layer}_{id} '
    typecasesql += f"WHEN {layer}_{id} IS NOT NULL THEN '{layer}' "
    
    sql = f"""
        INSERT INTO {schema}.temp2(cluster_id, {layer}_{id})
        SELECT cluster_id, max({layer}_{id}) as {layer}_{id} 
        FROM {schema}.temp1 
        WHERE {layer}_{id} IS NOT NULL GROUP BY cluster_id;

        DELETE FROM {schema}.temp1 WHERE cluster_id IN (SELECT cluster_id FROM {schema}.temp2);
        """
    executeQuery(conn, sql)

checkEmpty(conn, f"select count(*) FROM {schema}.temp1", "error when computing source feature for each modelled crossing - some points not processed");
checkEmpty(conn, f"select count(*) FROM ( select cluster_id, count(*) from {schema}.temp2 group by cluster_id having count(*) > 1 ) foo", "error when computing source feature for each modelled crossing - multiple values found");


sql = f"""

ALTER TABLE {schema}.crossing_points add column transport_feature_id integer;
ALTER TABLE {schema}.crossing_points add column transport_feature_source varchar(64);

UPDATE {schema}.crossing_points SET 
transport_feature_id = CASE {idcasesql} ELSE NULL END,
transport_feature_source = CASE {typecasesql} ELSE NULL END
FROM {schema}.temp2 
WHERE {schema}.temp2.cluster_id = {schema}.crossing_points.cluster_id;

DROP TABLE {schema}.temp1;
DROP TABLE {schema}.temp2; 
"""
executeQuery(conn, sql)

for layer in railLayers:
    print("Adding {layer} crossings to crossing points")

    sql = f"""
    INSERT INTO {schema}.crossing_points (transport_feature_id, transport_feature_source, eflowpath_id, geometry)
    SELECT {id}, '{layer}', eflowpath_id, geometry 
    FROM {schema}.{layer}_crossings;
    """
    executeQuery(conn, sql);

sql = f"select count(*) FROM {schema}.crossing_points WHERE transport_feature_id is null or transport_feature_source is null";
checkEmpty(conn, sql, "a cluster was found without any source transport layer");

#clean up
sql = f"""
DROP TABLE {schema}.cluster_1;
DROP TABLE {schema}.cluster_2;
DROP TABLE {schema}.cluster_by_id;
"""
executeQuery(conn, sql);

print(f"** PROCESSING COMPLETE ** results in {schema}.crossing_points table")