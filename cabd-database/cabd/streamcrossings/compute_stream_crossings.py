import psycopg2 as pg2
import sys
import subprocess
import argparse
import configparser
from _ast import Continue


#-- PARSE COMMAND LINE ARGUMENTS --  
parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=False)
parser.add_argument('-user', type=str, help='the username to access the database')
parser.add_argument('-password', type=str, help='the password to access the database')
args = parser.parse_args()
configfile = "config.ini"
if (args.c):
    configfile = args.c

#-- READ PARAMETERS FOR CONFIG FILE -- 
config = configparser.ConfigParser()
config.read(configfile)

#database settings 
dbHost = config['DATABASE']['host']
dbPort = config['DATABASE']['port']
dbName = config['DATABASE']['name']
dbUser = args.user
dbPassword = args.password

#output data schema
schema = config['DATABASE']['data_schema']
cabdSRID = config['DATABASE']['cabdSRID']

mSRID  = config['SETTINGS']['mSRID']
mGeometry = config['SETTINGS']['mGeometry']
#distance in meters (mSRID projection units) for clustering points
clusterDistance = config['SETTINGS']['clusterDistance']

#chyf stream network aois as '<aoiuuid>','<aoiuuid>'
aoi_raw = config['SETTINGS']['aoi_raw']
aois = str(aoi_raw)[1:-1]


#data tables
#set to None if doesn't exist for data 
railTable = config['DATASETS']['railTable'].strip()
roadsTable = config['DATASETS']['roadsTable'].strip()
resourceRoadsTable = config['DATASETS']['resourceRoadsTable'].strip()
trailTable = config['DATASETS']['trailTable'].strip()

railTable = None if railTable == "None" else railTable
roadsTable = None if roadsTable == "None" else roadsTable
resourceRoadsTable = None if resourceRoadsTable == "None" else resourceRoadsTable
trailTable = None if trailTable == "None" else trailTable


railAttributes = config['DATASETS']['railAttributes'].strip()
trailAttributes = config['DATASETS']['trailAttributes'].strip()
roadAttributes = config['DATASETS']['roadAttributes'].strip()
resourceRoadsAttributes = config['DATASETS']['resourceRoadsAttributes'].strip()

#geometry and unique id fields from the above tables
#id MUST be an integer 
geometry = config['DATASETS']['geometryField'].strip()
id = config['DATASETS']['idField'].strip()

#chyf stream data
streamTable = config['CHYF']['streamTable'].strip()
streamPropTable = config['CHYF']['streamPropTable'].strip()
streamNameTable = config['CHYF']['streamNameTable'].strip()


#all source transport layers to be used for computing crossings
layers = [railTable, roadsTable, resourceRoadsTable, trailTable]

#non-rail layers - these are included in the clustering
#these should be in order of priority for assigning ids to point
nonRailLayers = [roadsTable, resourceRoadsTable, trailTable]
railLayers = [railTable]

#prioritize roads layer over other layers in clusters for determining what cluster point to keep
player = config['DATASETS']['priorityLayer']
if (player == "roadsTable"):
    priorityLayer=roadsTable
elif (player == "railTable"):
    priorityLayer=railTable
elif (player == "resourceRoadsTable"):
    priorityLayer=resourceRoadsTable    
elif (player == "trailTable"):
    priorityLayer=trailTable    


layers = []
allayers = config['DATASETS']['allLayers'].split(",")
for l in allayers:
    l = l.strip()
    if (l == "roadsTable"):
        layers.append(roadsTable)
    elif (l == "railTable"):
        layers.append(railTable)
    elif (l == "resourceRoadsTable"):
        layers.append(resourceRoadsTable)    
    elif (l == "trailTable"):
        layers.append(trailTable)    

nonRailLayers = []
allayers = config['DATASETS']['nonRailLayers'].split(",")
for l in allayers:
    l = l.strip()
    if (l == "roadsTable"):
        nonRailLayers.append(roadsTable)
    elif (l == "railTable"):
        nonRailLayers.append(railTable)
    elif (l == "resourceRoadsTable"):
        nonRailLayers.append(resourceRoadsTable)    
    elif (l == "trailTable"):
        nonRailLayers.append(trailTable)    

railLayers = []
allayers = config['DATASETS']['railLayers'].split(",")
for l in allayers:
    l = l.strip()
    if (l == "roadsTable"):
        railLayers.append(roadsTable)
    elif (l == "railTable"):
        railLayers.append(railTable)
    elif (l == "resourceRoadsTable"):
        railLayers.append(resourceRoadsTable)    
    elif (l == "trailTable"):
        railLayers.append(trailTable)    

print ("-- Processing Parameters --")
print (f"Database: {dbHost}:{dbPort}/{dbName}")
print (f"Data Schema: {schema}")
print (f"CABD SRID: {cabdSRID}")
print (f"CHyF Data: {streamTable} {streamPropTable} {streamNameTable} {aois}")
print (f"Meters Projection: {mSRID} ")
print (f"Cluster Distance: {clusterDistance} ")

print (f"Data Tables: {railTable} {roadsTable} {resourceRoadsTable} {trailTable} ")
print (f"Id/Geometry Fields: {id} {geometry}")

print (f"All Layers: {layers}")
print (f"Non Rail Layers: {nonRailLayers}")
print (f"Rail Layers: {railLayers}")
print (f"Priority Layer: {priorityLayer}")

print (f"Rail Attributes: {railAttributes}")
print (f"Trail Attributes: {trailAttributes}")
print (f"Road Attributes: {roadAttributes}")
print (f"Resource Road Attributes: {resourceRoadsAttributes}")
print ("----")

#--
#-- function to execute a query 
#--
def executeQuery(connection, sql):
    #print (sql)
    with connection.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()
    
#--
#-- checks if the first column of the first row
#-- of the query results is 0 otherwise
# -- ends the program
def checkEmpty(connection, sql, error):    
    with connection.cursor() as cursor:
        cursor.execute(sql)
        count = cursor.fetchone()
        if (count[0] != 0):
            print ("ERROR: " + error)
            sys.exit(-1)
    

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
#TO DO: update this to look for the matching AOI data instead of just the table
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
    #print(sql)
    print("Copying streams into schema...")
    executeQuery(conn, sql)


sql = f"SELECT case when count(*) = 0 then 1 else 0 end FROM {schema}.{streamTable}"
checkEmpty(conn, sql, "stream table is empty")

sql = f"SELECT case when count(*) = 0 then 1 else 0 end FROM {schema}.{streamPropTable}"
checkEmpty(conn, sql, "stream property table is empty")


# ----
# -- Compute Crossings For All Layers
# ----
for layer in layers:
    if (layer is None):
        continue
    
    print("Computing crossings for " + layer)
    
    computeCrossing = f"""
        DROP TABLE IF EXISTS {schema}.{layer}_crossings;
    
        CREATE TABLE {schema}.crossing_temp as 
        SELECT a.{id} as {id}, b.id as chyf_stream_id, b.rivernameid1, b.rivernameid2, st_intersection(a.{geometry}, b.{geometry}) as {geometry}
        FROM {schema}.{layer} a, {schema}.{streamTable} b
        WHERE st_intersects(a.{geometry}, b.{geometry});
        
        CREATE TABLE {schema}.{layer}_crossings as
        SELECT {id} as {id}, chyf_stream_id, rivernameid1, rivernameid2, (st_dump({geometry})).geom as {geometry} 
        FROM {schema}.crossing_temp;
        
        DROP TABLE {schema}.crossing_temp;
    """
    executeQuery(conn, computeCrossing)
    
    
# ----
# -- combine all crossings EXCEPT rail crossings into a single layer-- COMBINE ALL
# ----
print("Combining crossings into single layer")

sql = f'DROP TABLE IF EXISTS {schema}.all_crossings;'
sql += f'CREATE TABLE {schema}.all_crossings (id serial, chyf_stream_id uuid, rivernameid1 uuid, rivernameid2 uuid,'
for layer in nonRailLayers:
    if (layer is None):
        continue
    sql = sql + f'{layer}_{id} integer, '
sql = sql + f'{geometry} geometry(POINT, {cabdSRID})); '

idFields = ""
for layer in nonRailLayers:
    if (layer is None):
        continue
    idFields = idFields + f'z.{layer}_{id},'
    sql = sql + f'INSERT INTO {schema}.all_crossings ({layer}_{id}, chyf_stream_id, rivernameid1, rivernameid2, {geometry}) SELECT {id}, chyf_stream_id, rivernameid1, rivernameid2, {geometry} FROM {schema}.{layer}_crossings; '

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
SELECT unnest(st_clusterwithin({mGeometry}, {clusterDistance})) as {geometry} FROM {schema}.all_crossings;

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
SELECT a.cluster_id, a.{geometry}, z.chyf_stream_id, z.rivernameid1, z.rivernameid2, {idFields}
FROM  {schema}.cluster_by_id a left join {schema}.all_crossings z 
on a.{geometry} = z.{geometry}_m ;
"""

executeQuery(conn, sql)


#QA check - this should return nothing
sql = f"SELECT count(*) FROM {schema}.cluster_by_id_with_data WHERE chyf_stream_id is null"
checkEmpty(conn, sql, "cluster_by_id_with_data table should not have any rows with null chyf_stream_id")

print("Extracting single points from cluster")
sql = f"""
DROP TABLE IF EXISTS {schema}.modelled_crossings;

--create an output table of clusters to keep
--start with clusters of a single point

CREATE TABLE {schema}.modelled_crossings AS  
SELECT cluster_id, geometry, chyf_stream_id, rivernameid1, rivernameid2
FROM {schema}.cluster_by_id_with_data 
WHERE cluster_id IN (
    SELECT cluster_id 
    FROM {schema}.cluster_by_id_with_data 
    GROUP BY cluster_id HAVING count(*) = 1
);

--remove these from processing table
DELETE FROM {schema}.cluster_by_id_with_data 
WHERE cluster_id IN (SELECT cluster_id FROM {schema}.modelled_crossings);
"""

executeQuery(conn, sql)


print("Computing point to keep for cluster with priority layer crossings")
# ----
# -- FIND POINT TO KEEP IN REMAINING CLUSTERS
# ----

#FIRST - process the "priority layer" - finding the most downstream point
# on the "biggest" stream using max upstream length as the proxy for "biggest" 

sql = f"""

--- find the largest stream on prioritizing road segments ---

DROP TABLE IF EXISTS {schema}.cluster_{priorityLayer}_id;

CREATE TABLE {schema}.cluster_{priorityLayer}_id as 
SELECT * FROM {schema}.cluster_by_id_with_data 
WHERE {priorityLayer}_{id} is not null; 

--for all these ones where there is only one 
--use this as the cluster point 
INSERT INTO {schema}.modelled_crossings 
SELECT cluster_id, {geometry}, chyf_stream_id, rivernameid1, rivernameid2
FROM {schema}.cluster_{priorityLayer}_id 
WHERE cluster_id in (
    SELECT cluster_id FROM {schema}.cluster_{priorityLayer}_id GROUP BY cluster_id HAVING count(*) = 1
);

--remove these from working data sets 
DELETE FROM {schema}.cluster_by_id_with_data WHERE cluster_id IN (select cluster_id FROM {schema}.modelled_crossings);
DELETE FROM {schema}.cluster_{priorityLayer}_id WHERE cluster_id IN (select cluster_id FROM {schema}.modelled_crossings);

-- for the remaining add location on line
ALTER TABLE  {schema}.cluster_{priorityLayer}_id ADD COLUMN point_on_line double precision;
UPDATE {schema}.cluster_{priorityLayer}_id
 set point_on_line = st_linelocatepoint(e.{geometry}, st_transform(cluster_{priorityLayer}_id.{geometry}, {cabdSRID}))
FROM {schema}.{streamTable} e where e.id = {schema}.cluster_{priorityLayer}_id.chyf_stream_id;

--add upstream length
ALTER TABLE  {schema}.cluster_{priorityLayer}_id ADD COLUMN upstream_length double precision;

UPDATE {schema}.cluster_{priorityLayer}_id 
SET upstream_length = CASE WHEN a.max_uplength IS NULL THEN b.length ELSE a.max_uplength + b.length END
FROM {schema}.{streamPropTable} a JOIN {schema}.{streamTable} b on a.id = b.id 
where {schema}.cluster_{priorityLayer}_id.chyf_stream_id = a.id;

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
SELECT distinct a.cluster_id, a.max_length, b.chyf_stream_id, b.rivernameid1, b.rivernameid2
FROM {schema}.cluster_{priorityLayer}_id b, {schema}.temp3 a
WHERE a.cluster_id = b.cluster_id and b.upstream_length = a.max_length;

--for the flowpath we want to find the most downstream point in the cluster
DROP TABLE IF EXISTS {schema}.temp5;
CREATE TABLE {schema}.temp5 AS 
SELECT a.cluster_id , max(point_on_line) as min_pol
FROM {schema}.cluster_{priorityLayer}_id a JOIN {schema}.temp4 b on a.cluster_id = b.cluster_id and a.chyf_stream_id = b.chyf_stream_id
GROUP BY a.cluster_id;
"""

executeQuery(conn, sql)

#should be empty
sql = f"SELECT COUNT(*) FROM (select cluster_id from {schema}.cluster_{priorityLayer}_id except select cluster_id from {schema}.temp5) foo"
checkEmpty(conn, sql, "error when computing clusters with priority layer - error with temporary table 5")

sql = f"""
--merge to find downstream point
DROP TABLE IF EXISTS {schema}.temp6;
CREATE TABLE {schema}.temp6 AS 
SELECT distinct a.cluster_id, a.geometry, a.chyf_stream_id, a.rivernameid1, a.rivernameid2
FROM {schema}.cluster_{priorityLayer}_id a JOIN {schema}.temp5 b on a.cluster_id  = b.cluster_id
and a.point_on_line = b.min_pol;
"""
executeQuery(conn, sql)

#should be empty
sql = f" SELECT count(*) FROM (SELECT cluster_id FROM {schema}.cluster_{priorityLayer}_id except SELECT cluster_id FROM {schema}.temp6) foo"
checkEmpty(conn, sql, "error when computing clusters with priority layer - error with temporary table 6")

sql = f"""
--add to main table and remove from processing
INSERT INTO {schema}.modelled_crossings (cluster_id, geometry, chyf_stream_id, rivernameid1, rivernameid2)
SELECT cluster_id, geometry, chyf_stream_id, rivernameid1, rivernameid2 FROM {schema}.temp6;

--remove these from nrbn data & cluster_by_id_with_data
DELETE FROM {schema}.cluster_by_id_with_data WHERE cluster_id IN (SELECT cluster_id FROM {schema}.modelled_crossings);
DELETE FROM {schema}.cluster_{priorityLayer}_id WHERE cluster_id IN (SELECT cluster_id FROM {schema}.modelled_crossings);
"""
executeQuery(conn, sql)

#should be empty
sql = f"select count(*) from {schema}.cluster_{priorityLayer}_id;"
checkEmpty(conn, sql, "error when computing clusters with priority layer - not all clusters with point from the priority layer were processed")

sql = f"""
DROP TABLE {schema}.temp6;
DROP TABLE {schema}.temp5;
DROP TABLE {schema}.temp4;
DROP TABLE {schema}.temp3;
DROP TABLE {schema}.cluster_{priorityLayer}_id ;
"""
executeQuery(conn, sql)


print("Computing point to keep for cluster with remaining clusters")
#SECOND - process the remaining layers - finding the most downstream point
# on the "biggest" stream using max upstreamm length as the proxy for "biggest" 


sql = f"""
-- add location on line
ALTER TABLE  {schema}.cluster_by_id_with_data ADD COLUMN point_on_line double precision;

UPDATE {schema}.cluster_by_id_with_data 
SET point_on_line = st_linelocatepoint(e.{geometry}, st_transform(cluster_by_id_with_data.{geometry}, {cabdSRID}))
FROM {schema}.{streamTable} e 
WHERE e.id = {schema}.cluster_by_id_with_data.chyf_stream_id ;

ALTER TABLE {schema}.cluster_by_id_with_data ADD COLUMN upstream_length double precision;

UPDATE {schema}.cluster_by_id_with_data 
SET upstream_length = CASE WHEN a.max_uplength is null THEN b.length ELSE a.max_uplength + b.length END
FROM {schema}.{streamPropTable} a join {schema}.{streamTable} b on a.id = b.id 
WHERE {schema}.cluster_by_id_with_data.chyf_stream_id = a.id;
--case statement is to deal with secondaries - these have no upstream length
--there were a few cases where these were crossed but they were all on the same edge so it 
--doesn't matter

--for each cluster we need edge id with the maximum 
DROP TABLE IF EXISTS {schema}.temp3;
CREATE TABLE {schema}.temp3 AS
SELECT cluster_id, max(upstream_length) as max_length from {schema}.cluster_by_id_with_data 
GROUP BY cluster_id;


--map the cluster to the edge
DROP TABLE IF EXISTS {schema}.temp4;
CREATE TABLE {schema}.temp4 AS
SELECT distinct a.cluster_id, a.max_length, b.chyf_stream_id, b.rivernameid1, b.rivernameid2
FROM {schema}.cluster_by_id_with_data b, {schema}.temp3 a
WHERE a.cluster_id = b.cluster_id and b.upstream_length = a.max_length;


--for the flowpath we want to find the most downstream point in the cluster
DROP TABLE IF EXISTS {schema}.temp5;
CREATE TABLE {schema}.temp5 as 
SELECT a.cluster_id , max(point_on_line) as min_pol
FROM {schema}.cluster_by_id_with_data a JOIN {schema}.temp4 b on a.cluster_id = b.cluster_id and a.chyf_stream_id = b.chyf_stream_id
GROUP BY a.cluster_id;

"""
executeQuery(conn, sql)

#should be empty
sql = f"select count(*) FROM (select cluster_id from {schema}.cluster_by_id_with_data cni except select cluster_id from {schema}.temp5) foo"
checkEmpty(conn, sql, "Not all cluster points processed - error with temp5")

sql = f"""
DROP TABLE IF EXISTS {schema}.temp6;

CREATE TABLE {schema}.temp6 AS
SELECT distinct a.cluster_id, a.geometry, a.chyf_stream_id, a.rivernameid1, a.rivernameid2
FROM {schema}.cluster_by_id_with_data a JOIN {schema}.temp5 b on a.cluster_id  = b.cluster_id
and a.point_on_line = b.min_pol;
"""
executeQuery(conn, sql)

#should be empty
sql = f"select count(*) FROM (SELECT cluster_id from {schema}.cluster_by_id_with_data cni except select cluster_id from {schema}.temp6) foo"
checkEmpty(conn, sql, "Not all cluster points processed - error with temp6")


sql = f"""
INSERT INTO {schema}.modelled_crossings (cluster_id, geometry, chyf_stream_id, rivernameid1, rivernameid2)
SELECT cluster_id, geometry, chyf_stream_id, rivernameid1, rivernameid2 FROM {schema}.temp6;

--remove these from cluster_by_id_with_data
DELETE FROM {schema}.cluster_by_id_with_data WHERE cluster_id IN (SELECT cluster_id FroM {schema}.modelled_crossings);
"""
executeQuery(conn, sql)

#should be empty
sql = f"select count(*) from {schema}.cluster_by_id_with_data"
checkEmpty(conn, sql, "error when computing clusters - not all clusters were processed")

sql = f"select count(*) FROM (select cluster_id, count(*) from {schema}.modelled_crossings group by cluster_id having count(*) > 1) foo"
checkEmpty(conn, sql, "error when computing clusters - multiple points from cluster retained")

sql = f"""
DROP TABLE {schema}.temp6;
DROP TABLE {schema}.temp5;
DROP TABLE {schema}.temp4;
DROP TABLE {schema}.temp3;
DROP TABLE {schema}.cluster_by_id_with_data ;
"""
executeQuery(conn, sql)

sql = f"""
alter table {schema}.modelled_crossings rename column {geometry} to {geometry}_m;
alter table {schema}.modelled_crossings add column {geometry} geometry(point, {cabdSRID});
update {schema}.modelled_crossings set {geometry} = st_Transform({geometry}_m, {cabdSRID});
"""

executeQuery(conn, sql)

print("Adding source data ids to crossing points")

fieldswithtype = ""
fields = ""
for layer in nonRailLayers:
    if (layer is None):
        continue
    fieldswithtype += f'{layer}_{id} integer,'
    fields += f'{layer}_{id},'
fields = fields[:-1]
fieldswithtype = fieldswithtype[:-1]

sql = f"""
--figure out which feature id to use for cluster details
DROP TABLE IF EXISTS {schema}.temp1;

CREATE TABLE {schema}.temp1 as 
select a.cluster_id, b.*
from {schema}.modelled_crossings a, {schema}.all_crossings b 
where a.{mGeometry} = b.{mGeometry};

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

idcasesql = ""
typecasesql = ""
for layer in nonRailLayers:
    if (layer is None):
        continue
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

checkEmpty(conn, f"select count(*) FROM {schema}.temp1", "error when computing source feature for each modelled crossing - some points not processed")
checkEmpty(conn, f"select count(*) FROM ( select cluster_id, count(*) from {schema}.temp2 group by cluster_id having count(*) > 1 ) foo", "error when computing source feature for each modelled crossing - multiple values found")


sql = f"""

ALTER TABLE {schema}.modelled_crossings add column transport_feature_id integer;
ALTER TABLE {schema}.modelled_crossings add column transport_feature_source varchar(64);

UPDATE {schema}.modelled_crossings SET 
transport_feature_id = CASE {idcasesql} ELSE NULL END,
transport_feature_source = CASE {typecasesql} ELSE NULL END
FROM {schema}.temp2 
WHERE {schema}.temp2.cluster_id = {schema}.modelled_crossings.cluster_id;

DROP TABLE {schema}.temp1;
DROP TABLE {schema}.temp2; 
"""
executeQuery(conn, sql)

for layer in railLayers:
    print(f"Adding {layer} crossings to crossing points")

    sql = f"""
    INSERT INTO {schema}.modelled_crossings (transport_feature_id, transport_feature_source, chyf_stream_id, rivernameid1, rivernameid2, geometry, {mGeometry})
    SELECT {id}, '{layer}', chyf_stream_id, rivernameid1, rivernameid2, geometry, st_transform({geometry}, {mSRID})
    FROM {schema}.{layer}_crossings;
    """
    executeQuery(conn, sql)

sql = f"""
ALTER TABLE {schema}.modelled_crossings ADD column id uuid;
UPDATE {schema}.modelled_crossings SET id = gen_random_uuid();
ALTER TABLE {schema}.modelled_crossings DROP COLUMN cluster_id;
"""
executeQuery(conn, sql)

sql = f"select count(*) FROM {schema}.modelled_crossings WHERE transport_feature_id is null or transport_feature_source is null"
checkEmpty(conn, sql, "a cluster was found without any source transport layer")

print("De-duplicating sets of very close rail crossings...")

# find sets of rail crossings and keep most downstream point only

sql = f"""
DROP TABLE IF EXISTS {schema}.close_points;

CREATE TABLE {schema}.close_points AS (
	with clusters as (
	SELECT id, chyf_stream_id, transport_feature_source, transport_feature_id, geometry, {mGeometry},
      ST_ClusterDBSCAN(geometry_m, eps := 6, minpoints := 2) OVER() AS cluster_id
	FROM {schema}.modelled_crossings
	WHERE transport_feature_source = '{railTable}')
select * from clusters
where cluster_id is not null
order by cluster_id asc);
"""
executeQuery(conn, sql)

sql = f"""
-- add location on line
ALTER TABLE {schema}.close_points ADD COLUMN point_on_line double precision;

UPDATE {schema}.close_points 
SET point_on_line = st_linelocatepoint(e.{geometry}, st_transform(close_points.{geometry}, {cabdSRID}))
FROM {schema}.{streamTable} e 
WHERE e.id = {schema}.close_points.chyf_stream_id;

ALTER TABLE {schema}.close_points ADD COLUMN upstream_length double precision;

UPDATE {schema}.close_points 
SET upstream_length = CASE WHEN a.max_uplength is null THEN b.length ELSE a.max_uplength + b.length END
FROM {schema}.{streamPropTable} a join {schema}.{streamTable} b on a.id = b.id 
WHERE {schema}.close_points.chyf_stream_id = a.id;

--for each cluster we need edge id with the maximum 
DROP TABLE IF EXISTS {schema}.temp3;
CREATE TABLE {schema}.temp3 AS
SELECT cluster_id, max(upstream_length) as max_length from {schema}.close_points 
GROUP BY cluster_id;


--map the cluster to the edge
DROP TABLE IF EXISTS {schema}.temp4;
CREATE TABLE {schema}.temp4 AS
SELECT distinct a.cluster_id, a.max_length, b.chyf_stream_id
FROM {schema}.close_points b, {schema}.temp3 a
WHERE a.cluster_id = b.cluster_id and b.upstream_length = a.max_length;


--for the flowpath we want to find the most downstream point in the cluster
DROP TABLE IF EXISTS {schema}.temp5;
CREATE TABLE {schema}.temp5 as 
SELECT a.cluster_id , max(point_on_line) as min_pol
FROM {schema}.close_points a JOIN {schema}.temp4 b on a.cluster_id = b.cluster_id and a.chyf_stream_id = b.chyf_stream_id
GROUP BY a.cluster_id;

"""
executeQuery(conn, sql)

#should be empty
sql = f"select count(*) FROM (select cluster_id from {schema}.close_points cni except select cluster_id from {schema}.temp5) foo"
checkEmpty(conn, sql, "Not all cluster points processed - error with temp5")

sql = f"""
DROP TABLE IF EXISTS {schema}.temp6;

CREATE TABLE {schema}.temp6 AS
SELECT distinct a.cluster_id, a.id, a.geometry, a.chyf_stream_id
FROM {schema}.close_points a JOIN {schema}.temp5 b on a.cluster_id  = b.cluster_id
and a.point_on_line = b.min_pol;
"""
executeQuery(conn, sql)

#should be empty
sql = f"select count(*) FROM (SELECT cluster_id from {schema}.close_points cni except select cluster_id from {schema}.temp6) foo"
checkEmpty(conn, sql, "Not all cluster points processed - error with temp6")


sql = f"""
--remove all other points in cluster from modelled crossings
DELETE FROM {schema}.modelled_crossings WHERE id NOT IN (SELECT id FROM {schema}.temp6) AND id IN (SELECT id FROM {schema}.close_points);
"""
executeQuery(conn, sql)

#should be empty
sql = f"""
	with clusters as (
	SELECT id, transport_feature_source, transport_feature_id, {mGeometry},
      ST_ClusterDBSCAN({mGeometry}, eps := 6, minpoints := 2) OVER() AS cid
	FROM {schema}.modelled_crossings
    WHERE transport_feature_source = '{railTable}')

    select count(*) from clusters
    where cid is not null;
"""
checkEmpty(conn, sql, "There are still rail crossings within 6 m of each other that need to be removed")

sql = f"""
DROP TABLE {schema}.temp6;
DROP TABLE {schema}.temp5;
DROP TABLE {schema}.temp4;
DROP TABLE {schema}.temp3;
DROP TABLE {schema}.close_points;
"""
executeQuery(conn, sql)

#clean up
sql = f"""
DROP TABLE {schema}.cluster_1;
DROP TABLE {schema}.cluster_2;
DROP TABLE {schema}.cluster_by_id;
"""
executeQuery(conn, sql)


print(f"Adding layer attributes to crossing points: ")
attributeValues = [railAttributes, trailAttributes, roadAttributes, resourceRoadsAttributes]
attributeTables = [railTable, trailTable, roadsTable, resourceRoadsTable]
prefix = ["a", "b", "c", "d"]
sql = f"select cp.*,"
sqlfrom = f" {schema}.modelled_crossings cp "

for i in range(0, len(attributeTables), 1):
    print(attributeTables[i])
    if (attributeTables[i] is None):
        continue

    if (attributeValues[i] is None or attributeValues[i] == ""):
        continue
    
    fields = attributeValues[i].split(",")
    for field in fields:
        sql += f"{prefix[i]}.{field}," 
    
    sqlfrom += f" left join {schema}.{attributeTables[i]} {prefix[i]} on {prefix[i]}.{id} = cp.transport_feature_id and cp.transport_feature_source = '{attributeTables[i]}' "

sql = sql[:-1]
sql = f"""
DROP TABLE IF EXISTS {schema}.modelled_crossings_with_attributes;
CREATE INDEX {schema}_modelled_crossings_transport_feature_id_idx on {schema}.modelled_crossings (transport_feature_id);
CREATE TABLE {schema}.modelled_crossings_with_attributes AS {sql} FROM {sqlfrom}"""

executeQuery(conn, sql)

sql = f"""
DROP TABLE {schema}.modelled_crossings;
ALTER TABLE {schema}.modelled_crossings_with_attributes rename to modelled_crossings;
CREATE INDEX {schema}_modelled_crossings_transport_feature_id_idx on {schema}.modelled_crossings (transport_feature_id);
CREATE INDEX {schema}_modelled_crossings_transport_feature_source_idx on {schema}.modelled_crossings (transport_feature_source);
CREATE INDEX {schema}_modelled_crossings_{geometry}_idx on {schema}.modelled_crossings using gist({geometry});
"""
executeQuery(conn, sql)

# add stream information
sql = f"""
ALTER TABLE {schema}.modelled_crossings ADD COLUMN stream_name_1 varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN stream_name_2 varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN strahler_order integer;

UPDATE {schema}.modelled_crossings SET stream_name_1 = n.name_en FROM public.{streamNameTable} n WHERE rivernameid1 = n.name_id;
UPDATE {schema}.modelled_crossings SET stream_name_2 = n.name_en FROM public.{streamNameTable} n WHERE rivernameid2 = n.name_id;
UPDATE {schema}.modelled_crossings SET strahler_order = p.strahler_order FROM {schema}.{streamPropTable} p WHERE chyf_stream_id = p.id;

"""
executeQuery(conn, sql)

print(f"*Results in {schema}.modelled_crossings table*")
print("** PROCESSING COMPLETE **")