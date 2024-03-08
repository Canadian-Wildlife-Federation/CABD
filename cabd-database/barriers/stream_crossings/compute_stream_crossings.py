import sys
import argparse
import configparser
import ast
from datetime import datetime
import getpass
import psycopg2 as pg2
from psycopg2.extras import RealDictCursor

startTime = datetime.now()
print("Start time:", startTime)

#-- PARSE COMMAND LINE ARGUMENTS --  
parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=True)
parser.add_argument('--copystreams', dest='streams', action='store_true', help='stream data needs to be copied')
parser.add_argument('--ignorestreams', dest='streams', action='store_false', help='stream data is already present')
args = parser.parse_args()
configfile = args.c

#-- READ PARAMETERS FOR CONFIG FILE --
config = configparser.ConfigParser()
config.read(configfile)

#database settings
dbHost = config['DATABASE']['host']
dbPort = config['DATABASE']['port']
dbName = config['DATABASE']['name']
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

#output data schema
schema = config['DATABASE']['data_schema']
cabdSRID = config['DATABASE']['cabdSRID']

mSRID  = config['SETTINGS']['mSRID']
mGeometry = config['SETTINGS']['mGeometry']
#distance in meters (mSRID projection units) for clustering points
clusterDistance = config['SETTINGS']['clusterDistance']
railClusterDistance = config['SETTINGS']['railClusterDistance']

#chyf stream network aois as '<aoiuuid>','<aoiuuid>'
prCode = config['SETTINGS']['prCode']

#data tables
#set to an empty dict if doesn't exist for data
rail = ast.literal_eval(config['DATASETS']['railTable'])
roads = ast.literal_eval(config['DATASETS']['roadsTable'])
resourceRoads = ast.literal_eval(config['DATASETS']['resourceRoadsTable'])
trail = ast.literal_eval(config['DATASETS']['trailTable'])

all_datasets = rail | roads | resourceRoads | trail

railTable = [k for k in rail]
roadsTable = [k for k in roads]
resourceRoadsTable = [k for k in resourceRoads]
trailTable = [k for k in trail]

railTable = None if not railTable else railTable
roadsTable = None if not roadsTable else roadsTable
resourceRoadsTable = None if not resourceRoadsTable else resourceRoadsTable
trailTable = None if not trailTable else trailTable

#geometry and unique id fields from the above tables
#id MUST be an integer 
geometry = config['DATASETS']['geometryField'].strip()
id = config['DATASETS']['idField'].strip()

#chyf stream data
streamTable = config['CHYF']['streamTable'].strip()
streamPropTable = config['CHYF']['streamPropTable'].strip()
streamNameTable = config['CHYF']['streamNameTable'].strip()

#all source transport layers to be used for computing crossings
layers = [k for k in all_datasets]

#non-rail layers - these are included in the clustering
#these should be in order of priority for assigning ids to point
nonRailLayers = ast.literal_eval(config['DATASETS']['nonRailLayers'])

#rail layers - these are clustered separately from other features
railLayers = ast.literal_eval(config['DATASETS']['railLayers'])

print ("-- Processing Parameters --")
print (f"Database: {dbHost}:{dbPort}/{dbName}")
print (f"Data Schema: {schema}")
print (f"CABD SRID: {cabdSRID}")
print (f"CHyF Data: {streamTable} {streamPropTable} {streamNameTable}")
print (f"Meters Projection: {mSRID} ")
print (f"Cluster Distance: {clusterDistance} ")

print (f"Data Tables: {railTable} {roadsTable} {resourceRoadsTable} {trailTable} ")
print (f"Id/Geometry Fields: {id} {geometry}")

print (f"All Layers: {layers}")
print (f"Non Rail Layers: {nonRailLayers}")
print (f"Rail Layers: {railLayers}")

print ("----")

#--
#-- function to execute a query
#--
def executeQuery(conn, sql):
    #print (sql)
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()


#--
#-- checks if the first column of the first row
#-- of the query results is 0 otherwise
# -- ends the program
#--
def checkEmpty(conn, sql, error):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        count = cursor.fetchone()
        if count[0] != 0:
            print ("ERROR: " + error)
            sys.exit(-1)


#--
#-- function to check if a table exists
#--
def checkTableExists(conn, tableName):

    query = f"""
    SELECT EXISTS(SELECT 1 FROM information_schema.tables 
    WHERE table_catalog='{dbName}' AND 
        table_schema='{schema}' AND 
        table_name='{tableName}');
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        result = result[0]

    return result


def getStreamData(conn):

    def getChyfData(aoi_list):

        if len(aoi_list) > 1:
            aois = tuple(aoi_list)
        else:
            aois = str(tuple(aoi_list))
            aois = aois.replace(",", "")

        print("Getting CHyF data")

        sql = f"""
        INSERT INTO {schema}.{streamTable} 
            (id,
            source_id,
            aoi_id,
            ef_type,
            ef_subtype,
            rank,
            length,
            rivernameid1,
            rivernameid2,
            geometry)
        SELECT
            id,
            id,
            aoi_id,
            ef_type,
            ef_subtype,
            rank,
            length,
            rivernameid1,
            rivernameid2,
            geometry
        FROM chyf_flowpath
        WHERE aoi_id in {aois} AND ef_type != 2;

        CREATE TABLE {schema}.{streamPropTable} as SELECT * FROM chyf_flowpath_properties WHERE aoi_id IN {aois};
        """
        executeQuery(conn, sql)

    def getNhnData(aoi_list):

        aois = tuple(aoi_list)

        print("Getting NHN data")

        sql = f"""
        INSERT INTO {schema}.{streamTable} 
            (id,
            source_id,
            short_name,
            rivernameid1,
            rivernameid2,
            name_1,
            name_2,
            geometry)
        SELECT
            gen_random_uuid(),
            nid,
            dataset_name,
            nameid_1,
            nameid_2,
            name_1,
            name_2,
            ST_LineMerge(ST_CurveToLine(geometry))
        FROM nhn_raw.flowpaths
        WHERE dataset_name in {aois};
        """
        executeQuery(conn, sql)

    aoiQuery = f"""
        SELECT a.id FROM cabd.nhn_workunit a, cabd.province_territory_codes b
        WHERE st_intersects(a.polygon, b.geometry)
        AND b.code = '{prCode}'
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(aoiQuery)
        rows = cursor.fetchall()

    aoiTuple = tuple([row['id'] for row in rows])
    aoiNhn = list(aoiTuple)

    if prCode == 'nt': # special handling for NWT where 3 aois were combined
        indiv = ['10MC001', '10LC000', '1001000']
        combined = "MACK001"
        aoiNhn = list(x for x in aoiNhn if x not in indiv)

        aoiQuery = f"""
            SELECT id, short_name from chyf_aoi
            where short_name in {aoiTuple} OR short_name = '{combined}';
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(aoiQuery)
            rows = cursor.fetchall()

        aoiChyf = []

        for row in rows:
            id = row['id']
            name = row['short_name']
            aoiChyf.append(id)

            if name in aoiNhn:
                aoiNhn.remove(name)
            else:
                continue

    else:
        aoiQuery = f"""
            SELECT id, short_name from chyf_aoi
            where short_name in {aoiTuple};
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(aoiQuery)
            rows = cursor.fetchall()

        aoiChyf = []

        for row in rows:
            id = row['id']
            name = row['short_name']
            aoiChyf.append(id)
            aoiNhn.remove(name)

    if (len(aoiChyf) + len(aoiNhn)) != len(aoiTuple):
        if prCode == 'nt': # allow this in NT because of combined AOIs
            pass
        else:
            print("Error: Not all AOIs have been queued for download")
    else:
        pass

    sql = f"""
        DROP TABLE IF EXISTS {schema}.{streamTable};
        DROP TABLE IF EXISTS {schema}.{streamPropTable};

        CREATE TABLE {schema}.{streamTable} (
            id uuid not null,
            source_id varchar,
            aoi_id uuid,
            short_name varchar,
            ef_type integer,
            ef_subtype integer,
            rank integer,
            length double precision,
            rivernameid1 varchar,
            rivernameid2 varchar,
            name_1 varchar(100),
            name_2 varchar(100),
            geometry geometry(LineString,4617)
        );
    """
    executeQuery(conn, sql)

    if aoiChyf:
        getChyfData(aoiChyf)

        sql = f"""
        CREATE INDEX {schema}_{streamPropTable}_id on {schema}.{streamPropTable} (id);
        ANALYZE {schema}.{streamPropTable};
        """
        executeQuery(conn, sql)

    if aoiNhn:
        getNhnData(aoiNhn)

    sql = f"""
        CREATE INDEX {schema}_{streamTable}_{geometry} on {schema}.{streamTable} using gist({geometry}); 
        CREATE INDEX {schema}_{streamTable}_id on {schema}.{streamTable} (id);
        ANALYZE {schema}.{streamTable};
    """
    executeQuery(conn, sql)

    # calculate length as this is missing in NHN data, and is required for clustering operations
    sql = f"""
    UPDATE {schema}.{streamTable} SET length = ST_Length(geometry::geography) WHERE length IS NULL;
    """
    executeQuery(conn, sql)

    if aoiChyf:
        sql = f"SELECT case when count(*) = 0 then 1 else 0 end FROM {schema}.{streamPropTable}"
        checkEmpty(conn, sql, "stream property table is empty")

    else:
        # warn user if stream data has not been fetched
        sql = f"SELECT case when count(*) = 0 then 1 else 0 end FROM {schema}.{streamTable}"
        checkEmpty(conn, sql, "stream table is empty")

# ----
# -- compute modelled crossings for each layer
# ----
def computeCrossings(conn):
    for layer in layers:
        if layer is None:
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
# -- combine all crossings EXCEPT rail crossings into a single layer
# ----
def combineCrossings(conn):

    print("Combining crossings into single layer")

    sql = f'DROP TABLE IF EXISTS {schema}.all_crossings;'
    sql += f'CREATE TABLE {schema}.all_crossings (id serial, chyf_stream_id uuid, rivernameid1 varchar, rivernameid2 varchar,'
    for layer in nonRailLayers:
        sql = sql + f'{layer}_{id} integer, '
    sql = sql + f'{geometry} geometry(POINT, {cabdSRID})); '

    idFields = ""
    for layer in nonRailLayers:
        idFields = idFields + f'z.{layer}_{id},'
        sql = sql + f"""INSERT INTO {schema}.all_crossings ({layer}_{id}, chyf_stream_id, rivernameid1, rivernameid2, {geometry}) SELECT {id}, chyf_stream_id, rivernameid1, rivernameid2, {geometry} FROM {schema}.{layer}_crossings WHERE ST_GeometryType({geometry}) = 'ST_Point';"""

    sql += f'CREATE INDEX all_crossings_geometry on {schema}.all_crossings using gist({geometry}); ANALYZE {schema}.all_crossings'
    executeQuery(conn, sql)
    idFields = idFields[:-1]

# ----
# -- Cluster Points
# ----
def clusterPoints(conn):

    idFields = ""
    for layer in nonRailLayers:
        if layer is None:
            continue
        idFields = idFields + f'z.{layer}_{id},'
    idFields = idFields[:-1]

    print("Clustering Points")
    sql = f"""
    -- convert geometry to equal area projection so we can cluster by distances

    ALTER TABLE {schema}.all_crossings ADD COLUMN {mGeometry} geometry(point, {mSRID});
    UPDATE {schema}.all_crossings set {mGeometry} = st_transform({geometry}, {mSRID}) ;

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

    ALTER TABLE {schema}.modelled_crossings ADD COLUMN {mGeometry} geometry(point,{mSRID});
    UPDATE {schema}.modelled_crossings SET {mGeometry} = st_transform({geometry}, {mSRID});

    """
    executeQuery(conn, sql)

def processClusters(conn):

    for layer in nonRailLayers:

        print(layer)

        if layer is None:
            continue

        print(f"""Computing point to keep for cluster with {layer} crossings""")

        sql = f"""

        --- find the largest stream on prioritizing road segments ---

        DROP TABLE IF EXISTS {schema}.cluster_{layer}_id;

        CREATE TABLE {schema}.cluster_{layer}_id as 
        SELECT * FROM {schema}.cluster_by_id_with_data 
        WHERE {layer}_{id} is not null;

        --for all these ones where there is only one 
        --use this as the cluster point
        INSERT INTO {schema}.modelled_crossings (cluster_id, geometry, chyf_stream_id, rivernameid1, rivernameid2, {mGeometry})
        SELECT 
            cluster_id, {geometry}, chyf_stream_id, rivernameid1, rivernameid2, st_transform({geometry}, {mSRID})
        FROM {schema}.cluster_{layer}_id 
        WHERE cluster_id in (
            SELECT cluster_id FROM {schema}.cluster_{layer}_id GROUP BY cluster_id HAVING count(*) = 1
        );

        --remove these from working data sets 
        DELETE FROM {schema}.cluster_by_id_with_data WHERE cluster_id IN (select cluster_id FROM {schema}.modelled_crossings);
        DELETE FROM {schema}.cluster_{layer}_id WHERE cluster_id IN (select cluster_id FROM {schema}.modelled_crossings);

        -- for the remaining add location on line
        ALTER TABLE  {schema}.cluster_{layer}_id ADD COLUMN point_on_line double precision;
        UPDATE {schema}.cluster_{layer}_id
        set point_on_line = st_linelocatepoint(e.{geometry}, st_transform(cluster_{layer}_id.{geometry}, {cabdSRID}))
        FROM {schema}.{streamTable} e where e.id = {schema}.cluster_{layer}_id.chyf_stream_id;

        --add upstream length
        ALTER TABLE  {schema}.cluster_{layer}_id ADD COLUMN upstream_length double precision;
        """
        executeQuery(conn,sql)

        propTableExist = checkTableExists(conn, streamPropTable)

        if propTableExist:

            sql = f"""
            UPDATE {schema}.cluster_{layer}_id 
            SET upstream_length = CASE WHEN a.max_uplength IS NULL THEN b.length ELSE a.max_uplength + b.length END
            FROM {schema}.{streamPropTable} a JOIN {schema}.{streamTable} b on a.id = b.id 
            where {schema}.cluster_{layer}_id.chyf_stream_id = a.id;

            --case statement above is to deal with secondaries - these have no upstream length
            --there were a few cases where these were crossed by they were all on the same edge so it 
            --doesn't matter
            --we are using this up length to determine which edge to pick when there is more than
            --one stream edge being crossed
            """
            executeQuery(conn, sql)

        sql = f"""
        --new addition to deal with flowpaths that are from unprocessed NHN (no chyf_properties table)
        UPDATE {schema}.cluster_{layer}_id 
        SET upstream_length = b.length
        FROM {schema}.{streamTable} b
        WHERE {schema}.cluster_{layer}_id.chyf_stream_id = b.id AND upstream_length IS NULL;

        --for each cluster we need edge id with the maximum 
        DROP TABLE IF EXISTS {schema}.temp3;
        CREATE TABLE {schema}.temp3 AS 
        SELECT cluster_id, max(upstream_length) as max_length 
        FROM {schema}.cluster_{layer}_id group by cluster_id;

        --map the cluster to the edge
        DROP TABLE IF EXISTS {schema}.temp4;
        CREATE TABLE {schema}.temp4 AS 
        SELECT distinct a.cluster_id, a.max_length, b.chyf_stream_id, b.rivernameid1, b.rivernameid2
        FROM {schema}.cluster_{layer}_id b, {schema}.temp3 a
        WHERE a.cluster_id = b.cluster_id and b.upstream_length = a.max_length;

        --for the flowpath we want to find the most downstream point in the cluster
        DROP TABLE IF EXISTS {schema}.temp5;
        CREATE TABLE {schema}.temp5 AS 
        SELECT a.cluster_id , max(point_on_line) as min_pol
        FROM {schema}.cluster_{layer}_id a JOIN {schema}.temp4 b on a.cluster_id = b.cluster_id and a.chyf_stream_id = b.chyf_stream_id
        GROUP BY a.cluster_id;
        """
        executeQuery(conn, sql)

        #should be empty
        sql = f"SELECT COUNT(*) FROM (select cluster_id from {schema}.cluster_{layer}_id except select cluster_id from {schema}.temp5) foo"
        checkEmpty(conn, sql, f"error when computing clusters with {layer} - error with temporary table 5")

        sql = f"""
        --merge to find downstream point
        DROP TABLE IF EXISTS {schema}.temp6;
        CREATE TABLE {schema}.temp6 AS 
        SELECT distinct a.cluster_id, a.geometry, a.chyf_stream_id, a.rivernameid1, a.rivernameid2
        FROM {schema}.cluster_{layer}_id a JOIN {schema}.temp5 b on a.cluster_id  = b.cluster_id
        and a.point_on_line = b.min_pol;
        """
        executeQuery(conn, sql)

        #should be empty
        sql = f" SELECT count(*) FROM (SELECT cluster_id FROM {schema}.cluster_{layer}_id except SELECT cluster_id FROM {schema}.temp6) foo"
        checkEmpty(conn, sql, f"error when computing clusters with {layer} - error with temporary table 6")

        sql = f"""
        --add to main table and remove from processing
        INSERT INTO {schema}.modelled_crossings (cluster_id, geometry, chyf_stream_id, rivernameid1, rivernameid2, {mGeometry})
        SELECT cluster_id, geometry, chyf_stream_id, rivernameid1, rivernameid2, st_transform({geometry}, {mSRID}) FROM {schema}.temp6;

        --remove these from layer data & cluster_by_id_with_data
        DELETE FROM {schema}.cluster_by_id_with_data WHERE cluster_id IN (SELECT cluster_id FROM {schema}.modelled_crossings);
        DELETE FROM {schema}.cluster_{layer}_id WHERE cluster_id IN (SELECT cluster_id FROM {schema}.modelled_crossings);
        """
        executeQuery(conn, sql)

        #should be empty
        sql = f"select count(*) from {schema}.cluster_{layer}_id;"
        checkEmpty(conn, sql, f"error when computing clusters with {layer} - not all clusters with point from the layer were processed")

        sql = f"""
        DROP TABLE {schema}.temp6;
        DROP TABLE {schema}.temp5;
        DROP TABLE {schema}.temp4;
        DROP TABLE {schema}.temp3;
        --DROP TABLE {schema}.cluster_{layer}_id;
        """
        executeQuery(conn, sql)

    print("Adding source data ids to crossing points")

    fieldswithtype = ""
    fields = ""
    for layer in nonRailLayers:
        if layer is None:
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
        if layer is None:
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


def processRail(conn):

    for layer in railLayers:
        print(f"Adding {layer} crossings to crossing points")

        sql = f"""
        INSERT INTO {schema}.modelled_crossings (transport_feature_id, transport_feature_source, chyf_stream_id, rivernameid1, rivernameid2, geometry, {mGeometry})
        SELECT {id}, '{layer}', chyf_stream_id, rivernameid1, rivernameid2, geometry, st_transform({geometry}, {mSRID})
        FROM {schema}.{layer}_crossings;
        """
        executeQuery(conn, sql)

        sql = f"""
        ALTER TABLE {schema}.modelled_crossings ADD column IF NOT EXISTS id uuid;
        UPDATE {schema}.modelled_crossings SET id = gen_random_uuid();
        ALTER TABLE {schema}.modelled_crossings DROP COLUMN cluster_id;
        """
        executeQuery(conn, sql)

        sql = f"select count(*) FROM {schema}.modelled_crossings WHERE transport_feature_id is null or transport_feature_source is null"
        checkEmpty(conn, sql, "a cluster was found without any source transport layer")

        print("De-duplicating sets of very close rail crossings")

        # find sets of rail crossings and keep most downstream point only

        sql = f"""
        DROP TABLE IF EXISTS {schema}.close_points;

        CREATE TABLE {schema}.close_points AS (
            with clusters as (
            SELECT id, chyf_stream_id, transport_feature_source, transport_feature_id, geometry, {mGeometry},
            ST_ClusterDBSCAN(geometry_m, eps := {railClusterDistance}, minpoints := 2) OVER() AS cluster_id
            FROM {schema}.modelled_crossings
            WHERE transport_feature_source = '{layer}')
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
        """
        executeQuery(conn, sql)

        propTableExist = checkTableExists(conn, streamPropTable)

        if propTableExist:

            sql = f"""
            UPDATE {schema}.close_points 
            SET upstream_length = CASE WHEN a.max_uplength is null THEN b.length ELSE a.max_uplength + b.length END
            FROM {schema}.{streamPropTable} a join {schema}.{streamTable} b on a.id = b.id 
            WHERE {schema}.close_points.chyf_stream_id = a.id;
            """
            executeQuery(conn, sql)

        sql = f"""
        --new addition to deal with flowpaths that are from unprocessed NHN (no chyf_properties table)
        UPDATE {schema}.close_points
        SET upstream_length = b.length
        FROM {schema}.{streamTable} b
        WHERE {schema}.close_points.chyf_stream_id = b.id AND upstream_length IS NULL;

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
            ST_ClusterDBSCAN({mGeometry}, eps := {railClusterDistance}, minpoints := 2) OVER() AS cid
            FROM {schema}.modelled_crossings
            WHERE transport_feature_source = '{layer}')

            select count(*) from clusters
            where cid is not null;
        """
        checkEmpty(conn, sql, "There are still rail crossings within {railClusterDistance} m of each other that need to be removed")

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

    sql = f"""
    ALTER TABLE {schema}.modelled_crossings ADD column IF NOT EXISTS id uuid;
    UPDATE {schema}.modelled_crossings SET id = gen_random_uuid();
    """
    executeQuery(conn, sql)


def matchArchive(conn):

    print("Matching ids to archived crossings")

    query = f"""
        ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS new_crossing_subtype varchar;
        ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS reviewer_status varchar;
        ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS reviewer_comments varchar;

        with match as (
            SELECT
            a.id AS modelled_id,
            nn.id AS archive_id,
            nn.new_crossing_subtype AS new_crossing_subtype,
            nn.reviewer_status AS reviewer_status,
            nn.reviewer_comments AS reviewer_comments,
            nn.dist
            FROM {schema}.modelled_crossings a
            CROSS JOIN LATERAL (
                SELECT
                id,
                new_crossing_subtype,
                reviewer_status,
                reviewer_comments,
                ST_Distance(a.{mGeometry}, b.{mGeometry}) as dist
                FROM {schema}.modelled_crossings_archive b
                ORDER BY a.{mGeometry} <-> b.{mGeometry}
                LIMIT 1
            ) as nn
            WHERE nn.dist < 1
        ),

        match_distinct AS (
            select distinct on(archive_id) archive_id, dist, modelled_id, new_crossing_subtype, reviewer_status, reviewer_comments
            from match
            order by archive_id, dist asc
        )

        UPDATE {schema}.modelled_crossings a
        SET 
            id = m.archive_id,
            new_crossing_subtype = m.new_crossing_subtype,
            reviewer_status = m.reviewer_status,
            reviewer_comments = m.reviewer_comments
        FROM match_distinct m WHERE m.modelled_id = a.id;

        DROP TABLE {schema}.modelled_crossings_archive;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)

def finalizeCrossings(conn):

    print("Adding layer attributes to crossing points: ")

    # generate list of aliases to refer to each table
    prefix = []
    alpha = 'a'
    for i in range(0, len(all_datasets), 1):
        prefix.append(alpha)
        alpha = chr(ord(alpha) + 1)

    sql = "select cp.*,"
    sqlfrom = f" {schema}.modelled_crossings cp "

    idx = 0

    # iterate through datasets and get attribute values from dict
    # then add to sql query
    for k in all_datasets:
        print(k)
        if k:
            attributeValues = all_datasets[k]
        else:
            continue

        print("Attribute Values:", attributeValues)

        for val in attributeValues:
            sql += f"{prefix[idx]}.{val},"

        sqlfrom += f" left join {schema}.{k} {prefix[idx]} on {prefix[idx]}.{id} = cp.transport_feature_id and cp.transport_feature_source = '{k}' "

        idx = idx + 1

    sql = sql[:-1]
    sql = f"""
    DROP TABLE IF EXISTS {schema}.modelled_crossings_with_attributes;
    CREATE INDEX {schema}_modelled_crossings_transport_feature_id_idx on {schema}.modelled_crossings (transport_feature_id);
    CREATE TABLE {schema}.modelled_crossings_with_attributes AS {sql} FROM {sqlfrom}
    """
    executeQuery(conn, sql)

    sql = f"""
    DROP TABLE {schema}.modelled_crossings;
    ALTER TABLE {schema}.modelled_crossings_with_attributes rename to modelled_crossings;
    ALTER TABLE {schema}.modelled_crossings ALTER COLUMN {geometry} TYPE geometry(Point, {cabdSRID}) USING st_transform(geometry,{cabdSRID});
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
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS new_crossing_subtype varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS reviewer_status varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS reviewer_comments varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN last_modified TIMESTAMPTZ default now();
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN last_modified_by varchar default user;

    UPDATE {schema}.modelled_crossings SET stream_name_1 = n.name_en FROM public.{streamNameTable} n WHERE rivernameid1 = n.name_id::varchar;
    UPDATE {schema}.modelled_crossings SET stream_name_2 = n.name_en FROM public.{streamNameTable} n WHERE rivernameid2 = n.name_id::varchar;
    """
    executeQuery(conn, sql)

    propTableExist = checkTableExists(conn, streamPropTable)

    if propTableExist:

        sql = f"""
        UPDATE {schema}.modelled_crossings SET strahler_order = p.strahler_order FROM {schema}.{streamPropTable} p WHERE chyf_stream_id = p.id;
        """

        executeQuery(conn, sql)

    sql = f"""

    ALTER TABLE {schema}.modelled_crossings
        ADD CONSTRAINT {schema}_crossing_subtype_fkey FOREIGN KEY (new_crossing_subtype)
        REFERENCES stream_crossings.crossing_type_codes (name_en) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

    GRANT USAGE ON SCHEMA {schema} TO cwf_user;
    GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO cwf_user;
    GRANT UPDATE(new_crossing_subtype) ON {schema}.modelled_crossings TO cwf_user;
    GRANT UPDATE(reviewer_status) ON {schema}.modelled_crossings TO cwf_user;
    GRANT UPDATE(reviewer_comments) ON {schema}.modelled_crossings TO cwf_user;
    """
    executeQuery(conn, sql)

    print("Getting NHN workunit names")
    sql = f"""
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN nhn_watershed_id varchar;
    UPDATE {schema}.modelled_crossings AS c SET nhn_watershed_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, c.geometry);
    """
    executeQuery(conn, sql)

    sql = f"""
    --create function trigger to change a timestamp value upon an update
    CREATE OR REPLACE FUNCTION trigger_set_timestamp()
    RETURNS TRIGGER AS $$
    BEGIN
    NEW.last_modified = NOW();
    RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    --create a trigger to execute the function
    CREATE TRIGGER set_timestamp
    BEFORE UPDATE ON {schema}.modelled_crossings
    FOR EACH ROW
    EXECUTE PROCEDURE trigger_set_timestamp();

    --create function trigger to change a username value upon an update
    CREATE OR REPLACE FUNCTION trigger_set_usertimestamp()
    RETURNS TRIGGER AS $$
    BEGIN
    NEW.last_modified_by = user;
    RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    --create a trigger to execute the function
    CREATE TRIGGER set_usertimestamp
    BEFORE UPDATE ON {schema}.modelled_crossings
    FOR EACH ROW
    EXECUTE PROCEDURE trigger_set_usertimestamp();
    """
    executeQuery(conn, sql)

    print(f"*Results in {schema}.modelled_crossings table*")

def main():

    # -- MAIN SCRIPT --  

    print("Connecting to database...")

    conn = pg2.connect(database=dbName, 
                    user=dbUser, 
                    host=dbHost, 
                    password=dbPassword, 
                    port=dbPort)

    if args.streams:
        print("Copying streams into schema")
        getStreamData(conn)

    else:
        print("Skipping copy of stream data")

    crossingsExist = checkTableExists(conn, 'modelled_crossings')

    if crossingsExist:
        print("Creating an archive table from previous crossings")
        # create an archive table to match later
        query = f"""
            DROP TABLE IF EXISTS {schema}.modelled_crossings_archive;
            CREATE TABLE {schema}.modelled_crossings_archive AS SELECT * FROM {schema}.modelled_crossings;
            CREATE INDEX {schema}_modelled_crossings_archive_{mGeometry}_idx on {schema}.modelled_crossings_archive using gist({mGeometry});
        """
        executeQuery(conn, query)

        computeCrossings(conn)
        combineCrossings(conn)
        clusterPoints(conn)
        processClusters(conn)
        processRail(conn)
        matchArchive(conn)
        finalizeCrossings(conn)

    else:
        computeCrossings(conn)
        combineCrossings(conn)
        clusterPoints(conn)
        processClusters(conn)
        processRail(conn)
        finalizeCrossings(conn)

    print("Done!")
    endTime = datetime.now()
    print("End time:", endTime)
    print("Total runtime: " + str((datetime.now() - startTime)))

if __name__ == "__main__":
    main()
