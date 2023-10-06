import psycopg2 as pg2
import sys
import argparse
import configparser
import ast

#-- PARSE COMMAND LINE ARGUMENTS --  
parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=True)
parser.add_argument('-user', type=str, help='the username to access the database')
parser.add_argument('-password', type=str, help='the password to access the database')
args = parser.parse_args()
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

mSRID = config['SETTINGS']['mSRID']
mGeometry = config['SETTINGS']['mGeometry']

#geometry and unique id fields from the above tables
#id MUST be an integer 
geometry = config['DATASETS']['geometryField'].strip()
id = config['DATASETS']['idField'].strip()

#chyf stream data
streamTable = config['CHYF']['streamTable'].strip()
streamPropTable = config['CHYF']['streamPropTable'].strip()

print ("-- Processing Parameters --")
print (f"Database: {dbHost}:{dbPort}/{dbName}")
print (f"Data Schema: {schema}")
print (f"CABD SRID: {cabdSRID}")
print (f"Meters Projection: {mSRID} ")

print (f"Id/Geometry Fields: {id} {geometry}")

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
def checkEmpty(conn, sql, error):    
    with conn.cursor() as cursor:
        cursor.execute(sql)
        count = cursor.fetchone()
        if (count[0] != 0):
            print ("ERROR: " + error)
            sys.exit(-1)

def getCrossings(conn):

    print("Retrieving crossing points")

    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    DROP TABLE IF EXISTS {schema}.modelled_crossings;

    CREATE TABLE {schema}.modelled_crossings AS (SELECT * FROM public.bc_crossings WHERE crossing_feature_type NOT IN ('DAM', 'WEIR'));

    ALTER TABLE {schema}.modelled_crossings ADD COLUMN {geometry} geometry(point, {cabdSRID});
    UPDATE {schema}.modelled_crossings SET {geometry} = ST_Force2D(ST_Transform(geom, {cabdSRID}));

    ALTER TABLE {schema}.modelled_crossings ADD COLUMN {mGeometry} geometry(point, {mSRID});
    UPDATE {schema}.modelled_crossings SET {mGeometry} = ST_Force2D(ST_Transform(geom, {mSRID}));

    ALTER TABLE {schema}.modelled_crossings DROP COLUMN geom;
    ALTER TABLE {schema}.modelled_crossings ADD PRIMARY KEY (aggregated_crossings_id);

    """
    executeQuery(conn, sql)

def clipToWatershed(conn):

    print("Clipping to watershed extent")

    sql = f"""
    CREATE INDEX IF NOT EXISTS {schema}_crossing_geometry_idx ON {schema}.modelled_crossings USING gist({geometry});

    DROP TABLE IF EXISTS {schema}.temp_watersheds;

    CREATE TABLE {schema}.temp_watersheds AS (
        SELECT ST_Union(geometry) AS geometry FROM public.chyf_aoi
        WHERE id IN ({aois}));

    CREATE INDEX IF NOT EXISTS {schema}_temp_watersheds_idx ON {schema}.temp_watersheds USING gist(geometry);

    DELETE FROM {schema}.modelled_crossings
    WHERE aggregated_crossings_id IN (
        SELECT a.aggregated_crossings_id FROM
        {schema}.modelled_crossings a, {schema}.temp_watersheds b
        WHERE NOT ST_Intersects(a.{geometry}, b.geometry)
    );

    DROP TABLE {schema}.temp_watersheds;
    """
    executeQuery(conn, sql)

def addColumns(conn):

    print("Mapping column names to modelled crossings data structure")

    sql = f"""
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_id varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_type varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_name varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS roadway_type varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS roadway_surface varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_owner varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS railway_operator varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS num_railway_tracks varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_condition varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS passability_status varchar;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_type varchar;

    UPDATE {schema}.modelled_crossings SET transport_feature_type = 
        CASE
        WHEN crossing_feature_type = 'RAIL' THEN 'rail'
        WHEN crossing_feature_type = 'ROAD, DEMOGRAPHIC' THEN 'road'
        WHEN crossing_feature_type = 'ROAD, RESOURCE/OTHER' THEN 'resource road'
        WHEN crossing_feature_type = 'TRAIL' THEN 'trail'
        ELSE NULL END;

    UPDATE {schema}.modelled_crossings SET passability_status = 
        CASE
        WHEN barrier_status = 'PASSABLE' THEN 'passable'
        WHEN barrier_status = 'BARRIER' THEN 'barrier'
        WHEN barrier_status = 'POTENTIAL' AND crossing_source= 'MODELLED CROSSINGS' THEN 'unknown'
        WHEN barrier_status = 'POTENTIAL' AND crossing_source= 'PSCIS' THEN 'partial barrier'
        WHEN barrier_status = 'UNKNOWN' THEN 'unknown'
        ELSE NULL END;

    UPDATE {schema}.modelled_crossings SET crossing_type = 
        CASE
        WHEN crossing_subtype_code = 'BRIDGE' then 'bridge'
        WHEN crossing_subtype_code = 'FORD' then 'ford'
        ELSE NULL END;

    """
    executeQuery(conn, sql)

def addNames(conn):

    print("Adding transport feature names")

    sql = f"""
    DROP TABLE IF EXISTS {schema}.transport_names;

    CREATE TABLE {schema}.transport_names AS (
    SELECT
        foo.aggregated_crossings_id,
        a.modelled_crossing_id,
        foo.crossing_feature_type,
        foo.crossing_source,
        foo.pscis_road_name,
        b.structured_name_1,
        c.road_section_name,
        d.petrlm_development_road_name,
        foo.rail_track_name
    FROM {schema}.modelled_crossings foo
    LEFT JOIN public.bc_modelled_stream_crossings a ON a.modelled_crossing_id = foo.modelled_crossing_id
    LEFT JOIN public.bc_transport_line b ON b.transport_line_id = a.transport_line_id
    LEFT JOIN public.bc_ften_road_section_lines_svw c ON c.objectid = a.ften_road_section_lines_id
    LEFT JOIN public.bc_og_petrlm_dev_rds_pre06_pub_sp d ON d.og_petrlm_dev_rd_pre06_pub_id = a.og_petrlm_dev_rd_pre06_pub_id
    );

    UPDATE {schema}.transport_names SET rail_track_name = NULL WHERE rail_track_name = 'None';
    UPDATE {schema}.transport_names SET structured_name_1 = NULL WHERE structured_name_1 = 'unsigned';

    UPDATE {schema}.modelled_crossings SET transport_feature_name = NULL;

    UPDATE {schema}.modelled_crossings SET transport_feature_name = pscis_road_name WHERE pscis_road_name IS NOT NULL;
    UPDATE {schema}.modelled_crossings SET transport_feature_name = rail_track_name WHERE (rail_track_name IS NOT NULL AND rail_track_name != 'None' AND crossing_source = 'MODELLED CROSSINGS');

    UPDATE {schema}.modelled_crossings foo SET transport_feature_name = b.road_section_name FROM {schema}.transport_names b
        WHERE b.modelled_crossing_id = foo.modelled_crossing_id
        AND b.road_section_name IS NOT NULL AND b.crossing_feature_type = 'ROAD, RESOURCE/OTHER'
        AND b.crossing_source IN ('MODELLED CROSSINGS', 'PSCIS')
        AND COALESCE(b.pscis_road_name, b.structured_name_1, b.rail_track_name) IS NULL;

    UPDATE {schema}.modelled_crossings foo SET transport_feature_name = b.structured_name_1 FROM {schema}.transport_names b
        WHERE b.modelled_crossing_id = foo.modelled_crossing_id
        AND b.structured_name_1 IS NOT NULL
        AND (b.crossing_source = 'MODELLED CROSSINGS' OR b.crossing_feature_type = 'ROAD, DEMOGRAPHIC')
        AND COALESCE(b.pscis_road_name, b.road_section_name, b.rail_track_name) IS NULL;
    
    UPDATE {schema}.modelled_crossings foo SET transport_feature_name = b.structured_name_1 FROM {schema}.transport_names b
        WHERE b.modelled_crossing_id = foo.modelled_crossing_id
        AND foo.transport_feature_name IS NULL AND b.structured_name_1 IS NOT NULL
        AND foo.crossing_feature_type = 'ROAD, RESOURCE/OTHER';

    DROP TABLE {schema}.transport_names;
    """

    executeQuery(conn,sql)

def updateAttributes(conn):

    print("Updating attributes")

    sql = f"""
    UPDATE {schema}.modelled_crossings SET roadway_type = transport_line_type_description WHERE transport_line_type_description IS NOT NULL;
    UPDATE {schema}.modelled_crossings SET roadway_surface = transport_line_surface_description WHERE transport_line_surface_description IS NOT NULL;

    UPDATE {schema}.modelled_crossings SET transport_feature_owner = 
        CASE
        WHEN crossing_feature_type = 'ROAD, RESOURCE/OTHER' AND ogc_proponent IS NOT NULL THEN ogc_proponent
        WHEN crossing_feature_type = 'ROAD, RESOURCE/OTHER' AND ften_client_name IS NOT NULL THEN ften_client_name
        WHEN crossing_feature_type = 'RAIL' AND rail_owner_name IS NOT NULL THEN rail_owner_name
        ELSE NULL END;

    UPDATE {schema}.modelled_crossings SET railway_operator = rail_operator_english_name WHERE rail_operator_english_name IS NOT NULL;
    UPDATE {schema}.modelled_crossings SET transport_feature_condition = LOWER(ften_life_cycle_status_code) WHERE ften_life_cycle_status_code IS NOT NULL;

    ALTER TABLE {schema}.modelled_crossings ADD COLUMN id uuid;
    UPDATE {schema}.modelled_crossings SET id = gen_random_uuid();
    """

    executeQuery(conn,sql)

def checkCrossingLocations(conn):

    print("Checking if any crossings intersect multiple streams")

    checkQuery = f"""
    with test as (
        SELECT a.aggregated_crossings_id AS crossing_id, a.geometry AS geom, b.id AS stream_id FROM {schema}.modelled_crossings a, {schema}.eflowpath b WHERE st_intersects(a.snapped_point, b.geometry)
        ORDER BY a.aggregated_crossings_id ASC)

    SELECT crossing_id, geom, COUNT(*)
    FROM test
    GROUP BY crossing_id, geom
    HAVING COUNT(*) > 1;
    """

    while (True):
        
        with conn.cursor() as cursor:
            cursor.execute(checkQuery)
            row = cursor.fetchone()
        
        print("-----------------------------")
        print("ERROR: Some crossings intersect multiple streams\nPlease review and snap these (using the snapped_point geometry) to a single stream before continuing\nUse the following query to find these streams and compare to FWA networks for location:")
        print(checkQuery)
        input("Press any key to continue cleanup...")
        print("-----------------------------")
        
        if row is None:
            print("All crossings have been fixed!")
            break

def matchStreams(conn):

    print("Snapping to CHyF streams")

    sql = f"""

    CREATE INDEX IF NOT EXISTS {schema}_stream_geometry_idx ON {schema}.{streamTable} USING gist ({geometry});

    --create new snapping function for snapping modelled crossings

    CREATE OR REPLACE FUNCTION {schema}.snap_to_streams(
        src_schema character varying,
        src_table character varying,
        raw_geom character varying,
        snapped_geom character varying,
        max_distance_m double precision)
        RETURNS void
        LANGUAGE 'plpgsql'
        COST 100
        VOLATILE PARALLEL UNSAFE
    AS $BODY$

    DECLARE
    pnt_rec RECORD;
    fp_rec RECORD;
    BEGIN

        FOR pnt_rec IN EXECUTE format('SELECT aggregated_crossings_id, %I as rawg FROM %I.%I WHERE %I is not null', raw_geom, src_schema, src_table,raw_geom) 
        LOOP 
            --RAISE NOTICE '%s: %s', pnt_rec.aggregated_crossings_id, pnt_rec.rawg;
            FOR fp_rec IN EXECUTE format ('SELECT fp.geometry as geometry, st_distance(%L::geometry::geography, fp.geometry::geography) AS distance FROM {schema}.{streamTable} fp WHERE st_expand(%L::geometry, 0.01) && fp.geometry and st_distance(%L::geometry::geography, fp.geometry::geography) < %s ORDER BY distance ', pnt_rec.rawg, pnt_rec.rawg, pnt_rec.rawg, max_distance_m)
            LOOP
                EXECUTE format('UPDATE %I.%I SET %I = ST_LineInterpolatePoint(%L::geometry, ST_LineLocatePoint(%L::geometry, %L::geometry)) WHERE aggregated_crossings_id = %L', src_schema, src_table, snapped_geom,fp_rec.geometry, fp_rec.geometry, pnt_rec.rawg, pnt_rec.aggregated_crossings_id);
                --RAISE NOTICE '%s', fp_rec.distance;	
                EXIT;
            
            END LOOP;
        END LOOP;
    END;
    $BODY$;

    ALTER FUNCTION {schema}.snap_to_streams(character varying, character varying, character varying, character varying, double precision)
        OWNER TO katherineo;

    GRANT EXECUTE ON FUNCTION {schema}.snap_to_streams(character varying, character varying, character varying, character varying, double precision) TO PUBLIC;
    GRANT EXECUTE ON FUNCTION {schema}.snap_to_streams(character varying, character varying, character varying, character varying, double precision) TO cabd;

    ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS snapped_point geometry(Point, {cabdSRID});
    SELECT {schema}.snap_to_streams('{schema}', 'modelled_crossings', '{geometry}', 'snapped_point', 50);
    UPDATE {schema}.modelled_crossings SET snapped_point = {geometry} WHERE snapped_point IS NULL;

    CREATE INDEX {schema}_modelled_crossings_snapped_point_idx on {schema}.modelled_crossings using gist(snapped_point);
    """
    executeQuery(conn, sql)

    checkCrossingLocations(conn)

    sql = f"""
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN chyf_stream_id uuid;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN strahler_order integer;
    UPDATE {schema}.modelled_crossings SET {mGeometry} = ST_Transform(snapped_point, {mSRID});
    CREATE INDEX {schema}_modelled_crossings_{mGeometry}_idx on {schema}.modelled_crossings using gist({mGeometry});

    ALTER TABLE {schema}.{streamTable} ADD COLUMN {mGeometry} geometry(LineString, {mSRID});
    UPDATE {schema}.{streamTable} SET {mGeometry} = ST_Transform({geometry}, {mSRID});
    CREATE INDEX {schema}_{streamTable}_{mGeometry}_idx on {schema}.{streamTable} using gist({mGeometry});

    UPDATE {schema}.modelled_crossings AS t1 SET chyf_stream_id = t2.id FROM {schema}.{streamTable} AS t2 WHERE ST_DWithin(t1.{mGeometry}, t2.{mGeometry}, 0.01);
    UPDATE {schema}.modelled_crossings SET strahler_order = p.strahler_order FROM {schema}.{streamPropTable} p WHERE chyf_stream_id = p.id;
    """

    print("Joining stream information to points")
    executeQuery(conn, sql)

def main():

    print("Connecting to database...")

    conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)
    
    getCrossings(conn)
    # clipToWatershed(conn)
    addColumns(conn)
    addNames(conn)
    updateAttributes(conn)
    matchStreams(conn)

    print("** CLEANUP COMPLETE **")

if __name__ == "__main__":
    main()