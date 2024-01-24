import argparse
import configparser
import subprocess
from datetime import datetime
from pathlib import Path
import getpass
import psycopg2 as pg2

startTime = datetime.now()
print("Start time:", startTime)

#-- PARSE COMMAND LINE ARGUMENTS --
parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=True)
parser.add_argument('-file', type=str, help='the file containing assessment data', required=True)
parser.add_argument('-distance', type=int, help='the distance in meters for matching points', required=True)
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

#assessment data
dataFile = args.file

#distance to match points
dist = args.distance

#table name from filename
targetTable = dataFile.replace('\\', '/')
targetTable = Path(targetTable).stem

#output data schema
schema = config['DATABASE']['data_schema']
cabdSRID = config['DATABASE']['cabdSRID']

mSRID  = config['SETTINGS']['mSRID']
mGeometry = config['SETTINGS']['mGeometry']

#chyf stream network aois as '<aoiuuid>','<aoiuuid>'
aoi_raw = config['SETTINGS']['aoi_raw']
aois = str(aoi_raw)[1:-1]

#alternate ogr options
ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"
#ogr = "C:\\Program Files\\QGIS 3.22.3\\bin\\ogr2ogr.exe"

print ("-- Parameters --")
print (f"Database: {dbHost}:{dbPort}/{dbName}")
print (f"Data Schema: {schema}")
print (f"CABD SRID: {cabdSRID}")
print (f"Meters Projection: {mSRID} ")
print (f"Input file: {dataFile} ")
print (f"Target table: {targetTable} ")
print ("----")

def loadData(conn, file):

    print("Loading data")

    # load updates into a table
    orgDb="dbname='" + dbName + "' host='" + dbHost + "' port='" + dbPort + "' user='" + dbUser + "' password='" + dbPassword + "'"

    pycmd = '"' + ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + mSRID + ' -nln "' + schema + '.' + targetTable + '" -lco GEOMETRY_NAME=geometry "' + file + '"'
    print(pycmd)
    subprocess.run(pycmd)

    query = f"""
    ALTER TABLE {schema}.{targetTable} ADD COLUMN unique_id uuid;
    UPDATE {schema}.{targetTable} SET unique_id = gen_random_uuid();
    ALTER TABLE {schema}.{targetTable} OWNER TO cwf_analyst;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def matchIds(conn):

    print(f"""Matching to modelled crossings within {dist} m""")

    query = f"""
    ALTER TABLE {schema}.{targetTable} DROP COLUMN IF EXISTS crossing_id;
    ALTER TABLE {schema}.{targetTable} ADD COLUMN crossing_id uuid;

    ALTER TABLE {schema}.{targetTable} DROP COLUMN IF EXISTS crossing_distance;
    ALTER TABLE {schema}.{targetTable} ADD COLUMN crossing_distance double precision;

    with match as (
        SELECT
        a.id AS id,
        nn.unique_id AS unique_id,
        nn.dist
        FROM {schema}.modelled_crossings a
        CROSS JOIN LATERAL (
            SELECT
            unique_id,
            ST_Distance(a.{mGeometry}, b.geometry) as dist
            FROM {schema}.{targetTable} b
            ORDER BY a.{mGeometry} <-> b.geometry
            LIMIT 1
        ) as nn
        WHERE nn.dist < {dist}
    ),

    match_distinct AS (
        select distinct on(unique_id) unique_id, dist, id
        from match
        order by unique_id, dist asc
    )

    UPDATE {schema}.{targetTable} a
    SET crossing_id = m.id,
        crossing_distance = m.dist
    FROM match_distinct AS m WHERE m.unique_id = a.unique_id;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def mapAttributes(conn):

    print("Mapping attributes to matched points")

    query = f"""
    ALTER TABLE {schema}.modelled_crossings DROP COLUMN IF EXISTS passability_status;
    ALTER TABLE {schema}.modelled_crossings ADD COLUMN passability_status varchar;

    UPDATE {schema}.modelled_crossings
    SET passability_status = 'barrier', crossing_type = 'culvert'
    FROM {schema}.{targetTable} a WHERE id = a.crossing_id;

    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def main():

    # -- MAIN SCRIPT --

    print("Connecting to database...")

    conn = pg2.connect(database=dbName,
                    user=dbUser,
                    host=dbHost,
                    password=dbPassword,
                    port=dbPort)

    loadData(conn, dataFile)
    matchIds(conn)

    print("Done!")
    endTime = datetime.now()
    print("End time:", endTime)
    print("Total runtime: " + str((datetime.now() - startTime)))

if __name__ == "__main__":
    main()
