import subprocess
import os
import argparse
import configparser
import getpass
import psycopg2 as pg2

parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=True)
parser.add_argument('-file', type=str, help='the file containing data to load')
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
srcFile = args.file

#output data schema
schema = config['DATABASE']['data_schema']
cabdSRID = config['DATABASE']['cabdSRID']
mSRID  = config['SETTINGS']['mSRID']

def log(message):
    if 1:
        print(message)

#alternate ogr options
# ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"
# ogr = "C:\\Program Files\\QGIS 3.22.3\\bin\\ogr2ogr.exe"
ogr = "C:\\Program Files\\QGIS 3.22.1\\bin\\ogr2ogr.exe"

conn = pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)

#create schema
query = f"""
CREATE SCHEMA IF NOT EXISTS {schema};
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

log("Loading data into: " + schema)

#load files into db
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"

my_env = os.environ.copy()
#my_env["PGCLIENTENCODING"] = "LATIN1"

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nrn_nl" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "nrn_nl" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.resource_newfoundland" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "resource_newfoundland" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.resource_labrador" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "resource_labrador" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

log("Cleaning up layers...")

# rename ids to be unique for insert into modelled crossing table
query = f"""
ALTER TABLE {schema}.nrn_nl RENAME COLUMN nid TO nrn_nid;

ALTER TABLE {schema}.resource_labrador RENAME COLUMN globalid TO lab_globalid;
ALTER TABLE {schema}.resource_labrador RENAME COLUMN road_name TO lab_road_name;
ALTER TABLE {schema}.resource_labrador RENAME COLUMN authority TO lab_authority;
ALTER TABLE {schema}.resource_labrador RENAME COLUMN road_type TO lab_road_type;
ALTER TABLE {schema}.resource_labrador RENAME COLUMN road_class TO lab_road_class;
ALTER TABLE {schema}.resource_labrador RENAME COLUMN road_surfa TO lab_road_surfa;
ALTER TABLE {schema}.resource_labrador RENAME COLUMN "comments" TO lab_comments;

"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

log("LOAD DONE")
