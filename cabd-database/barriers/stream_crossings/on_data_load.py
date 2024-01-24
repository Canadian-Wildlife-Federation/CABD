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
ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"
#ogr = "C:\\Program Files\\QGIS 3.22.3\\bin\\ogr2ogr.exe"

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

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nrwn_on_track" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "nrwn_on_track" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.orn_segment" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "orn_segment" -t_srs EPSG:' + cabdSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.mnrf_segment" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "mnrf_segment" -t_srs EPSG:' + cabdSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.otn_segment" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "otn_segment" -t_srs EPSG:' + cabdSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nrwn_on_structure_ln" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "nrwn_on_structure_ln" -t_srs EPSG:' + mSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nrwn_on_structure_pt" -lco GEOMETRY_NAME=geometry -nlt POINT "' + srcFile + '" "nrwn_on_structure_pt" -t_srs EPSG:' + mSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

log("Cleaning up layers...")

#remove trails that are canoe or paddling routes
query = f"""
DELETE FROM {schema}.otn_segment
WHERE trail_name ILIKE '%canoe%'
OR permitted_uses = 'Paddling';
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

#rename ids to be unique for insert into modelled crossing table
query = f"""
ALTER TABLE {schema}.orn_segment
RENAME COLUMN ogf_id TO orn_ogf_id;

ALTER TABLE {schema}.mnrf_segment
RENAME COLUMN ogf_id TO mnrf_ogf_id;

ALTER TABLE {schema}.otn_segment
RENAME COLUMN ogf_id TO otn_ogf_id;

ALTER TABLE {schema}.nrwn_on_track
RENAME COLUMN nid TO nrwn_nid;
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

log("LOAD DONE")
