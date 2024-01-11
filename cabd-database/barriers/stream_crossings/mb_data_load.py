import psycopg2 as pg2
import subprocess
import os
import argparse
import configparser

parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=True)
parser.add_argument('-user', type=str, help='the username to access the database')
parser.add_argument('-password', type=str, help='the password to access the database')
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
dbUser = args.user
dbPassword = args.password
srcFile = args.file

#output data schema
schema = config['DATABASE']['data_schema']
cabdSRID = config['DATABASE']['cabdSRID']
mSRID  = config['SETTINGS']['mSRID']

def log(message):
    if (1):
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

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nrwn_mb_track" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "nrwn_mb_track" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.mbrn_segment" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "mbrn_segment" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.mbrn_2021" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "mbrn_2021" -t_srs EPSG:' + mSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.mbrn_municipal" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "mbrn_municipal" -t_srs EPSG:' + mSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.mbrn_cap_region" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "mbrn_cap_region" -t_srs EPSG:' + mSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.mbtn_trails" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "mbtn_trails" -t_srs EPSG:' + mSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)




log("Cleaning up layers...")

#rename ids to be unique for insert into modelled crossing table
query = f"""
ALTER TABLE {schema}.nrwn_mb_track
RENAME COLUMN nid TO nrwn_nid;

ALTER TABLE {schema}.mbrn_2021
RENAME COLUMN objectid TO mbrn_2021_fid;

ALTER TABLE {schema}.mbrn_segment
RENAME COLUMN nid TO mbrn_seg_nid;

ALTER TABLE {schema}.mbrn_cap_region
RENAME COLUMN id TO mbrn_cap_fid;


"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()
      
log("LOAD DONE")