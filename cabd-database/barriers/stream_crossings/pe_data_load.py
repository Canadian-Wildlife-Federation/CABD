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

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nrn_pe" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "nrn_pe" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.confed_trail" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "confederation_trail" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.bridges_2005_inventory" -lco GEOMETRY_NAME=geometry -nlt POINT "' + srcFile + '" "bridges_2005_inventory" -t_srs EPSG:' + mSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.confed_trail_inventory" -lco GEOMETRY_NAME=geometry -nlt POINT "' + srcFile + '" "confederation_trail_inventory" -t_srs EPSG:' + mSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

log("Cleaning up layers...")

# rename ids to be unique for insert into modelled crossing table
query = f"""
ALTER TABLE {schema}.bridges_2005_inventory
RENAME COLUMN fid TO bridges_2005_inv_fid;

ALTER TABLE {schema}.bridges_2005_inventory
ADD COLUMN fid serial;

ALTER TABLE {schema}.nrn_pe
RENAME COLUMN nid TO nrn_nid;

ALTER TABLE {schema}.confed_trail_inventory
RENAME COLUMN fid TO confed_trail_inv_fid;

ALTER TABLE {schema}.confed_trail_inventory
ADD COLUMN fid serial;

ALTER TABLE {schema}.confed_trail
ADD COLUMN trail_name varchar DEFAULT 'Confederation Trail';
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

# remove some transportation types we want to exclude
query = f"""
DELETE FROM {schema}.bridges_2005_inventory WHERE watercours ILIKE 'dry%';
DELETE FROM {schema}.confed_trail WHERE "status" = 'Undeveloped';
DELETE FROM {schema}.confed_trail_inventory WHERE stru_type IS NULL;
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

log("LOAD DONE")
