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

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nsrn" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "nsrn" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nstdb_ln" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "nstdb_ln" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nstdb_poly" -lco GEOMETRY_NAME=geometry -nlt POLYGON "' + srcFile + '" "nstdb_poly" -t_srs EPSG:' + cabdSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nstdb_pt" -lco GEOMETRY_NAME=geometry -nlt POINT "' + srcFile + '" "nstdb_pt" -t_srs EPSG:' + mSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.nssdb_pt" -lco GEOMETRY_NAME=geometry -nlt POINT "' + srcFile + '" "nssdb_pt" -t_srs EPSG:' + mSRID + ' -overwrite"'
log(pycmd)
subprocess.run(pycmd)

log("Cleaning up layers...")

#rename ids to be unique for insert into modelled crossing table
query = f"""
ALTER TABLE {schema}.nstdb_pt
RENAME COLUMN shape_fid TO nstdb_pt_shape_fid;

ALTER TABLE {schema}.nstdb_ln
RENAME COLUMN shape_fid TO nstdb_ln_shape_fid;

ALTER TABLE {schema}.nstdb_ln
RENAME COLUMN feat_code TO nstdb_ln_feat_code;

ALTER TABLE {schema}.nstdb_ln
RENAME COLUMN feat_desc TO nstdb_ln_feat_desc;

ALTER TABLE {schema}.nstdb_poly
RENAME COLUMN shape_fid TO nstdb_poly_shape_fid;
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

#remove some types of transport features we don't want
query = f"""
DELETE FROM {schema}.nstdb_poly WHERE feat_desc IN ('CUTTING polygon', 'FILL polygon');
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

log("LOAD DONE")
