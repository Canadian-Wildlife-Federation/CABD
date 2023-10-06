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

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.reseau_ferroviaire" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "reseau_ferroviaire" -t_srs EPSG:' + cabdSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.reseau_routier" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "reseau_routier" -t_srs EPSG:' + cabdSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.sentiers_quad_fqcq" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "sentiers_quad_fqcq" -t_srs EPSG:' + cabdSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + schema + '.route_verte" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcFile + '" "route_verte" -t_srs EPSG:' + cabdSRID + ' -overwrite"' 
log(pycmd)
subprocess.run(pycmd)


log("Cleaning up layers...")

# rename ids to be unique for insert into modelled crossing table
query = f"""
ALTER TABLE {schema}.reseau_ferroviaire
RENAME COLUMN aqrp_uuid TO reseau_ferroviaire_uuid;

ALTER TABLE {schema}.reseau_routier
RENAME COLUMN aqrp_uuid TO reseau_routier_uuid;

ALTER TABLE {schema}.sentiers_quad_fqcq
RENAME COLUMN uuid TO sentiers_quad_fqcq_uuid;

ALTER TABLE {schema}.route_verte
RENAME COLUMN aqrp_uuid TO route_verte_uuid;
"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

# delete records that do not have associated infrastructure (e.g., ferry crossings)
query = f"""
--ferry routes
DELETE FROM {schema}.reseau_ferroviaire WHERE classvoie = 'Transbordeur';

--winter roads, maritime routes
DELETE FROM {schema}.reseau_routier WHERE caractrte = 'Saisonnier' OR clsrte = 'Liaison maritime';

--trail not constructed yet or the route is part of an existing road
DELETE FROM {schema}.route_verte WHERE codetatavc = 'P' OR (codtypvcyc != '5' AND codtypvcyc IS NOT NULL);
"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()
      
log("LOAD DONE")