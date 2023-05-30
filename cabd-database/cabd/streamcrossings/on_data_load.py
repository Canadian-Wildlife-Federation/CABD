import psycopg2 as pg2
import subprocess
import sys
import os

def log(message):
    if (1):
        print(message)
        
        
#alternate ogr options
ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"
#ogr = "C:\\Program Files\\QGIS 3.22.3\\bin\\ogr2ogr.exe"

# os.environ["PROJ_LIB"] = "C:\Program Files\GDAL\projlib"
# print(os.environ["PROJ_LIB"])

dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = sys.argv[1]
dbPassword = sys.argv[2]

srcfile = ""
srcfile = sys.argv[3]
#temporary schema for loading and manipulating data
workingSchema = "on_data_test"

conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

#create schema
query = f"""
CREATE SCHEMA IF NOT EXISTS {workingSchema};
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

log("Loading data into: " + workingSchema)


#load files into db
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"


my_env = os.environ.copy()
#my_env["PGCLIENTENCODING"] = "LATIN1"

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.nrwn_on_track" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "nrwn_on_track" -t_srs EPSG:4617 -overwrite"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.orn_segment" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "orn_segment" -t_srs EPSG:4617 -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.mnrf_segment" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "mnrf_segment" -t_srs EPSG:4617 -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.otn_segment" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "otn_segment" -t_srs EPSG:4617 -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.nrwn_on_structure_ln" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "nrwn_on_structure_ln" -t_srs EPSG:3347 -overwrite"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.nrwn_on_structure_pt" -lco GEOMETRY_NAME=geometry -nlt POINT "' + srcfile + '" "nrwn_on_structure_pt" -t_srs EPSG:3347 -overwrite"'
log(pycmd)
subprocess.run(pycmd)

log("Cleaning up layers...")

#remove trails that are canoe or paddling routes
query = f"""
DELETE FROM {workingSchema}.otn_segment
WHERE trail_name ILIKE '%canoe%'
OR permitted_uses = 'Paddling';
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

#rename ids to be unique for insert into modelled crossing table
query = f"""
ALTER TABLE {workingSchema}.orn_segment
RENAME COLUMN ogf_id TO orn_ogf_id;

ALTER TABLE {workingSchema}.mnrf_segment
RENAME COLUMN ogf_id TO mnrf_ogf_id;

ALTER TABLE {workingSchema}.otn_segment
RENAME COLUMN ogf_id TO otn_ogf_id;

ALTER TABLE {workingSchema}.nrwn_on_track
RENAME COLUMN nid TO nrwn_nid;
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()
      
log("LOAD DONE")