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
workingSchema = "nb_data_test"

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

log("loading data into: " + workingSchema)


#load files into db
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"


my_env = os.environ.copy()
#my_env["PGCLIENTENCODING"] = "LATIN1"

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.canvec_rail" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "canvec_50k_nb_transport_track_segment_1" -t_srs EPSG:4617"' 
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.gnb_roads" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "gnb_forestry_roads" -t_srs EPSG:4617"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.nbrn_roads" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "nbrn_road_segment_entity" -t_srs EPSG:4617"'
log(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.nrhn_streams" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "nbhn_0000_01_watercourse" -t_srs EPSG:4617"'
log(pycmd)
subprocess.run(pycmd)
      
print ("LOAD DONE")
