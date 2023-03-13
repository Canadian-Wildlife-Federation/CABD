import psycopg2 as pg2
import sys
import subprocess
import os

def log(message):
    if (1):
        print(message)
        
        
#alternate ogr options
#ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"
ogr = "C:\\Program Files\\QGIS 3.22.3\\bin\\ogr2ogr.exe"

#os.environ["PROJ_LIB"] = "C:\Program Files\QGIS 3.22.3\share\proj";
#print(os.environ["PROJ_LIB"])

dbHost = "localhost"
dbPort = "8446";
dbName = "cabd";
dbUser = "USERNAME";
dbPassword = "PASSWORD";


srcfile = "C:/data/CWF/streamcrossings/nb_networks/nb_networks/nb_networks.gpkg";
#temporary schema for loading and manipulating data 
workingSchema = "nb_data"

log("loading data into: " + workingSchema)


#load files into db
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"


my_env = os.environ.copy()
#my_env["PGCLIENTENCODING"] = "LATIN1"

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.canvec_rail" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "canvec_50k_nb_transport_track_segment_1" -t_srs EPSG:4617"' 
log(pycmd)
subprocess.run(pycmd, env=my_env)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.gnb_roads" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "gnb_forestry_roads" -t_srs EPSG:4617"'
log(pycmd)
subprocess.run(pycmd, env=my_env)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.nbrn_roads" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "nbrn_road_segment_entity" -t_srs EPSG:4617"'
log(pycmd)
subprocess.run(pycmd, env=my_env)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.nrhn_streams" -lco GEOMETRY_NAME=geometry -nlt MULTILINESTRING "' + srcfile + '" "nbhn_0000_01_watercourse" -t_srs EPSG:4617"'
log(pycmd)
subprocess.run(pycmd, env=my_env)
      
print ("LOAD DONE")
