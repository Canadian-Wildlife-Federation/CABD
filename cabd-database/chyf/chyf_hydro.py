import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\Program Files\\QGIS 3.12\\bin\\ogr2ogr.exe";

dbHost = "HOST"
dbPort = "PORT"
dbName = "DBNAME"
dbUser = "USERNAME"
dbPassword = "PASSWORD"

#temporary schema for loading and manipulating data 
workingSchema = "load";

#get datafile and region from command-line
dataFile = '';
region = '';

if len(sys.argv) == 3:
    dataFile = sys.argv[1]
    region = sys.argv[2]

if dataFile == '' or region == '':
    print("Data file and region required.  Usage: chyf_hydro.py <dataFile> <region>")
    sys.exit()

print("Loading data for " + region + " from file " +  dataFile)


conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

query = f"""
CREATE SCHEMA if not exists {workingSchema};
DROP TABLE if exists {workingSchema}.aoi;
DROP TABLE if exists {workingSchema}.catchments;
DROP TABLE if exists {workingSchema}.flowpaths;
"""

with conn.cursor() as cursor:
    cursor.execute(query);
conn.commit();

#load data using ogr
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -sql "SELECT * FROM AOI" -nln "' + workingSchema + '.aoi" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + dataFile
print(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -sql "SELECT * FROM EFlowpaths" -nln "' + workingSchema + '.flowpaths" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + dataFile
print(pycmd)
subprocess.run(pycmd)

pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -sql "SELECT * FROM ECatchments" -nln "' + workingSchema + '.catchments" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + dataFile
print(pycmd)
subprocess.run(pycmd)




#run scripts to load data
query = f"""

delete from chyf.flowpath where region_id = '{region}';
delete from chyf.waterbody where region_id = '{region}';
delete from chyf.elementary_catchment where region_id = '{region}';
delete from chyf.working_limit where region_id = '{region}';

insert into chyf.working_limit(region_id, geometry) 
select '{region}', st_transform(st_geometryn(geometry,1), 4326) from {workingSchema}.aoi;

insert into chyf.flowpath(region_id, type, rank, length, geometry) 
select '{region}', 
    case when ef_type = 1 then 'Observed' 
        when ef_type = 2 then 'Bank' 
        when ef_type = 3 then 'Inferred' 
        when ef_type = 4 then 'Constructed' 
        else null end as type, 
    case when rank = 1 then 'Primary' 
        when rank = 2 then 'Secondary' 
        ELSE null end as rank, 
    ST_LengthSpheroid(st_transform(st_geometryn(geometry, 1), 4326), 
    'SPHEROID[\"WGS 84\",6378137,298.257223563]') as length, 
    st_transform(st_geometryn(geometry,1), 4326) as geometry 
from {workingSchema}.flowpaths;
    
insert into chyf.waterbody(region_id, definition, area, geometry) 
select '{region}', -1, 
    st_area(st_transform(st_geometryn(geometry, 1), 4326)::geography), 
    st_transform(st_geometryn(geometry,1), 4326) 
from {workingSchema}.catchments where ec_type = 4;
    
insert into chyf.elementary_catchment(region_id, area, geometry) 
select '{region}', 
    st_area(st_transform(st_geometryn(geometry,1), 4326)::geography), 
    st_transform(st_geometryn(geometry,1), 4326) 
from {workingSchema}.catchments where ec_type != 4;

drop table {workingSchema}.aoi; 
drop table {workingSchema}.flowpaths; 
drop table {workingSchema}.catchments;
    """

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()
        
print("script complete")