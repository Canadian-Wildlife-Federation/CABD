#!/bin/sh

DB_HOST="localhost"
DB_PORT="5432"
DB_USER="smart"
DB_PASSWORD="smart"
DB_NAME="chyf"
PSQL="psql"
TEMP_SCHEMA="temp";

echo "Loading Data File: $1"
echo "As Region: $2"

if [ -z "$2" ] || [ -z "$1" ]; then 
	echo "A region id must be provided (load.sh <file> <region_id>)"
	exit
fi
#--if regsion is null or blank fail

#D0="$PSQL -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_USER -c \"create schema if not exists $TEMP_SCHEMA;DROP TABLE if exists $TEMP_SCHEMA.aoi;DROP TABLE if exists $TEMP_SCHEMA.catchments;DROP TABLE if exists $TEMP_SCHEMA.flowpaths\""
$PSQL -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_USER -c "create schema if not exists $TEMP_SCHEMA;DROP TABLE if exists $TEMP_SCHEMA.aoi;DROP TABLE if exists $TEMP_SCHEMA.catchments;DROP TABLE if exists $TEMP_SCHEMA.flowpaths"

DB_CONNECTION="dbname='$DB_NAME' host='$DB_HOST' port='$DB_PORT' user='$DB_USER' password='$DB_PASSWORD'"

echo "Loading AOI..."
ogr2ogr -f "PostgreSQL" PG:"$DB_CONNECTION" -sql "SELECT * FROM AOI" -nln "temp.aoi" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI $1
echo "Loading Flowpaths..."
ogr2ogr -f "PostgreSQL" PG:"$DB_CONNECTION" -sql "SELECT * FROM EFlowpaths" -nln "temp.flowpaths" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI $1
echo "Loading Catchments..."
ogr2ogr -f "PostgreSQL" PG:"$DB_CONNECTION" -sql "SELECT * FROM ECatchments" -nln "temp.catchments" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI $1

echo "Post Processing in Database..."
C1="insert into chyf.working_limit(region_id, geometry) select '$2', st_transform(st_geometryn(geometry,1), 4326) from $TEMP_SCHEMA.aoi"
C1="$C1;insert into chyf.flowpath(region_id, type, rank, length, geometry) select '$2', case when ef_type = 1 then 'Observed' when ef_type = 2 then 'Bank' when ef_type = 3 then 'Inferred' when ef_type = 4 then 'Constructed' else null end as type, case when rank = 1 then 'Primary' when rank = 2 then 'Secondary' ELSE null end as rank, ST_LengthSpheroid(st_transform(st_geometryn(geometry, 1), 4326), 'SPHEROID[\"WGS 84\",6378137,298.257223563]') as length, st_transform(st_geometryn(geometry,1), 4326) as geometry from $TEMP_SCHEMA.flowpaths"
C1="$C1;insert into chyf.waterbody(region_id, definition, area, geometry) select '$2', -1, st_area(st_transform(st_geometryn(geometry, 1), 4326)::geography), st_transform(st_geometryn(geometry,1), 4326) from $TEMP_SCHEMA.catchments where ec_type = 4"
C1="$C1;insert into chyf.elementary_catchment(region_id, area, geometry) select '$2', st_area(st_transform(st_geometryn(geometry,1), 4326)::geography), st_transform(st_geometryn(geometry,1), 4326) from $TEMP_SCHEMA.catchments where ec_type != 4"
C1="$C1;drop table $TEMP_SCHEMA.aoi; drop table $TEMP_SCHEMA.flowpaths; drop table $TEMP_SCHEMA.catchments"

#D1="$PSQL -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_USER -c \"$C1\""
$PSQL -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_USER -c "$C1"

echo "Load of $1 as $2 Complete"