#!/bin/sh

DB_HOST="localhost"
DB_PORT="5432"
DB_USER="smart"
DB_PASSWORD="smart"
DB_NAME="chyf"
PSQL="psql"
TEMP_SCHEMA="temp";

BARRIER_TABLE="dams.dams_medium_large";

echo "Loading Data File: $1"
echo "Target Table: $BARRIER_TABLE"

if [ -z "$1" ]; then 
	echo "A data file must be provided (load.sh <file> )"
	exit
fi
#--if regsion is null or blank fail

$PSQL -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_USER -c "create schema if not exists $TEMP_SCHEMA;DROP TABLE if exists $TEMP_SCHEMA.barrier_data;"

DB_CONNECTION="dbname='$DB_NAME' host='$DB_HOST' port='$DB_PORT' user='$DB_USER' password='$DB_PASSWORD'"

echo "Loading Data..."
ogr2ogr -f "PostgreSQL" PG:"$DB_CONNECTION" -nln "temp.barrier_data" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI $1

echo "Post Processing in Database..."
C1="insert into $BARRIER_TABLE(cadb_id


C1="insert into chyf.working_limit(region_id, geometry) select '$2', st_transform(st_geometryn(geometry,1), 4326) from $TEMP_SCHEMA.aoi"
C1="$C1;insert into chyf.flowpath(region_id, type, rank, length, geometry) select '$2', case when ef_type = 1 then 'Observed' when ef_type = 2 then 'Bank' when ef_type = 3 then 'Inferred' when ef_type = 4 then 'Constructed' else null end as type, case when rank = 1 then 'Primary' when rank = 2 then 'Secondary' ELSE null end as rank, ST_LengthSpheroid(st_transform(st_geometryn(geometry, 1), 4326), 'SPHEROID[\"WGS 84\",6378137,298.257223563]') as length, st_transform(st_geometryn(geometry,1), 4326) as geometry from $TEMP_SCHEMA.flowpaths"
C1="$C1;insert into chyf.waterbody(region_id, definition, area, geometry) select '$2', -1, st_area(st_transform(st_geometryn(geometry, 1), 4326)::geography), st_transform(st_geometryn(geometry,1), 4326) from $TEMP_SCHEMA.catchments where ec_type = 4"
C1="$C1;insert into chyf.elementary_catchment(region_id, area, geometry) select '$2', st_area(st_transform(st_geometryn(geometry,1), 4326)::geography), st_transform(st_geometryn(geometry,1), 4326) from $TEMP_SCHEMA.catchments where ec_type != 4"
C1="$C1;drop table $TEMP_SCHEMA.aoi; drop table $TEMP_SCHEMA.flowpaths; drop table $TEMP_SCHEMA.catchments"

#D1="$PSQL -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_USER -c \"$C1\""
$PSQL -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_USER -c "$C1"

echo "Load of $1 as $2 Complete"