# this script fixes the data structure of workunits to match
# various changes that have been made to scripts after the workunit was loaded

# this script should only be used for data in the Mackenzie basin
# and can be removed after processing of the Mackenzie basin is complete

import psycopg2 as pg2
import sys

if len(sys.argv) != 7:
    print("Invalid Usage: nhn_fixes.py <host> <port> <dbname> <dbuser> <dbpassword> <nhnworkunit>")

dbHost = sys.argv[1]
dbPort = sys.argv[2]
dbName = sys.argv[3]
dbUser = sys.argv[4]
dbPassword = sys.argv[5]

workunit = sys.argv[6].upper()

conn = pg2.connect(database=dbName, 
                user=dbUser, 
                host=dbHost, 
                password=dbPassword, 
                port=dbPort)

workingSchema = "nhn" + workunit.lower()
print("Fixing data structure: " + workunit)  

query1 = f"""

--fix empty strings in eflowpath name fields
UPDATE {workingSchema}.eflowpath SET geographicalnamedb = TRIM(geographicalnamedb);
UPDATE {workingSchema}.eflowpath SET geographicalnamedb = NULL WHERE TRIM(geographicalnamedb) = '';
UPDATE {workingSchema}.eflowpath SET nameid1 = NULL WHERE TRIM(nameid1) = '';
UPDATE {workingSchema}.eflowpath SET nameid2 = NULL WHERE TRIM(nameid2) = '';
UPDATE {workingSchema}.eflowpath SET name1 = NULL WHERE TRIM(name1) = '';
UPDATE {workingSchema}.eflowpath SET name2 = NULL WHERE TRIM(name2) = '';

--fix empty strings in ecatchment name fields
UPDATE {workingSchema}.ecatchment SET geographicalnamedb = TRIM(geographicalnamedb);
UPDATE {workingSchema}.ecatchment SET geographicalnamedb = NULL WHERE geographicalnamedb = '';
UPDATE {workingSchema}.ecatchment SET lakeid1 = NULL WHERE TRIM(lakeid1) = '';
UPDATE {workingSchema}.ecatchment SET lakeid2 = NULL WHERE TRIM(lakeid2) = '';
UPDATE {workingSchema}.ecatchment SET riverid1 = NULL WHERE TRIM(riverid1) = '';
UPDATE {workingSchema}.ecatchment SET riverid2 = NULL WHERE TRIM(riverid2) = '';
UPDATE {workingSchema}.ecatchment SET lakename1 = NULL WHERE TRIM(lakename1) = '';
UPDATE {workingSchema}.ecatchment SET lakename2 = NULL WHERE TRIM(lakename2) = '';
UPDATE {workingSchema}.ecatchment SET rivername1 = NULL WHERE TRIM(rivername1) = '';
UPDATE {workingSchema}.ecatchment SET rivername2 = NULL WHERE TRIM(rivername2) = '';

--add name fields to terminal points
ALTER TABLE {workingSchema}.terminal_node
ADD COLUMN rivernameid1 varchar(32),
ADD COLUMN rivernameid2 varchar(32),
ADD COLUMN rivername1 varchar(32),
ADD COLUMN rivername2 varchar(32) ,
ADD COLUMN geodbname varchar;

-- apply names to terminal points
-- this will only work for linear intersections when the centroid of the intersection
-- is closest to the skeleton line (this is generally the case but it doesn't have to be)
-- these should be reviewed
UPDATE {workingSchema}.terminal_node
SET rivernameid1 = a.nameid1, rivernameid2 = a.nameid2, rivername1 = a.name1, rivername2 = a.name2, geodbname = a.geographicalnamedb
FROM
{workingSchema}.eflowpath a
WHERE st_intersects(a.geometry, {workingSchema}.terminal_node.geometry)
AND a.ef_type in (1, 3, 4);
"""

with conn.cursor() as cursor:
    cursor.execute(query1)
conn.commit()

print ("Data structure fixes complete: " + workunit)
print ("Check names on terminal nodes and fix problematic geometry at catchment edges before running nhn_2_fpprocessing")