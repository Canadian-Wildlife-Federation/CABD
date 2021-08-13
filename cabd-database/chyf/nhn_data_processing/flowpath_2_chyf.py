import psycopg2 as pg2
import sys
import subprocess
import tempfile
import zipfile
import shutil
import re
import os
from nhn_data_qa import run_qa

if len(sys.argv) != 7:
    print("Invalid Usage: flowpath_2_chyf.py <host> <port> <dbname> <dbuser> <dbpassword> <dataschema> ")
    sys.exit()
    
dbHost = sys.argv[1]
dbPort = sys.argv[2]
dbName = sys.argv[3]
dbUser = sys.argv[4]
dbPassword = sys.argv[5]

schema = sys.argv[6];

def log(message):
    if (1):
        print(message);

def populate_nexus(conn):
    
    print ("generating nexus for schema: " + schema)
    
    query = f"""

--create index nhnfp_eflowpath_geometry on nhnfp.eflowpath using gist(geometry);
--create index nhnfp_ecatchment_geometry on nhnfp.ecatchment using gist(geometry);
    --clear existing data
    alter table {schema}.eflowpath drop column if exists ecatchment_id;
    alter table {schema}.eflowpath drop column if exists from_nexus_id;
    alter table {schema}.eflowpath drop column if exists to_nexus_id;
    drop table if exists {schema}.nexus;
    drop table if exists {schema}.nexus_edge;
    
    --populate ecatchmentid for eflowpath
    alter table {schema}.eflowpath add column ecatchment_id uuid references {schema}.ecatchment(internal_id);

    update {schema}.eflowpath set ecatchment_id = a.internal_id
    from {schema}.ecatchment a 
    where a.geometry && {schema}.eflowpath.geometry 
    and ST_Relate(a.geometry, {schema}.eflowpath.geometry,'1********');

    -- create & populate a table for nexus
    create table {schema}.nexus(
      id uuid not null primary key, 
      nexus_type smallint, 
      bank_ecatchment_id uuid, 
      geometry geometry(POINT, 4617)
    );
    create index {schema}_nexus_geomidx on {schema}.nexus using gist(geometry);

    insert into {schema}.nexus(id, geometry)
    select uuid_generate_v4(), geometry 
      from (
        select distinct geometry from (
          select st_startpoint(a.geometry) as geometry from {schema}.eflowpath a
          union
          select st_endpoint(a.geometry) as geometry from {schema}.eflowpath a
      ) b ) as unq;

    --create a temporary nexus_edge table
    create table {schema}.nexus_edge(eflowpath_id uuid, nexus_id uuid, type integer);

    insert into {schema}.nexus_edge(eflowpath_id, nexus_id, type)
    select a.internal_id, b.id, 1 
    from {schema}.eflowpath a, {schema}.nexus b
    where a.geometry && b.geometry and st_equals(st_startpoint(a.geometry), b.geometry)
    union
    select a.internal_id, b.id, 2
    from {schema}.eflowpath a, {schema}.nexus b
    where a.geometry && b.geometry and st_equals(st_endpoint(a.geometry), b.geometry);

    --bank nexus
    update {schema}.nexus set nexus_type = 6 where id in (
    select hw.nexus_id from (
      select distinct nexus_id from {schema}.nexus_edge where type = 1
      except
      select distinct nexus_id from {schema}.nexus_edge where type = 2
    ) hw
    join {schema}.nexus_edge e on hw.nexus_id = e.nexus_id 
    join {schema}.eflowpath e2 on e2.internal_id  = e.eflowpath_id 
    where e2.ef_type = 2
    );

    --headwaters
    update {schema}.nexus set nexus_type = 1 where nexus_type is null and id in (
    select hw.nexus_id from (
      select distinct nexus_id from {schema}.nexus_edge where type = 1
      except
      select distinct nexus_id from {schema}.nexus_edge where type = 2
    ) hw );

    --terminal boundary nodes
    update {schema}.nexus set nexus_type = 3 where nexus_type is null and id in (
    select hw.nexus_id from 
    (
    select distinct nexus_id from {schema}.nexus_edge where type = 2
    except
    select distinct nexus_id from {schema}.nexus_edge where type = 1
    ) hw join {schema}.nexus n on n.id = hw.nexus_id
    where n.geometry in (select geometry from {schema}.terminal_node)
    );

    --terminal isolated nodes
    update {schema}.nexus set nexus_type = 2 where nexus_type is null and id in (
    select hw.nexus_id from (
      select distinct nexus_id from {schema}.nexus_edge where type = 2
      except
      select distinct nexus_id from {schema}.nexus_edge where type = 1
    ) hw );

    --flowpath
    update {schema}.nexus set nexus_type = 4 where nexus_type is null and id in (
    select distinct nexus_id from {schema}.nexus_edge b
    join {schema}.eflowpath e on b.eflowpath_id  = e.internal_id 
    where e.ef_type  = 1 and nexus_id not in
    (select id from {schema}.nexus where nexus_type is not null));

    --water
    update {schema}.nexus set nexus_type = 5 where nexus_type is null and id in (
    select cnt.nexus_id from (
      select distinct b.nexus_id, e.ecatchment_id
      from {schema}.nexus_edge b join {schema}.eflowpath e on b.eflowpath_id  = e.internal_id 
      where b.nexus_id in (select id from {schema}.nexus where nexus_type is null)) cnt
      group by cnt.nexus_id having count(*) > 1
    );

    --everything else is inferred
    update {schema}.nexus set nexus_type = 7 where nexus_type is null;

    --ADD NEXUS INFO TO EFLOWPATH
    alter table {schema}.eflowpath add column from_nexus_id uuid references {schema}.nexus(id);
    alter table {schema}.eflowpath add column to_nexus_id uuid references {schema}.nexus(id);

    update {schema}.eflowpath set from_nexus_id = a.nexus_id
    from {schema}.nexus_edge a where a.eflowpath_id = {schema}.eflowpath.internal_id
    and a.type = 1;

    update {schema}.eflowpath set to_nexus_id = a.nexus_id
    from {schema}.nexus_edge a where a.eflowpath_id = {schema}.eflowpath.internal_id
    and a.type = 2;

    drop table {schema}.nexus_edge;
"""

    log(query)

    #with conn.cursor() as cursor:
    #    cursor.execute(query)
    conn.commit()


def populate_names(conn):
    
    print ("populating names for schema: " + schema)
    
    query = f"""

    --clear existing data
    alter table {schema}.eflowpath drop column if exists chyf_name_id;
    alter table {schema}.ecatchment drop column if exists chyf_name_id;
    
    alter table {schema}.eflowpath add column chyf_name_id uuid ;
    alter table {schema}.ecatchment add column chyf_name_id uuid ;

    --add any new names to the names table that aren't there
    -- for eflowpaths
    insert into chyf.names (id, name_en, name_fr, cgndb_id)
    select uuid_generate_v4(), name_string, null, nameid_1::uuid
    from (
        select distinct name_string, nameid_1 
        from {schema}.eflowpath 
        where nameid_1 is not null and geonamedb = 'CGNDB' 
            and nameid_1::uuid not in (select cgndb_id from chyf.names )
        ) foo;
    
    --update reference
    update {schema}.eflowpath set chyf_name_id = a.id
    from chyf.names a 
    where a.cgndb_id = {schema}.eflowpath.nameid_1::uuid;

    --add any new names to the names table that aren't there
    -- for ecatchments
    insert into chyf.names (id, name_en, name_fr, cgndb_id)
    select uuid_generate_v4(), lakename_1, null, lakeid_1::uuid
    from (
        select distinct lakename_1, lakeid_1
        from {schema}.ecatchment 
        where lakeid_1 is not null and geonamedb = 'CGNDB' 
            and lakeid_1::uuid not in (select cgndb_id from chyf.names )
        ) foo;
        
    --update reference
    update {schema}.ecatchment set chyf_name_id = a.id
    from chyf.names a where a.cgndb_id = {schema}.ecatchment.lakeid_1::uuid;
    
    insert into chyf.names (id, name_en, name_fr, cgndb_id)
    select uuid_generate_v4(), rivname_1, null, rivid_1::uuid
    from (
        select distinct rivname_1, rivid_1
        from {schema}.ecatchment 
        where lakeid_1 is null and rivid_1 is not null and geonamedb = 'CGNDB' 
            and rivid_1::uuid not in (select cgndb_id from chyf.names )
        ) foo;
        
    --update reference
    update {schema}.ecatchment set chyf_name_id = a.id
    from chyf.names a 
    where chyf_name_id is null and a.cgndb_id = {schema}.ecatchment.rivid_1::uuid;
    
"""
    
    log(query)

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def copy_to_production(conn):
    
    print ("copying data from " + schema + " to chyf ")
    
    query = f"""
       --delete any existing data for aoi
        
        delete from chyf.eflowpath where aoi_id in (select a.id from chyf.aoi a, {schema}.aoi b on a.short_name = b.name);
        delete from chyf.ecatchment where aoi_id in (select a.id from chyf.aoi a, {schema}.aoi b on a.short_name = b.name);
       -- delete from chyf.nexus where aoi_id in (select a.id from chyf.aoi a, {schema}.aoi b on a.short_name = b.name);
        delete from chyf.terminal_point where aoi_id in (select a.id from chyf.aoi a, {schema}.aoi b on a.short_name = b.name);
        delete from chyf.shoreline where aoi_id in (select a.id from chyf.aoi a, {schema}.aoi b on a.short_name = b.name);
        delete from chyf.aoi where short_name in (select name from {schema}.aoi);
        
        insert into chyf.aoi (id, short_name, geometry)
        select id, name, st_transform(geometry, 4617) from {schema}.aoi;    
      
        insert into chyf.terminal_point(id, aoi_id, flow_direction, geometry)
        select id, aoi_id, flow_direction, st_transform(geometry, 4617) from {schema}.terminal_node ;
        
        insert into chyf.shoreline(id, aoi_id, geometry)
        select id, aoi_id, st_transform(geometry, 4617) from {schema}.shoreline ;

        insert into chyf.ecatchment(id, ec_type, ec_subtype, area, aoi_id, name_id, geometry)
        select internal_id, ec_type, ec_subtype, st_area(geometry), aoi_id, chyf_name_id, st_transform(geometry, 4617) 
        from {schema}.ecatchment ;

        insert into chyf.nexus(id, nexus_type, bank_ecatchment_id, geometry)
        select id, nexus_type, bank_ecatchment_id, st_transform(geometry, 4617)
        from {schema}.nexus;
                
        insert into chyf.eflowpath(id, ef_type, ef_subtype, rank, length, 
          name_id, aoi_id, ecatchment_id, from_nexus_id, to_nexus_id, geometry)
        select internal_id, ef_type, ef_subtype, rank, st_length(geometry), chyf_name_id, 
          aoi_id, ecatchment_id, from_nexus_id, to_nexus_id, st_transform(geometry, 4617) 
        from {schema}.eflowpath ;
      
    """    
    log(query)

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    
    
    
#--- MAIN FUNCTION ----
conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

populate_nexus(conn)
#populate_names(conn)
#copy_to_production(conn)

print ("LOAD DONE")
