#-------REQUIRES pgcrypto

import psycopg2 as pg2
import sys
from datetime import datetime

if len(sys.argv) != 7:
    print("Invalid Usage: flowpath_2_chyf.py <host> <port> <dbname> <dbuser> <dbpassword> <dataschema> ")
    sys.exit()
    
dbHost = sys.argv[1]
dbPort = sys.argv[2]
dbName = sys.argv[3]
dbUser = sys.argv[4]
dbPassword = sys.argv[5]

schema = sys.argv[6]

chyfschema = "chyf2"

startTime = datetime.now()

#status flag for aoi to be copied over
readystatus = "CHYF_READY"
#status flag updated after aoi copied over
donestatus = "CHYF_DONE"


def log(message):
    if (1):
        print(message);

def populate_nexus(conn):
    
    print ("generating nexus for schema: " + schema)
    
    queries = [
        #--clear existing data
        f"drop table if exists {schema}.nexus;",
        f"drop table if exists {schema}.nexus_edge;",
        
        f"ANALYZE {schema}.eflowpath_extra;",
        f"ANALYZE {schema}.eflowpath;",
        f"ANALYZE {schema}.ecatchment;",
        
        #populate ecatchmentid for eflowpath
        f"drop table if exists {schema}.temp;",
        
        f"""create table {schema}.temp as
        select a.internal_id, c.internal_id as ecatchment_id
        from {schema}.eflowpath_extra a, {schema}.ecatchment c
        where st_contains(c.geometry, a.point);""",
        
        f"create index ididx on {schema}.temp(internal_id);",
        
        f"""
        update {schema}.eflowpath_extra set ecatchment_id = a.ecatchment_id
        from {schema}.temp a
        where a.internal_id = {schema}.eflowpath_extra.internal_id;
        """,
        
        f"drop table {schema}.temp;",
        #create & populate a table for nexus
        f"""create table {schema}.nexus(
          id uuid not null primary key, 
          nexus_type smallint, 
          bank_ecatchment_id uuid, 
          geometry geometry(POINT, 4617)
        );""",
        
        f"create index {schema}_nexus_geomidx on {schema}.nexus using gist(geometry);",
        
        f"""
        with edges as (
            select a.geometry as geometry from {schema}.eflowpath a join {schema}.aoi b on a.aoi_id = b.id and b.status = '{readystatus}'
        )
        insert into {schema}.nexus(id, geometry)
        select gen_random_uuid(), geometry 
          from (
            select st_startpoint(geometry) as geometry from edges
              union
            select st_endpoint(geometry) as geometry from edges
          ) unqpnts ;

        """,
     
        
        f"""
            --update with id with existing chyf nodes (these are nodes as aoi boundaries)
            with mappingpart as (
              select a.id as chyfid, b.id as thisid
              from {chyfschema}.nexus a, {schema}.nexus b
              where a.geometry && b.geometry and
              st_x(a.geometry) = st_x(b.geometry) and
              st_y(a.geometry) = st_y(b.geometry)
            )
            update {schema}.nexus set id = mappingpart.chyfid
            from mappingpart 
            where mappingpart.thisid = {schema}.nexus.id;
    
        """,
        
        f"create table {schema}.nexus_edge(eflowpath_id uuid, nexus_id uuid, type integer);",
        
        
        f"""
            insert into {schema}.nexus_edge(eflowpath_id, nexus_id, type)
            select a.internal_id, b.id, 1 
            from {schema}.eflowpath a join {schema}.aoi c on a.aoi_id = c.id, {schema}.nexus b
            where c.status = '{readystatus}' and a.geometry && b.geometry and st_equals(st_startpoint(a.geometry), b.geometry)
            union
            select a.internal_id, b.id, 2
            from {schema}.eflowpath a join {schema}.aoi c on a.aoi_id = c.id, {schema}.nexus b
            where c.status = '{readystatus}' and a.geometry && b.geometry and st_equals(st_endpoint(a.geometry), b.geometry)
            union
            select a.id, b.id, 1
            from {chyfschema}.eflowpath a, {schema}.nexus b
            where a.from_nexus_id = b.id
            union
            select a.id, b.id, 2
            from {chyfschema}.eflowpath a, {schema}.nexus b
            where a.to_nexus_id = b.id;
        """,
        f"create index nexus_edge_eflowpath_id_idx on {schema}.nexus_edge(eflowpath_id);",  
        f"create index nexus_edge_nexus_id_idx on {schema}.nexus_edge(nexus_id);",

        f"""
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
        """,
        
        f"""
        --headwaters
        update {schema}.nexus set nexus_type = 1 where nexus_type is null and id in (
        select hw.nexus_id from (
          select distinct nexus_id from {schema}.nexus_edge where type = 1
          except
            ( 
              select distinct nexus_id from {schema}.nexus_edge where type = 2
              union
              select id from {chyfschema}.nexus
            )
        ) hw );
        """,
        
        f"""
        --terminal boundary nodes
        update {schema}.nexus set nexus_type = 3 where nexus_type is null and id in (
        select hw.nexus_id from 
        (
        select distinct nexus_id from {schema}.nexus_edge where type = 2
        except
          (
            select distinct nexus_id from {schema}.nexus_edge where type = 1
            union
            select id from {chyfschema}.nexus
          )
        ) hw join {schema}.nexus n on n.id = hw.nexus_id
        where n.geometry in (select a.geometry from {schema}.terminal_node a join {schema}.aoi b on a.aoi_id = b.id where b.status = '{readystatus}')
        );
        """,
        
        f"""
        --terminal isolated nodes
        update {schema}.nexus set nexus_type = 2 where nexus_type is null and id in (
        select hw.nexus_id from (
          select distinct nexus_id from {schema}.nexus_edge where type = 2
          except
            (
              select distinct nexus_id from {schema}.nexus_edge where type = 1
              union
              select id from {chyfschema}.nexus
            )
        ) hw );
        """,
        
        f"""
        --flowpath
        with flowpaths as (
           select a.internal_id, a.ef_type from {schema}.eflowpath a join {schema}.aoi b on a.aoi_id = b.id WHERE b.status = '{readystatus}' 
           union
           select id, ef_type from {chyfschema}.eflowpath
        ) 
        update {schema}.nexus set nexus_type = 4 where nexus_type is null and id in (
        select distinct nexus_id from {schema}.nexus_edge b join {schema}.nexus c on b.nexus_id = c.id
        join flowpaths e on b.eflowpath_id  = e.internal_id 
        where e.ef_type  = 1 and c.nexus_type is null);
        """,
        
        f"""
        --water
        with flowpaths as (
           select a.internal_id, a.ef_type, c.ecatchment_id 
           from {schema}.eflowpath a join {schema}.eflowpath_extra c on a.internal_id = c.internal_id join {schema}.aoi b on a.aoi_id = b.id 
           WHERE b.status = '{readystatus}' 
           union
           select id, ef_type, ecatchment_id from {chyfschema}.eflowpath
        ) 
        update {schema}.nexus set nexus_type = 5 where nexus_type is null and id in (
        select cnt.nexus_id from (
          select distinct b.nexus_id, e.ecatchment_id
          from {schema}.nexus_edge b join {schema}.nexus c on b.nexus_id = c.id join flowpaths e on b.eflowpath_id  = e.internal_id 
          where c.nexus_type is null) cnt
          group by cnt.nexus_id having count(*) > 1
        );
        """,
    
        f"""
        --everything else is inferred
        update {schema}.nexus set nexus_type = 7 where nexus_type is null;
        """,
        
        f"""
        --ADD NEXUS INFO TO EFLOWPATH
        update {schema}.eflowpath_extra set from_nexus_id = a.nexus_id
        from {schema}.nexus_edge a where a.eflowpath_id = {schema}.eflowpath_extra.internal_id
        and a.type = 1;
        """,
        
        f"""
        update {schema}.eflowpath_extra set to_nexus_id = a.nexus_id
        from {schema}.nexus_edge a where a.eflowpath_id = {schema}.eflowpath_extra.internal_id
        and a.type = 2;
        """,
        
        f"drop table {schema}.nexus_edge;"
    ]
    
    for query in queries:
        log(query)
        with conn.cursor() as cursor:
            cursor.execute(query)


def populate_names(conn):
    
    print ("populating names for schema: " + schema)
    
    queries = [
        f"""
          
        with allnames as (
            select distinct a.name_id as nameid, a.name as name, a.geodbname as geodbname
            from {schema}.feature_names a join {schema}.aoi b on a.aoi_id = b.id
            where b.status = '{readystatus}' and a.name_id not in (select geodb_id from chyf2.names)
        )
        --add any new names to the names table that aren't there
        insert into {chyfschema}.names (name_id, name_en, name_fr, geodb_id, geodbname)
        select gen_random_uuid(), name, null, nameid, geodbname
        from allnames 
        """,
        
        f"""
        --update reference for flowpaths
        update {schema}.eflowpath_extra set chyf_rivername_id1 = a.name_id
        from {chyfschema}.names a, {schema}.eflowpath b 
        where 
          b.internal_id = {schema}.eflowpath_extra.internal_id and
          a.geodb_id = b.rivernameid1 and
          b.rivernameid1 is not null and
          b.rivernameid1 != '';
          
        update {schema}.eflowpath_extra set chyf_rivername_id2 = a.name_id
        from {chyfschema}.names a, {schema}.eflowpath b 
        where 
          b.internal_id = {schema}.eflowpath_extra.internal_id and
          a.geodb_id = b.rivernameid2 and
          b.rivernameid2 is not null and
          b.rivernameid2 != '';
        """,
                
        
        f"""
          --update reference for catchments
        update {schema}.ecatchment_extra set chyf_rivername_id1 = a.name_id
        from {chyfschema}.names a, {schema}.ecatchment b 
        where 
          b.internal_id = {schema}.ecatchment_extra.internal_id and
          a.geodb_id = b.rivernameid1 and
          b.rivernameid1 is not null and
          b.rivernameid1 != '';
          
        update {schema}.ecatchment_extra set chyf_rivername_id2 = a.name_id
        from {chyfschema}.names a, {schema}.ecatchment b 
        where 
          b.internal_id = {schema}.ecatchment_extra.internal_id and
          a.geodb_id = b.rivernameid2 and
          b.rivernameid2 is not null and
          b.rivernameid2 != '';
          
        update {schema}.ecatchment_extra set chyf_lakename_id1 = a.name_id
        from {chyfschema}.names a, {schema}.ecatchment b 
        where 
          b.internal_id = {schema}.ecatchment_extra.internal_id and
          a.geodb_id = b.lakenameid1 and
          b.lakenameid1 is not null and
          b.lakenameid1 != '';
          
        update {schema}.ecatchment_extra set chyf_lakename_id2 = a.name_id
        from {chyfschema}.names a, {schema}.ecatchment b 
        where 
          b.internal_id = {schema}.ecatchment_extra.internal_id and
          a.geodb_id = b.lakenameid2 and
          b.lakenameid2 is not null and
          b.lakenameid2 != '';          
        """,
    ]
             
    for query in queries:
        log(query)
        with conn.cursor() as cursor:
            cursor.execute(query)

def copy_to_production(conn):
    
    print(f"copying data from {schema} to {chyfschema} ")
    
    queries = [ 
        
        "alter table chyf2.nexus drop constraint nexus_bank_ecatchment_id_fkey;",
        "alter table chyf2.eflowpath drop constraint eflowpath_ecatchment_id_fkey;",
        "alter table chyf2.ecatchment_attributes drop constraint ecatchment_attributes_id_fkey;",
        "alter table chyf2.eflowpath_attributes drop constraint eflowpath_attributes_id_fkey;",
        
        "alter table chyf2.eflowpath drop constraint eflowpath_from_nexus_id_fkey;",
        "alter table chyf2.eflowpath drop constraint eflowpath_to_nexus_id_fkey;",
        "alter table chyf2.eflowpath drop constraint eflowpath_rivernameid1_fkey;",
        "alter table chyf2.eflowpath drop constraint eflowpath_rivernameid2_fkey;",
        "alter table chyf2.ecatchment drop constraint ecatchment_rivernameid1_fkey;",
        "alter table chyf2.ecatchment drop constraint ecatchment_rivernameid2_fkey;",
        "alter table chyf2.ecatchment drop constraint ecatchment_lakenameid1_fkey;",
        "alter table chyf2.ecatchment drop constraint ecatchment_lakenameid2_fkey;",
        "alter table chyf2.terminal_point drop constraint terminal_point_rivernameid1_fkey;",
        "alter table chyf2.terminal_point drop constraint terminal_point_rivernameid2_fkey;",
        
        f"""
        INSERT into {chyfschema}.aoi (id, short_name, geometry)
        SELECT id, name, st_transform(geometry, 4617) 
        FROM {schema}.aoi 
        WHERE status = '{readystatus}';    
       
        """, 
        
        f"""
        INSERT into {chyfschema}.terminal_point(id, aoi_id, flow_direction, rivernameid1, rivernameid2, geometry)
        SELECT a.id, a.aoi_id, a.flow_direction, c.name_id, d.name_id, st_transform(a.geometry, 4617) 
        FROM {schema}.terminal_node a JOIN {schema}.aoi b on a.aoi_id = b.id
        LEFT JOIN {chyfschema}.names c on a.rivernameid1 = c.geodb_id
        LEFT JOIN {chyfschema}.names d on a.rivernameid2 = d.geodb_id
        WHERE b.status = '{readystatus}';
        """, 
        
        f"""
        INSERT into {chyfschema}.shoreline(id, aoi_id, geometry)
        SELECT a.id, a.aoi_id, st_transform(a.geometry, 4617) 
        FROM {schema}.shoreline a JOIN {schema}.aoi b on a.aoi_id = b.id
        WHERE b.status = '{readystatus}';
        """,
        
        "drop index chyf2.ecatchment_geometry_idx;",
        "drop index chyf2.ecatchment_aoi_id_idx;",
        "alter table chyf2.ecatchment drop constraint ecatchment_pkey;",
        
        f"""
        INSERT into {chyfschema}.ecatchment(id, nid, ec_type, ec_subtype, area, aoi_id, rivernameid1, rivernameid2, lakenameid1, lakenameid2, geometry)
        SELECT a.internal_id, a.nid, a.ec_type, a.ec_subtype, st_area(a.geometry::geography), a.aoi_id, c.chyf_rivername_id1, c.chyf_rivername_id2, c.chyf_lakename_id1, c.chyf_lakename_id2, st_transform(a.geometry, 4617) 
        FROM {schema}.ecatchment a JOIN {schema}.aoi b on a.aoi_id = b.id JOIN {schema}.ecatchment_extra c on c.internal_id = a.internal_id
        WHERE b.status = '{readystatus}';
        """, 
        
        "alter table chyf2.ecatchment add constraint ecatchment_pkey primary key (id);",
        "create index ecatchment_aoi_id_idx on chyf2.ecatchment(aoi_id);",
        "create index ecatchment_geometry_idx on chyf2.ecatchment using gist(geometry);",
        f"UPDATE {chyfschema}.nexus set nexus_type = a.nexus_type FROM {schema}.nexus a where a.id = {chyfschema}.nexus.id;", 
    
        f"delete from {schema}.nexus where id in (select id from chyf2.nexus)",

        "drop index chyf2.nexus_geometry_idx;",
        "alter table chyf2.nexus drop constraint nexus_pkey;",
        f"""    
        INSERT into {chyfschema}.nexus(id, nexus_type, bank_ecatchment_id, geometry)
        SELECT a.id, a.nexus_type, a.bank_ecatchment_id, st_transform(a.geometry, 4617)
        FROM {schema}.nexus a ;
        """, 
        "alter table chyf2.nexus add constraint nexus_pkey primary key(id);",
        "create index nexus_geometry_idx on chyf2.nexus using gist(geometry);",
    
        "alter table chyf2.eflowpath drop constraint eflowpath_pkey;",
        "drop index chyf2.eflowpath_geometry_idx;",
        "drop index chyf2.eflowpath_from_nexus_id_idx;",
        "drop index chyf2.eflowpath_aoi_id_idx;",
        "drop index chyf2.eflowpath_to_nexus_id_idx;",
        f"""
        INSERT into {chyfschema}.eflowpath(id, nid, ef_type, ef_subtype, rank, length, 
          rivernameid1, rivernameid2, aoi_id, ecatchment_id, from_nexus_id, to_nexus_id, geometry)
        SELECT a.internal_id, a.nid, a.ef_type, a.ef_subtype, a.rank, ST_LengthSpheroid(a.geometry, ss), c.chyf_rivername_id1, c.chyf_rivername_id2, 
          a.aoi_id, c.ecatchment_id, c.from_nexus_id, c.to_nexus_id, st_transform(a.geometry, 4617) 
        FROM {schema}.eflowpath a JOIN {schema}.aoi b on a.aoi_id = b.id JOIN {schema}.eflowpath_extra c on c.internal_id = a.internal_id, 
          CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) ss  
        WHERE b.status = '{readystatus}';
        """, 
        
        "alter table chyf2.eflowpath add constraint eflowpath_pkey primary key (id);",
        "create index eflowpath_geometry_idx on chyf2.eflowpath  using gist(geometry);",
        "create index eflowpath_from_nexus_id_idx on chyf2.eflowpath (from_nexus_id);",
        "create index eflowpath_aoi_id_idx on chyf2.eflowpath (aoi_id);",
        "create index eflowpath_to_nexus_id_idx on chyf2.eflowpath (to_nexus_id);",
        
        f"UPDATE {schema}.aoi SET status = '{donestatus}' where status = '{readystatus}';",
        
        #delete added fields for processing
        f"drop table if exists {schema}.eflowpath_extra;",
        f"drop table if exists {schema}.ecatchment_extra;",
        f"drop table if exists {schema}.nexus;",
        f"drop table if exists {schema}.nexus_edge;",
    
        "alter table chyf2.nexus add constraint nexus_bank_ecatchment_id_fkey foreign key (bank_ecatchment_id) references chyf2.ecatchment(id);",
        "alter table chyf2.eflowpath add constraint eflowpath_ecatchment_id_fkey foreign key (ecatchment_id) references chyf2.ecatchment(id);",
        "alter table chyf2.ecatchment_attributes add constraint ecatchment_attributes_id_fkey foreign key (id) references chyf2.ecatchment(id);",
        "alter table chyf2.eflowpath_attributes add constraint eflowpath_attributes_id_fkey foreign key (id) references chyf2.eflowpath(id);",
        "alter table chyf2.eflowpath add constraint eflowpath_from_nexus_id_fkey foreign key (from_nexus_id) references chyf2.nexus(id);",
        "alter table chyf2.eflowpath add constraint eflowpath_to_nexus_id_fkey foreign key (from_nexus_id) references chyf2.nexus(id);",
        "alter table chyf2.eflowpath add constraint eflowpath_rivernameid1_fkey foreign key (rivernameid1) references chyf2.names(name_id);",
        "alter table chyf2.eflowpath add constraint eflowpath_rivernameid2_fkey foreign key (rivernameid2) references chyf2.names(name_id);",
        "alter table chyf2.ecatchment add constraint ecatchment_rivernameid1_fkey foreign key (rivernameid1) references chyf2.names(name_id);",
        "alter table chyf2.ecatchment add constraint ecatchment_rivernameid2_fkey foreign key (rivernameid2) references chyf2.names(name_id);",
        "alter table chyf2.ecatchment add constraint ecatchment_lakenameid1_fkey foreign key (lakenameid1) references chyf2.names(name_id);",
        "alter table chyf2.ecatchment add constraint ecatchment_lakenameid2_fkey foreign key (lakenameid2) references chyf2.names(name_id);",
        "alter table chyf2.terminal_point add constraint terminal_point_rivernameid1_fkey foreign key (rivernameid1) references chyf2.names(name_id);",
        "alter table chyf2.terminal_point add constraint terminal_point_rivernameid2_fkey foreign key (rivernameid2) references chyf2.names(name_id);",
    ]
    
    for query in queries:
        log(query)
        with conn.cursor() as cursor:
            cursor.execute(query)
   
    
def delete_current(connt):
    
    print(f"copying the following aoi's into {chyfschema}: ");
    query = f"SELECT name FROM {schema}.aoi WHERE status = '{readystatus}'";
    with conn.cursor() as cursor:
        cursor.execute(query)
        items = cursor.fetchall()
        for item in items:
            print (item[0], end=" ");
        
    print("") 
    print(f"deleting existing data from {chyfschema}")
    
    queries = [ 
        #drop and re-create constraints for performance reasons
        "alter table chyf2.nexus drop constraint if exists nexus_bank_ecatchment_id_fkey",
        "alter table chyf2.eflowpath drop constraint if exists eflowpath_ecatchment_id_fkey",
        "alter table chyf2.ecatchment_attributes drop constraint if exists ecatchment_attributes_id_fkey",
        "alter table chyf2.eflowpath_attributes drop constraint if exists eflowpath_attributes_id_fkey",
        "alter table chyf2.eflowpath drop constraint if exists eflowpath_from_nexus_id_fkey",
        "alter table chyf2.eflowpath drop constraint if exists eflowpath_to_nexus_id_fkey",
        "alter table chyf2.eflowpath drop constraint if exists eflowpath_rivernameid1_fkey",
        "alter table chyf2.eflowpath drop constraint if exists eflowpath_rivernameid2_fkey",
        "alter table chyf2.ecatchment drop constraint if exists ecatchment_rivernameid1_fkey",
        "alter table chyf2.ecatchment drop constraint if exists ecatchment_rivernameid2_fkey",
        "alter table chyf2.ecatchment drop constraint if exists ecatchment_lakenameid1_fkey",
        "alter table chyf2.ecatchment drop constraint if exists ecatchment_lakenameid2_fkey",
        "alter table chyf2.terminal_point drop constraint if exists terminal_point_rivernameid1_fkey",
        "alter table chyf2.terminal_point drop constraint if exists terminal_point_rivernameid2_fkey",
        
        #--delete any existing data for aoi
        f"delete from {chyfschema}.eflowpath where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name and b.status = '{readystatus}');",
        f"delete from {chyfschema}.ecatchment where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name and b.status = '{readystatus}');",
        f"delete from {chyfschema}.terminal_point where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name and b.status = '{readystatus}');",
        f"delete from {chyfschema}.shoreline where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name and b.status = '{readystatus}');",
        f"delete from {chyfschema}.aoi where short_name in (select name from {schema}.aoi where status = '{readystatus}');",
       
        f"""delete from {chyfschema}.nexus where id in (
        select id from {chyfschema}.nexus except (
          select from_nexus_id from {chyfschema}.eflowpath
          union
          select to_nexus_id from {chyfschema}.eflowpath
        ));""",
        
        "alter table chyf2.nexus add constraint nexus_bank_ecatchment_id_fkey foreign key (bank_ecatchment_id) references chyf2.ecatchment(id);",
        "alter table chyf2.eflowpath add constraint eflowpath_ecatchment_id_fkey foreign key (ecatchment_id) references chyf2.ecatchment(id);",
        "alter table chyf2.ecatchment_attributes add constraint ecatchment_attributes_id_fkey foreign key (id) references chyf2.ecatchment(id);",
        "alter table chyf2.eflowpath_attributes add constraint eflowpath_attributes_id_fkey foreign key (id) references chyf2.eflowpath(id);",
        "alter table chyf2.eflowpath add constraint eflowpath_from_nexus_id_fkey foreign key (from_nexus_id) references chyf2.nexus(id);",
        "alter table chyf2.eflowpath add constraint eflowpath_to_nexus_id_fkey foreign key (from_nexus_id) references chyf2.nexus(id);",
        "alter table chyf2.eflowpath add constraint eflowpath_rivernameid1_fkey foreign key (rivernameid1) references chyf2.names(name_id);",
        "alter table chyf2.eflowpath add constraint eflowpath_rivernameid2_fkey foreign key (rivernameid2) references chyf2.names(name_id);",
        "alter table chyf2.ecatchment add constraint ecatchment_rivernameid1_fkey foreign key (rivernameid1) references chyf2.names(name_id);",
        "alter table chyf2.ecatchment add constraint ecatchment_rivernameid2_fkey foreign key (rivernameid2) references chyf2.names(name_id);",
        "alter table chyf2.ecatchment add constraint ecatchment_lakenameid1_fkey foreign key (lakenameid1) references chyf2.names(name_id);",
        "alter table chyf2.ecatchment add constraint ecatchment_lakenameid2_fkey foreign key (lakenameid2) references chyf2.names(name_id);",
        "alter table chyf2.terminal_point add constraint terminal_point_rivernameid1_fkey foreign key (rivernameid1) references chyf2.names(name_id);",
        "alter table chyf2.terminal_point add constraint terminal_point_rivernameid2_fkey foreign key (rivernameid2) references chyf2.names(name_id);",
        
        f"drop table if exists {schema}.eflowpath_extra;",
        f"drop table if exists {schema}.ecatchment_extra;",
        
        f"create table {schema}.eflowpath_extra (internal_id uuid references {schema}.eflowpath(internal_id), ecatchment_id uuid, from_nexus_id uuid, to_nexus_id uuid, chyf_rivername_id1 uuid, chyf_rivername_id2 uuid, point geometry(point, 4617), primary key(internal_id));",
        f"create table {schema}.ecatchment_extra (internal_id uuid references {schema}.ecatchment(internal_id), chyf_lakename_id1 uuid, chyf_lakename_id2 uuid, chyf_rivername_id1 uuid, chyf_rivername_id2 uuid, primary key(internal_id));",
    
        f"insert into {schema}.eflowpath_extra (internal_id, point) select a.internal_id, ST_LineInterpolatePoint(a.geometry, 0.5) from {schema}.eflowpath a join {schema}.aoi b on a.aoi_id = b.id and b.status = '{readystatus}';",
        f"insert into {schema}.ecatchment_extra (internal_id) select a.internal_id from {schema}.ecatchment a join {schema}.aoi b on a.aoi_id = b.id and b.status = '{readystatus}';",
        
        f"create index eflowpath_extra_point_idx on {schema}.eflowpath_extra using gist(point);"
    ]
        
    for query in queries:   
        log(query)
        with conn.cursor() as cursor:
            cursor.execute(query);

    
def qa_results(connection):
    
    query = f"""
        with primary_count as (
            select from_nexus_id
            from chyf2.eflowpath
            where rank = 1
            group by from_nexus_id
            having count(*) > 1
        )
        select a.id, st_astext(a.geometry) from chyf2.nexus a join primary_count c on a.id = c.from_nexus_id;
        
    """    
       
    log(query)
    with conn.cursor() as cursor:
        cursor.execute(query);
        rows = cursor.fetchall()
        
        for row in rows:
            print(f"Flowpath Rank Error: {row[1]} ");
    
    
    
#--- MAIN FUNCTION ----
conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

conn.autocommit = True
delete_current(conn)
populate_nexus(conn)
populate_names(conn)
copy_to_production(conn)
qa_results(conn)

conn.commit()

print ("LOAD DONE")
print("Runtime: " + str((datetime.now() - startTime)))