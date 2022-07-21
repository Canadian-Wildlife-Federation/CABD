import psycopg2 as pg2
import sys

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
        f"""
        --clear existing data
        drop table if exists {schema}.nexus;
        drop table if exists {schema}.nexus_edge;
        """,
         
        
        f"""
        --populate ecatchmentid for eflowpath
        update {schema}.eflowpath_extra set ecatchment_id = a.internal_id
        from {schema}.ecatchment a, {schema}.eflowpath b 
        where  
            b.internal_id = {schema}.eflowpath_extra.internal_id and 
            a.geometry && b.geometry and 
            ST_Relate(a.geometry, b.geometry,'1********');
            
        commit;
        """,
    
        
        f"""
        --create & populate a table for nexus
        create table {schema}.nexus(
          id uuid not null primary key, 
          nexus_type smallint, 
          bank_ecatchment_id uuid, 
          geometry geometry(POINT, 4617)
        );
        create index {schema}_nexus_geomidx on {schema}.nexus using gist(geometry);
        commit;
        """,
        
        f"""
        insert into {schema}.nexus(id, geometry)
        select uuid_generate_v4(), geometry 
          from (
            select distinct geometry from (
              select st_startpoint(a.geometry) as geometry from {schema}.eflowpath a join {schema}.aoi b on a.aoi_id = b.id and b.status = '{readystatus}'
              union
              select st_endpoint(a.geometry) as geometry from {schema}.eflowpath a join {schema}.aoi b on a.aoi_id = b.id and b.status = '{readystatus}'
          ) b ) as unq;

        commit;
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
    
            commit;
        """,
        
        f"""
            --create a temporary nexus_edge table
            create table {schema}.nexus_edge(eflowpath_id uuid, nexus_id uuid, type integer);
         commit;
        """,
        
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
            select a.id, b.id, 2
            from {chyfschema}.eflowpath a, {schema}.nexus b
            where a.from_nexus_id = b.id
            union
            select a.id, b.id, 2
            from {chyfschema}.eflowpath a, {schema}.nexus b
            where a.to_nexus_id = b.id;

            commit;
        """,
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

        commit;
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

        commit;
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

        commit;
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

        commit;
        """,
        
        f"""
        --flowpath
        with flowpaths as (
           select a.internal_id, a.ef_type from {schema}.eflowpath a join {schema}.aoi b on a.aoi_id = b.id WHERE b.status = '{readystatus}' 
           union
           select id, ef_type from {chyfschema}.eflowpath
        ) 
        update {schema}.nexus set nexus_type = 4 where nexus_type is null and id in (
        select distinct nexus_id from {schema}.nexus_edge b
        join flowpaths e on b.eflowpath_id  = e.internal_id 
        where e.ef_type  = 1 and nexus_id not in
        (select id from {schema}.nexus where nexus_type is not null));

        commit;
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
          from {schema}.nexus_edge b join flowpaths e on b.eflowpath_id  = e.internal_id 
          where b.nexus_id in (select id from {schema}.nexus where nexus_type is null)) cnt
          group by cnt.nexus_id having count(*) > 1
        );

        commit;
        """,
    
        f"""
        --everything else is inferred
        update {schema}.nexus set nexus_type = 7 where nexus_type is null;
        commit;
        """,
        
        f"""
        --ADD NEXUS INFO TO EFLOWPATH
        update {schema}.eflowpath_extra set from_nexus_id = a.nexus_id
        from {schema}.nexus_edge a where a.eflowpath_id = {schema}.eflowpath_extra.internal_id
        and a.type = 1;

        update {schema}.eflowpath_extra set to_nexus_id = a.nexus_id
        from {schema}.nexus_edge a where a.eflowpath_id = {schema}.eflowpath_extra.internal_id
        and a.type = 2;

        commit;
        """,
        
        f"""
        drop table {schema}.nexus_edge;
        commit;
        """
    ]
    for query in queries:
        log(query)
        with conn.cursor() as cursor:
            cursor.execute(query)


def populate_names(conn):
    
    print ("populating names for schema: " + schema)
    
    queries = [
          f"""
        --add any new names to the names table that aren't there
        -- for eflowpaths cgndb
        insert into {chyfschema}.names (name_id, name_en, name_fr, cgndb_id)
        select uuid_generate_v4(), name, null, name_id::uuid
        from (
            select distinct a.name, a.name_id 
            from {schema}.eflowpath a join {schema}.aoi b on a.aoi_id = b.id  
            where b.status = '{readystatus}'
                and a.name_id is not null
                and a.name_id != ''
                and a.geodbname = 'CGNDB' 
                and a.name_id::uuid not in (select cgndb_id from {chyfschema}.names )
            ) foo;
        commit;
        """,
        
        f"""
        --update reference for cgndb
        update {schema}.eflowpath_extra set chyf_name_id = a.name_id
        from {chyfschema}.names a, {schema}.eflowpath b 
        where 
          b.internal_id = {schema}.eflowpath_extra.internal_id and
          b.geodbname = 'CGNDB' and 
          a.cgndb_id = b.name_id::uuid and
          b.name_id is not null and
          b.name_id != '';
        commit;
        """,
                
        f"""
        --add any new names to the names table that aren't there
        -- for ecatchments
        insert into {chyfschema}.names (name_id, name_en, name_fr, cgndb_id)
        select uuid_generate_v4(), name, null, name_id::uuid
        from (
            select distinct a.name, a.name_id
            from {schema}.ecatchment a join {schema}.aoi b on a.aoi_id = b.id
            where b.status = '{readystatus}'
                and a.name_id is not null 
                and a.name_id != ''
                and a.geodbname = 'CGNDB' 
                and a.name_id::uuid not in (select cgndb_id from {chyfschema}.names )
            ) foo;
        commit;
        """,
        
        f"""
        --update reference
        update {schema}.ecatchment_extra set chyf_name_id = a.name_id
        from {chyfschema}.names a, {schema}.ecatchment b
        where 
            b.internal_id = {schema}.ecatchment_extra.internal_id and
            b.geodbname = 'CGNDB' and 
            b.name_id is not null and 
            b.name_id != '' and
            a.cgndb_id = b.name_id::uuid;
         
        commit;
        """
    ]
             
    for query in queries:
        log(query)
        with conn.cursor() as cursor:
            cursor.execute(query)

def copy_to_production(conn):
    
    print(f"copying data from {schema} to {chyfschema} ")
    
    query = f"""

        INSERT into {chyfschema}.aoi (id, short_name, geometry)
        SELECT id, name, st_transform(geometry, 4617) 
        FROM {schema}.aoi 
        WHERE status = '{readystatus}';    
      
        INSERT into {chyfschema}.terminal_point(id, aoi_id, flow_direction, geometry)
        SELECT a.id, a.aoi_id, a.flow_direction, st_transform(a.geometry, 4617) 
        FROM {schema}.terminal_node a JOIN {schema}.aoi b on a.aoi_id = b.id
        WHERE b.status = '{readystatus}';
        
        INSERT into {chyfschema}.shoreline(id, aoi_id, geometry)
        SELECT a.id, a.aoi_id, st_transform(a.geometry, 4617) 
        FROM {schema}.shoreline a JOIN {schema}.aoi b on a.aoi_id = b.id
        WHERE b.status = '{readystatus}';

        INSERT into {chyfschema}.ecatchment(id, nid, ec_type, ec_subtype, area, aoi_id, name_id, geometry)
        SELECT a.internal_id, a.nid, a.ec_type, a.ec_subtype, st_area(a.geometry::geography), a.aoi_id, c.chyf_name_id, st_transform(a.geometry, 4617) 
        FROM {schema}.ecatchment a JOIN {schema}.aoi b on a.aoi_id = b.id JOIN {schema}.ecatchment_extra c on c.internal_id = a.internal_id
        WHERE b.status = '{readystatus}';
        
        UPDATE {chyfschema}.nexus set nexus_type = a.nexus_type
        FROM {schema}.nexus a where a.id = {chyfschema}.nexus.id; 

        INSERT into {chyfschema}.nexus(id, nexus_type, bank_ecatchment_id, geometry)
        SELECT a.id, a.nexus_type, a.bank_ecatchment_id, st_transform(a.geometry, 4617)
        FROM {schema}.nexus a left join {chyfschema}.nexus b on a.id = b.id where b.id is null;
                
        INSERT into {chyfschema}.eflowpath(id, nid, ef_type, ef_subtype, rank, length, 
          name_id, aoi_id, ecatchment_id, from_nexus_id, to_nexus_id, geometry)
        SELECT a.internal_id, a.nid, a.ef_type, a.ef_subtype, a.rank, ST_LengthSpheroid(a.geometry, ss), c.chyf_name_id, 
          a.aoi_id, c.ecatchment_id, c.from_nexus_id, c.to_nexus_id, st_transform(a.geometry, 4617) 
        FROM {schema}.eflowpath a JOIN {schema}.aoi b on a.aoi_id = b.id JOIN {schema}.eflowpath_extra c on c.internal_id = a.internal_id, 
          CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) ss  
        WHERE b.status = '{readystatus}';
        

        UPDATE {schema}.aoi SET status = '{donestatus}' where status = '{readystatus}';
        
        --delete added fields for processing
        drop table if exists {schema}.eflowpath_extra;
        drop table if exists {schema}.ecatchment_extra;
        drop table if exists {schema}.nexus;
        drop table if exists {schema}.nexus_edge;

        commit;
    """    
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
    
    query = f"""
           --delete any existing data for aoi
        
        delete from {chyfschema}.eflowpath where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name and b.status = '{readystatus}');
        delete from {chyfschema}.ecatchment where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name and b.status = '{readystatus}');
        delete from {chyfschema}.terminal_point where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name and b.status = '{readystatus}');
        delete from {chyfschema}.shoreline where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name and b.status = '{readystatus}');
        delete from {chyfschema}.aoi where short_name in (select name from {schema}.aoi where status = '{readystatus}');
       
        delete from {chyfschema}.nexus where id in (
        select id from {chyfschema}.nexus except (
          select from_nexus_id from {chyfschema}.eflowpath
          union
          select to_nexus_id from {chyfschema}.eflowpath
        ));
        
    """    
       
    log(query)
    with conn.cursor() as cursor:
        cursor.execute(query);
    conn.commit()
    
    query = f""" 
        drop table if exists {schema}.eflowpath_extra;
        drop table if exists {schema}.ecatchment_extra;
        
        create table {schema}.eflowpath_extra (internal_id uuid references {schema}.eflowpath(internal_id), ecatchment_id uuid, from_nexus_id uuid, to_nexus_id uuid, chyf_name_id uuid, primary key(internal_id));
        create table {schema}.ecatchment_extra (internal_id uuid references {schema}.ecatchment(internal_id), chyf_name_id uuid, primary key(internal_id));
    
        insert into {schema}.eflowpath_extra (internal_id) select a.internal_id from {schema}.eflowpath a join {schema}.aoi b on a.aoi_id = b.id and b.status = '{readystatus}';
        insert into {schema}.ecatchment_extra (internal_id) select a.internal_id from {schema}.ecatchment a join {schema}.aoi b on a.aoi_id = b.id and b.status = '{readystatus}';
    """
    log(query)
    with conn.cursor() as cursor:
        cursor.execute(query);
    conn.commit()    
    
#--- MAIN FUNCTION ----
conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

delete_current(conn)
populate_nexus(conn)
populate_names(conn)
copy_to_production(conn)

conn.commit()

print ("LOAD DONE")

