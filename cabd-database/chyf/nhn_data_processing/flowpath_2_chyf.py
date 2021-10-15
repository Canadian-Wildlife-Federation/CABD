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

chyfschema = "chyf"

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
    
    --create a temporary nexus_edge table
    create table {schema}.nexus_edge(eflowpath_id uuid, nexus_id uuid, type integer);

    insert into {schema}.nexus_edge(eflowpath_id, nexus_id, type)
    select a.internal_id, b.id, 1 
    from {schema}.eflowpath a, {schema}.nexus b
    where a.geometry && b.geometry and st_equals(st_startpoint(a.geometry), b.geometry)
    union
    select a.internal_id, b.id, 2
    from {schema}.eflowpath a, {schema}.nexus b
    where a.geometry && b.geometry and st_equals(st_endpoint(a.geometry), b.geometry)
    union
    select a.id, b.id, 2
    from {chyfschema}.eflowpath a, {schema}.nexus b
    where a.from_nexus_id = b.id
    union
    select a.id, b.id, 2
    from {chyfschema}.eflowpath a, {schema}.nexus b
    where a.to_nexus_id = b.id;



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
        ( 
          select distinct nexus_id from {schema}.nexus_edge where type = 2
          union
          select id from {chyfschema}.nexus
        )
    ) hw );

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
    where n.geometry in (select geometry from {schema}.terminal_node)
    );

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

    --flowpath
    with flowpaths as (
       select internal_id, ef_type from {schema}.eflowpath e2 
       union
       select id, ef_type from {chyfschema}.eflowpath
    ) 
    update {schema}.nexus set nexus_type = 4 where nexus_type is null and id in (
    select distinct nexus_id from {schema}.nexus_edge b
    join flowpaths e on b.eflowpath_id  = e.internal_id 
    where e.ef_type  = 1 and nexus_id not in
    (select id from {schema}.nexus where nexus_type is not null));

    --water
    with flowpaths as (
       select internal_id, ef_type, ecatchment_id from {schema}.eflowpath e2 
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
    with conn.cursor() as cursor:
        cursor.execute(query)


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
    insert into {chyfschema}.names (name_id, name_en, name_fr, cgndb_id)
    select uuid_generate_v4(), name, null, name_id::uuid
    from (
        select distinct name, name_id 
        from {schema}.eflowpath 
        where name_id is not null
            and name_id != ''
            and geodbname = 'CGNDB' 
            and name_id::uuid not in (select cgndb_id from {chyfschema}.names )
        ) foo;
    
    --update reference
    update {schema}.eflowpath set chyf_name_id = a.name_id
    from {chyfschema}.names a 
    where a.cgndb_id = {schema}.eflowpath.name_id::uuid
      and {schema}.eflowpath.name_id is not null
      and {schema}.eflowpath.name_id != '';

    --add any new names to the names table that aren't there
    -- for ecatchments
    insert into {chyfschema}.names (name_id, name_en, name_fr, cgndb_id)
    select uuid_generate_v4(), name, null, name_id::uuid
    from (
        select distinct name, name_id
        from {schema}.ecatchment 
        where name_id is not null and name_id != ''
          and geodbname = 'CGNDB' 
            and name_id::uuid not in (select cgndb_id from {chyfschema}.names )
        ) foo;

    --update reference
    update {schema}.ecatchment set chyf_name_id = a.name_id
    from {chyfschema}.names a 
    where 
      {schema}.ecatchment.name_id is not null and 
      {schema}.ecatchment.name_id != '' and
       a.cgndb_id = {schema}.ecatchment.name_id::uuid;
         
    """
    log(query)

    with conn.cursor() as cursor:
        cursor.execute(query)

def copy_to_production(conn):
    
    print(f"copying data from {schema} to {chyfschema} ")
    
    query = f"""

        insert into {chyfschema}.aoi (id, short_name, geometry)
        select id, name, st_transform(geometry, 4617) from {schema}.aoi;    
      
        insert into {chyfschema}.terminal_point(id, aoi_id, flow_direction, geometry)
        select id, aoi_id, flow_direction, st_transform(geometry, 4617) from {schema}.terminal_node ;
        
        insert into {chyfschema}.shoreline(id, aoi_id, geometry)
        select id, aoi_id, st_transform(geometry, 4617) from {schema}.shoreline ;

        insert into {chyfschema}.ecatchment(id, ec_type, ec_subtype, area, aoi_id, name_id, geometry)
        select internal_id, ec_type, ec_subtype, st_area(geometry::geography), aoi_id, chyf_name_id, st_transform(geometry, 4617) 
        from {schema}.ecatchment ;
        
        update {chyfschema}.nexus set nexus_type = a.nexus_type
        from {schema}.nexus a where a.id = {chyfschema}.nexus.id; 

        insert into {chyfschema}.nexus(id, nexus_type, bank_ecatchment_id, geometry)
        select a.id, a.nexus_type, a.bank_ecatchment_id, st_transform(a.geometry, 4617)
        from {schema}.nexus a left join {chyfschema}.nexus b on a.id = b.id where b.id is null;
                
        insert into {chyfschema}.eflowpath(id, ef_type, ef_subtype, rank, length, 
          name_id, aoi_id, ecatchment_id, from_nexus_id, to_nexus_id, geometry)
        select internal_id, ef_type, ef_subtype, rank, ST_LengthSpheroid(geometry, ss), chyf_name_id, 
          aoi_id, ecatchment_id, from_nexus_id, to_nexus_id, st_transform(geometry, 4617) 
        from {schema}.eflowpath, CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) ss  ;
        
        
        --delete added fields for processing
        alter table {schema}.eflowpath drop column if exists ecatchment_id;
        alter table {schema}.eflowpath drop column if exists from_nexus_id;
        alter table {schema}.eflowpath drop column if exists to_nexus_id;
        alter table {schema}.eflowpath drop column if exists chyf_name_id;
        alter table {schema}.ecatchment drop column if exists chyf_name_id;
        drop table if exists {schema}.nexus;
        drop table if exists {schema}.nexus_edge;

    """    
    log(query)

    with conn.cursor() as cursor:
        cursor.execute(query)
    
    
def delete_current(connt):
    print(f"deleting existing data from {chyfschema}")
    
    query = f"""
           --delete any existing data for aoi
        
        delete from {chyfschema}.eflowpath where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name);
        delete from {chyfschema}.ecatchment where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name);
        delete from {chyfschema}.terminal_point where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name);
        delete from {chyfschema}.shoreline where aoi_id in (select a.id from {chyfschema}.aoi a, {schema}.aoi b where a.short_name = b.name);
        delete from {chyfschema}.aoi where short_name in (select name from {schema}.aoi);
       
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

cleanup = f"""
delete from {schema}.errors ;
delete from {schema}.construction_points ;
delete from {schema}.eflowpath ;
delete from {schema}.ecatchment;
delete from {schema}.shoreline ;
delete from {schema}.eflowpath ;
delete from {schema}.terminal_node ;
delete from {schema}.aoi;
"""

print ("To remove data from processing schema so it doesn't get copied a second time run the following db commands:")
print (cleanup)