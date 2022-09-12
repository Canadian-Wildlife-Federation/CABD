import psycopg2 as pg2
import sys

if len(sys.argv) != 7:
    print("Invalid Usage: nhn_2_fpprocessing.py <host> <port> <dbname> <dbuser> <dbpassword> <workingunit> ")
    sys.exit()
    
dbHost = sys.argv[1]
dbPort = sys.argv[2]
dbName = sys.argv[3]
dbUser = sys.argv[4]
dbPassword = sys.argv[5]

workunit = sys.argv[6].upper()
fromschema = "nhn" + workunit.lower()
toschema = "fpinput"
outschema = "fpoutput"
srid = 4617

def log(message):
    if (1):
        print(message);

def copytonhnraw(conn):
    
    print ("copying data from " + fromschema + " to " + toschema)
    
    query = f"""
    create schema if not exists {toschema};
    
    --create tables if they don't exist
    create table if not exists {toschema}.aoi(
        id uuid not null primary key,
        name varchar not null,
        status varchar default 'READY',
        geometry geometry(polygon, {srid})
    );
    create index if not exists {toschema}_aoi_geometry_idx on {toschema}.aoi using gist(geometry);
    
    create table if not exists {toschema}.ecatchment(
        id uuid not null primary key,
        nid varchar(32),
        aoi_id uuid not null references {toschema}.aoi(id),
        ec_type smallint not null,
        ec_subtype smallint,
        is_reservoir boolean,
        rivername1 varchar,
        rivernameid1 varchar(32),
        rivername2 varchar,
        rivernameid2 varchar(32),
        lakename1 varchar,
        lakenameid1 varchar(32),
        lakename2 varchar,
        lakenameid2 varchar(32),
        geodbname varchar,
        permanency integer,
        geometry geometry(polygon, {srid})
    );
    create index if not exists {toschema}_ecatchment_geometry_idx on {toschema}.ecatchment using gist(geometry);

    create table if not exists {toschema}.eflowpath(
        id uuid not null primary key,
        nid varchar(32),
        aoi_id uuid not null references {toschema}.aoi(id),
        ef_type smallint not null,
        ef_subtype smallint,
        direction_known smallint,
        rivername1 varchar,
        rivernameid1 varchar(32),
        rivername2 varchar,
        rivernameid2 varchar(32),
        geodbname varchar,
        geometry geometry(linestring, {srid})
    );
    create index if not exists {toschema}_eflowpath_geometry_idx on {toschema}.eflowpath using gist(geometry);

    create table if not exists {toschema}.shoreline(
        id uuid not null primary key,
        aoi_id uuid not null references {toschema}.aoi(id),
        geometry geometry(linestring, {srid})
    );
    create index if not exists {toschema}_shoreline_geometry_idx on {toschema}.shoreline using gist(geometry);
    
    create table if not exists {toschema}.terminal_node(
        id uuid not null primary key,
        aoi_id uuid not null references {toschema}.aoi(id),
        flow_direction smallint not null,
        rivername1 varchar,
        rivernameid1 varchar(32),
        rivername2 varchar,
        rivernameid2 varchar(32),
        geodbname varchar,
        geometry geometry(point, {srid})
    );
    create index if not exists {toschema}_terminal_node_geometry_idx on {toschema}.terminal_node using gist(geometry);
    
    --clear existing data
    delete from {toschema}.eflowpath where aoi_id in (select id from {toschema}.aoi where name = '{workunit}');
    delete from {toschema}.ecatchment where aoi_id in (select id from {toschema}.aoi where name = '{workunit}');
    delete from {toschema}.shoreline where aoi_id in (select id from {toschema}.aoi where name = '{workunit}');
    delete from {toschema}.terminal_node where aoi_id in (select id from {toschema}.aoi where name = '{workunit}');
    delete from {toschema}.aoi where name = '{workunit}';
    delete from {outschema}.eflowpath where aoi_id in (select id from {outschema}.aoi where name = '{workunit}');
    delete from {outschema}.ecatchment where aoi_id in (select id from {outschema}.aoi where name = '{workunit}');
    delete from {outschema}.shoreline where aoi_id in (select id from {outschema}.aoi where name = '{workunit}');
    delete from {outschema}.terminal_node where aoi_id in (select id from {outschema}.aoi where name = '{workunit}');
    delete from {outschema}.construction_points where aoi_id in (select id from {outschema}.aoi where name = '{workunit}');
    delete from {outschema}.errors where aoi_id in (select id from {outschema}.aoi where name = '{workunit}');     
    delete from {outschema}.feature_names where aoi_id in (select id from {outschema}.aoi where name = '{workunit}');         
    delete from {outschema}.aoi where name = '{workunit}';
    
    insert into {toschema}.aoi (id, name, geometry) 
    select id, '{workunit}', st_transform(geometry, {srid}) from {fromschema}.aoi;
    
    insert into {toschema}.shoreline(id, geometry, aoi_id) 
    select case when id is null then gen_random_uuid() else id end, 
    st_transform(geometry, {srid}), aoi_id 
    from {fromschema}.shoreline;
    
    insert into {toschema}.terminal_node(id, geometry, flow_direction, aoi_id, rivername1, rivernameid1, rivername2, rivernameid2, geodbname) 
    select case when id is null then gen_random_uuid() else id end, st_transform(geometry, {srid}), flow_direction, aoi_id, rivername1, rivernameid1, rivername2, rivernameid2, geodbname 
    from {fromschema}.terminal_node;
    
    insert into {toschema}.eflowpath(
      id, nid, aoi_id, ef_type, ef_subtype, 
      direction_known, geodbname,
      rivername1, rivernameid1, rivername2, rivernameid2, 
      geometry
    )

    select case when id is null then gen_random_uuid() else id end, nid,
      aoi_id, ef_type, ef_subtype, direction_known,
      geographicalnamedb,
      name1, nameid1, name2, nameid2,
      st_transform(geometry, {srid})
    from {fromschema}.eflowpath;
    
    insert into {toschema}.ecatchment(
      id, nid, aoi_id, ec_type, ec_subtype, permanency, 
      rivername1, rivernameid1, rivername2, rivernameid2, lakename1, lakenameid1, lakename2, lakenameid2, geodbname, is_reservoir, geometry)
    select case when id is null then gen_random_uuid() else id end, nid,
      aoi_id, ec_type, ec_subtype, permanency,
      rivername1, riverid1, rivername2, riverid2, lakename1, lakeid1, lakename2, lakeid2,
      geographicalnamedb, is_reservoir, 
      st_transform(geometry, {srid})
    from {fromschema}.ecatchment;
    --drop schema {fromschema} cascade;
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

copytonhnraw(conn)

print ("Copy complete; ready to generate flowpath data")
